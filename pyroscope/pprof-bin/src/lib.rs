#![allow(unused_assignments)]
mod ch64;
mod merge;

use ch64::city_hash_64;
use ch64::read_uint64_le;
use lazy_static::lazy_static;
use pprof_pb::google::v1::Function;
use pprof_pb::google::v1::Location;
use pprof_pb::google::v1::Profile;
use pprof_pb::google::v1::Sample;
use pprof_pb::querier::v1::FlameGraph;
use pprof_pb::querier::v1::Level;
use pprof_pb::querier::v1::SelectMergeStacktracesResponse;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::panic;
use std::sync::Mutex;
use std::vec::Vec;
use wasm_bindgen::prelude::*;

pub mod pprof_pb {

    pub mod google {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/google.v1.rs"));
        }
    }
    pub mod types {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/types.v1.rs"));
        }
    }
    pub mod querier {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/querier.v1.rs"));
        }
    }
}

struct TreeNodeV2 {
    //parent_id: u64,
    fn_id: u64,
    node_id: u64,
    slf: Vec<i64>,
    total: Vec<i64>,
}

struct Tree {
    names: Vec<String>,
    names_map: HashMap<u64, usize>,
    nodes: HashMap<u64, Vec<TreeNodeV2>>,
    sample_types: Vec<String>,
    max_self: Vec<i64>,
    nodes_num: i32,
}

fn find_node(id: u64, nodes: &Vec<TreeNodeV2>) -> i32 {
    let mut n: i32 = -1;
    for c in 0..nodes.len() {
        let _c = &nodes[c];
        if _c.node_id == id {
            n = c as i32;
            break;
        }
    }
    n
}

fn get_node_id(parent_id: u64, name_hash: u64, level: u16) -> u64 {
    let mut node_bytes: [u8; 16] = [0; 16];
    for i in 0..8 {
        node_bytes[i] = ((parent_id >> (i * 8)) & 0xFF) as u8;
    }
    for i in 0..8 {
        node_bytes[i + 8] = ((name_hash >> (i * 8)) & 0xFF) as u8;
    }
    let mut _level = level;
    if _level > 511 {
        _level = 511;
    }
    (city_hash_64(&node_bytes[0..]) >> 9) | ((_level as u64) << 55)
}

struct MergeTotalsProcessor {
    from_idx: Vec<i32>,
}

impl MergeTotalsProcessor {
    fn new(tree: &Tree, p: &Profile) -> MergeTotalsProcessor {
        let mut from_idx: Vec<i32> = vec![-1; tree.sample_types.len()];
        for i in 0..tree.sample_types.len() {
            let sample_type_to = &tree.sample_types[i];
            for j in 0..p.sample_type.len() {
                let sample_type_from = format!(
                    "{}:{}",
                    p.string_table[p.sample_type[j].r#type as usize],
                    p.string_table[p.sample_type[j].unit as usize]
                );
                if sample_type_from == *sample_type_to {
                    from_idx[i] = j as i32;
                    break;
                }
            }
        }
        MergeTotalsProcessor { from_idx }
    }

    fn merge_totals(
        &self,
        node: &mut TreeNodeV2,
        _max_self: &Vec<i64>,
        sample: &Sample,
        merge_self: bool,
    ) -> Vec<i64> {
        let mut max_self = _max_self.clone();
        for i in 0..self.from_idx.len() {
            if self.from_idx[i] == -1 {
                continue;
            }
            node.total[i] += sample.value[self.from_idx[i] as usize];
            if merge_self {
                node.slf[i] += sample.value[self.from_idx[i] as usize];
                for i in 0..max_self.len() {
                    if max_self[i] < node.slf[i] {
                        max_self[i] = node.slf[i];
                    }
                }
            }
        }
        max_self
    }
}

fn merge(tree: &mut Tree, p: &Profile) {
    let mut functions: HashMap<u64, &Function> = HashMap::new();
    for f in p.function.iter() {
        functions.insert(f.id, &f);
    }
    let mut locations: HashMap<u64, &Location> = HashMap::new();
    for l in p.location.iter() {
        locations.insert(l.id, &l);
    }

    let m = MergeTotalsProcessor::new(tree, p);
    for l in p.location.iter() {
        let line = &p.string_table[functions[&l.line[0].function_id].name as usize];
        let line_hash = city_hash_64(line.as_bytes());
        if tree.names_map.contains_key(&line_hash) {
            continue;
        }
        tree.names.push(line.clone());
        tree.names_map.insert(line_hash, tree.names.len() - 1);
    }

    for s in p.sample.iter() {
        let mut parent_id: u64 = 0;
        for i in (0..s.location_id.len()).rev() {
            let location = locations[&s.location_id[i]];
            let name = &p.string_table[functions[&location.line[0].function_id].name as usize];
            let name_hash = city_hash_64(name.as_bytes());
            let node_id = get_node_id(parent_id, name_hash, (s.location_id.len() - i) as u16);
            if !tree.nodes.contains_key(&parent_id) && tree.nodes_num < 2000000 {
                tree.nodes.insert(parent_id, Vec::new());
            }
            let mut fake_children: Vec<TreeNodeV2> = Vec::new();
            let children = tree.nodes.get_mut(&parent_id).unwrap_or(&mut fake_children);
            let mut n = find_node(node_id, children);
            if n == -1 {
                children.push(TreeNodeV2 {
                    //parent_id,
                    fn_id: name_hash,
                    node_id,
                    slf: vec![0; tree.sample_types.len()],
                    total: vec![0; tree.sample_types.len()],
                });
                let idx = children.len().clone() - 1;
                let max_self = m.merge_totals(
                    children.get_mut(idx).unwrap(),
                    tree.max_self.as_ref(),
                    s,
                    i == 0,
                );
                tree.max_self = max_self;
                n = idx as i32;
            } else if tree.nodes_num < 2000000 {
                m.merge_totals(
                    children.get_mut(n as usize).unwrap(),
                    &tree.max_self,
                    s,
                    i == 0,
                );
                tree.nodes_num += 1;
            }
            parent_id = node_id;
        }
    }
}

fn read_uleb128(bytes: &[u8]) -> (usize, usize) {
    let mut result = 0;
    let mut shift = 0;
    loop {
        let byte = bytes[shift];
        result |= ((byte & 0x7f) as usize) << (shift * 7);
        shift += 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    (result, shift)
}

fn bfs(t: &Tree, res: &mut Vec<Level>, sample_type: String) {
    let mut total: i64 = 0;
    let mut root_children: &Vec<TreeNodeV2> = &Vec::new();
    if t.nodes.contains_key(&(0u64)) {
        root_children = t.nodes.get(&(0u64)).unwrap();
    }

    let mut _sample_type_index: i32 = -1;
    for i in 0..t.sample_types.len() {
        if t.sample_types[i] == sample_type {
            _sample_type_index = i as i32;
            break;
        }
    }
    if _sample_type_index == -1 {
        return;
    }
    let sample_type_index = _sample_type_index as usize;

    for i in root_children.iter() {
        total += i.total[sample_type_index];
    }
    let mut lvl = Level::default();
    lvl.values.extend([0, total, 0, 0]);
    res.push(lvl);

    let mut totals = vec![0; t.sample_types.len()];
    totals[sample_type_index] = total;
    let total_node: TreeNodeV2 = TreeNodeV2 {
        slf: vec![0; t.sample_types.len()],
        total: totals,
        node_id: 0,
        fn_id: 0,
        //parent_id: 0
    };
    let mut prepend_map: HashMap<u64, i64> = HashMap::new();

    let mut reviewed: HashSet<u64> = HashSet::new();

    let mut refs: Vec<&TreeNodeV2> = vec![&total_node];
    let mut ref_len: usize = 1;
    while ref_len > 0 {
        let mut prepend: i64 = 0;
        let _refs = refs.clone();
        refs.clear();
        lvl = Level::default();
        for parent in _refs.iter() {
            prepend += prepend_map.get(&parent.node_id).unwrap_or(&0);
            let opt = t.nodes.get(&parent.node_id);

            if opt.is_none() {
                prepend += parent.total[sample_type_index];
                continue;
            }
            for n in opt.unwrap().iter() {
                if reviewed.contains(&n.node_id) {
                    // PANIC!!! WE FOUND A LOOP
                    return;
                } else {
                    reviewed.insert(n.node_id);
                }
                prepend_map.insert(n.node_id, prepend);
                refs.push(n);
                lvl.values.extend([
                    prepend as i64,
                    n.total[sample_type_index],
                    n.slf[sample_type_index],
                    *t.names_map.get(&n.fn_id).unwrap_or(&1) as i64,
                ]);
                prepend = 0;
            }
            prepend += parent.slf[sample_type_index];
        }
        res.push(lvl.clone());
        ref_len = refs.len();
    }
}

lazy_static! {
    static ref CTX: Mutex<HashMap<u32, Tree>> = Mutex::new(HashMap::new());
}

fn upsert_tree(ctx: &mut HashMap<u32, Tree>, id: u32, sample_types: Vec<String>) {
    if !ctx.contains_key(&id) {
        let _len = sample_types.len().clone();
        ctx.insert(
            id,
            Tree {
                names: vec!["total".to_string(), "n/a".to_string()],
                names_map: HashMap::new(),
                nodes: HashMap::new(),
                sample_types,
                max_self: vec![0; _len],
                nodes_num: 1,
            },
        );
    }
}

struct TrieReader {
    bytes: Vec<u8>,
    offs: usize,
}

impl TrieReader {
    fn new(bytes: &[u8]) -> TrieReader {
        TrieReader {
            bytes: bytes.to_vec(),
            offs: 0,
        }
    }

    fn read_uint64_le(&mut self) -> u64 {
        let res = read_uint64_le(&self.bytes[self.offs..]);
        self.offs += 8;
        res
    }

    fn read_size(&mut self) -> usize {
        let res = read_uleb128(&self.bytes[self.offs..]);
        self.offs += res.1;
        res.0
    }

    fn read_string(&mut self) -> String {
        let size = self.read_size();
        let string = String::from_utf8_lossy(&self.bytes[self.offs..self.offs + size]).to_string();
        self.offs += size;
        string
    }

    /*fn read_blob(&mut self) -> &[u8] {
        let size = self.read_size();
        let string = &self.bytes[self.offs..self.offs + size];
        self.offs += size;
        string
    }

    fn read_string_vec(&mut self) -> Vec<String> {
        let mut res = Vec::new();
        let size = self.read_size();
        for _ in 0..size {
            res.push(self.read_string());
        }
        res
    }*/

    fn read_blob_vec(&mut self) -> Vec<&[u8]> {
        let mut res = Vec::new();
        let size = self.read_size();
        for _ in 0..size {
            let uleb = read_uleb128(&self.bytes[self.offs..]);
            self.offs += uleb.1;
            let _size = uleb.0;
            let string = &self.bytes[self.offs..self.offs + _size];
            self.offs += _size;
            res.push(string);
        }
        res
    }
    /*fn end(&self) -> bool {
        self.offs >= self.bytes.len()
    }*/
}

fn merge_trie(tree: &mut Tree, bytes: &[u8], samples_type: &String) {
    let _sample_type_index = tree.sample_types.iter().position(|x| x == samples_type);
    if _sample_type_index.is_none() {
        return;
    }
    let sample_type_index = _sample_type_index.unwrap();
    let mut reader = TrieReader::new(bytes);
    let mut size = reader.read_size();
    for _i in 0..size {
        let id = reader.read_uint64_le();
        let func = reader.read_string();
        if !tree.names_map.contains_key(&id) && tree.names.len() < 2000000 {
            tree.names.push(func);
            tree.names_map.insert(id, tree.names.len() - 1);
        }
    }

    size = reader.read_size();
    for _i in 0..size {
        let parent_id = reader.read_uint64_le();
        let fn_id = reader.read_uint64_le();
        let node_id = reader.read_uint64_le();
        let _slf = reader.read_uint64_le() as i64;
        let _total = reader.read_uint64_le() as i64;
        if tree.max_self[sample_type_index] < _slf {
            tree.max_self[sample_type_index] = _slf;
        }
        let mut slf = vec![0; tree.sample_types.len()];
        slf[sample_type_index] = _slf;
        let mut total = vec![0; tree.sample_types.len()];
        total[sample_type_index] = _total;
        let mut n: i32 = -1;
        if tree.nodes.contains_key(&parent_id) {
            n = find_node(node_id, tree.nodes.get(&parent_id).unwrap());
        }
        if n != -1 {
            tree.nodes
                .get_mut(&parent_id)
                .unwrap()
                .get_mut(n as usize)
                .unwrap()
                .total[sample_type_index] += total[sample_type_index];
            tree.nodes
                .get_mut(&parent_id)
                .unwrap()
                .get_mut(n as usize)
                .unwrap()
                .slf[sample_type_index] += slf[sample_type_index];
        }
        if tree.nodes_num >= 2000000 {
            return;
        }
        if !tree.nodes.contains_key(&parent_id) {
            tree.nodes.insert(parent_id, Vec::new());
        }
        tree.nodes.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
            fn_id,
            //parent_id,
            node_id,
            slf,
            total,
        });
        tree.nodes_num += 1;
    }
}

/*fn upsert_string(prof: &mut Profile, s: String) -> i64 {
    let mut idx = 0;
    for i in 0..prof.string_table.len() {
        if prof.string_table[i] == s {
            idx = i as i64;
            break;
        }
    }
    if idx == 0 {
        idx = prof.string_table.len() as i64;
        prof.string_table.push(s);
    }
    idx
}*/

/*fn upsert_function(prof: &mut Profile, fn_id: u64, fn_name_id: i64) {
    for f in prof.function.iter() {
        if f.id == fn_id {
            return;
        }
    }
    let mut func = Function::default();
    func.name = fn_name_id;
    func.id = fn_id;
    func.filename = upsert_string(prof, "unknown".to_string());
    func.system_name = upsert_string(prof, "unknown".to_string());
    prof.function.push(func);
}*/

/*fn inject_locations(prof: &mut Profile, tree: &Tree) {
    for n in tree.names_map.iter() {
        let hash = *n.1 as u64;
        let name = tree.names[hash as usize].clone();
        let fn_idx = upsert_string(prof, name);
        upsert_function(prof, *n.0, fn_idx);
        let mut loc = Location::default();
        let mut line = Line::default();
        line.function_id = *n.0;
        loc.id = *n.0;
        loc.line = vec![line];
        prof.location.push(loc)
    }
}*/

/*fn upsert_sample(prof: &mut Profile, loc_id: Vec<u64>, val: i64, val_idx: i64) -> i64 {
    let mut idx = -1;
    for i in 0..prof.sample.len() {
        if prof.sample[i].location_id.len() != loc_id.len() {
            continue;
        }
        let mut found = true;
        for j in 0..prof.sample[i].location_id.len() {
            if prof.sample[i].location_id[j] != loc_id[j] {
                found = false;
                break;
            }
        }
        if found {
            idx = i as i64;
            break;
        }
    }
    if idx == -1 {
        let mut sample = Sample::default();
        sample.location_id = loc_id.clone();
        sample.location_id.reverse();
        idx = prof.sample.len() as i64;
        prof.sample.push(sample);
    }
    while prof.sample[idx as usize].value.len() <= val_idx as usize {
        prof.sample[idx as usize].value.push(0)
    }
    prof.sample[idx as usize].value[val_idx as usize] += val;
    idx
}*/

/*fn inject_functions(
    prof: &mut Profile,
    tree: &Tree,
    parent_id: u64,
    loc_ids: Vec<u64>,
    val_idx: i64,
) {
    if !tree.nodes.contains_key(&parent_id) {
        return;
    }
    let children = tree.nodes.get(&parent_id).unwrap();
    for node in children.iter() {
        let mut _loc_ids = loc_ids.clone();
        _loc_ids.push(node.fn_id);
        //TODO:
        upsert_sample(prof, _loc_ids.clone(), node.slf[0 /*TODO*/] as i64, val_idx);
        if tree.nodes.contains_key(&node.node_id) {
            inject_functions(prof, tree, node.node_id, _loc_ids, val_idx);
        }
    }
}*/

/*fn merge_profile(tree: &Tree, prof: &mut Profile, sample_type: String, sample_unit: String) {
    let mut value_type = ValueType::default();
    value_type.r#type = upsert_string(prof, sample_type);
    value_type.unit = upsert_string(prof, sample_unit);
    prof.sample_type.push(value_type);
    let type_idx = prof.sample_type.len() as i64 - 1;
    inject_locations(prof, tree);
    inject_functions(prof, tree, 0, vec![], type_idx);
}*/

#[wasm_bindgen]
pub fn merge_prof(id: u32, bytes: &[u8], sample_type: String) {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id, vec![sample_type]);
        let mut tree = ctx.get_mut(&id).unwrap();
        let prof = Profile::decode(bytes).unwrap();
        merge(&mut tree, &prof);
    });
    match p {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn merge_tree(id: u32, bytes: &[u8], sample_type: String) {
    let result = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id, vec![sample_type.clone()]);
        let mut tree = ctx.get_mut(&id).unwrap();
        merge_trie(&mut tree, bytes, &sample_type);
        0
    });
    match result {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn export_tree(id: u32, sample_type: String) -> Vec<u8> {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        let mut res = SelectMergeStacktracesResponse::default();
        upsert_tree(&mut ctx, id, vec![sample_type.clone()]);
        let tree = ctx.get_mut(&id).unwrap();
        let mut fg = FlameGraph::default();
        fg.names = tree.names.clone();
        fg.max_self = tree.max_self[0 /* TODO */];
        fg.total = 0;
        let mut root_children: &Vec<TreeNodeV2> = &vec![];
        if tree.nodes.contains_key(&(0u64)) {
            root_children = tree.nodes.get(&(0u64)).unwrap();
        }
        for n in root_children.iter() {
            fg.total += n.total[0 /*TODO*/] as i64;
        }
        bfs(tree, &mut fg.levels, sample_type.clone());
        res.flamegraph = Some(fg);
        return res.encode_to_vec();
    });
    match p {
        Ok(res) => return res,
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn export_trees_pprof(payload: &[u8]) -> Vec<u8> {
    let p = panic::catch_unwind(|| {
        let mut reader = TrieReader::new(payload);
        let bin_profs = reader.read_blob_vec();
        let mut merger = merge::ProfileMerge::new();
        for bin_prof in bin_profs {
            let mut prof = Profile::decode(bin_prof).unwrap();
            merger.merge(&mut prof);
        }
        let res = merger.profile();
        res.encode_to_vec()
    });
    match p {
        Ok(res) => return res,
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn drop_tree(id: u32) {
    let mut ctx = CTX.lock().unwrap();
    if ctx.contains_key(&id) {
        ctx.remove(&id);
    }
}

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}
