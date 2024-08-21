#![allow(unused_assignments)]
mod ch64;
mod merge;

use std::cmp::Ordering;
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
use pprof_pb::querier::v1::FlameGraphDiff;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::panic;
use std::sync::Mutex;
use std::vec::Vec;
use wasm_bindgen::prelude::*;
use std::sync::Arc;

//TODO: REMOVE
use std::fs;

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

impl TreeNodeV2 {
    pub fn clone(&self) -> TreeNodeV2 {
        TreeNodeV2 {
            fn_id: self.fn_id,
            node_id: self.node_id,
            slf: self.slf.clone(),
            total: self.total.clone(),
        }
    }
    pub fn set_total_and_self(&self, slf: Vec<i64>, total: Vec<i64>) -> TreeNodeV2 {
        let mut res = self.clone();
        res.slf = slf;
        res.total = total;
        return res;
    }
}

struct Tree {
    names: Vec<String>,
    names_map: HashMap<u64, usize>,
    nodes: HashMap<u64, Vec<Arc<TreeNodeV2>>>,
    sample_types: Vec<String>,
    max_self: Vec<i64>,
    nodes_num: i32,
}

impl Tree {
    pub fn total(&self) -> i64 {
        let mut total: i64 = 0;
        if !self.nodes.contains_key(&0) {
            return  0 as i64;
        }
        for c in 0..self.nodes.get(&0).unwrap().len() {
            let _c = &self.nodes.get(&0).unwrap()[c];
            total += _c.total[0];
        }
        total
    }
    pub fn add_name(&mut self, name: String, name_hash: u64) {
        if self.names_map.contains_key(&name_hash) {
            return;
        }
        self.names.push(name);
        self.names_map.insert(name_hash, self.names.len() - 1);
    }
}

fn find_node(id: u64, nodes: &Vec<Arc<TreeNodeV2>>) -> i32 {
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
        node: Arc<TreeNodeV2>,
        _max_self: &Vec<i64>,
        sample: &Sample,
        merge_self: bool,
    ) -> (TreeNodeV2, Vec<i64>) {
        let mut max_self = _max_self.clone();
        let mut res: TreeNodeV2 = TreeNodeV2 {
            fn_id: node.fn_id,
            node_id: node.node_id,
            slf: vec![0; node.slf.len()],
            total: vec![0; node.slf.len()],
        };
        for i in 0..self.from_idx.len() {
            if self.from_idx[i] == -1 {
                continue;
            }
            res.total[i] += sample.value[self.from_idx[i] as usize];
            if merge_self {
                res.slf[i] += sample.value[self.from_idx[i] as usize];
                for i in 0..max_self.len() {
                    if max_self[i] < node.slf[i] {
                        max_self[i] = node.slf[i];
                    }
                }
            }
        }
        (res, max_self)
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
            let mut fake_children: Vec<Arc<TreeNodeV2>> = Vec::new();
            let children = tree.nodes.get_mut(&parent_id).unwrap_or(&mut fake_children);
            let mut n = find_node(node_id, children);
            if n == -1 {
                children.push(Arc::new(TreeNodeV2 {
                    //parent_id,
                    fn_id: name_hash,
                    node_id,
                    slf: vec![0; tree.sample_types.len()],
                    total: vec![0; tree.sample_types.len()],
                }));
                let idx = children.len().clone() - 1;
                let new_node_and_max_self = m.merge_totals(
                    children.get(idx).unwrap().clone(),
                    tree.max_self.as_ref(),
                    s,
                    i == 0,
                );
                children[idx] = Arc::new(new_node_and_max_self.0);
                tree.max_self = new_node_and_max_self.1;
                n = idx as i32;
            } else if tree.nodes_num < 2000000 {
                m.merge_totals(
                    children.get_mut(n as usize).unwrap().clone(),
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
    let mut root_children: &Vec<Arc<TreeNodeV2>> = &Vec::new();
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
    static ref CTX: Mutex<HashMap<u32, Mutex<Tree>>> = Mutex::new(HashMap::new());
}

fn upsert_tree(ctx: &mut HashMap<u32, Mutex<Tree>>, id: u32, sample_types: Vec<String>) {
    if !ctx.contains_key(&id) {
        let _len = sample_types.len().clone();
        ctx.insert(
            id,
            Mutex::new(Tree {
                names: vec!["total".to_string(), "n/a".to_string()],
                names_map: HashMap::new(),
                nodes: HashMap::new(),
                sample_types,
                max_self: vec![0; _len],
                nodes_num: 1,
            }),
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
            let mut __node = tree.nodes.get_mut(&parent_id).unwrap().get_mut(n as usize).unwrap().clone();
            let mut _node = __node.as_ref().clone();
            _node.total[sample_type_index] += total[sample_type_index];
            _node.slf[sample_type_index] += slf[sample_type_index];
            tree.nodes.get_mut(&parent_id).unwrap()[n as usize] = Arc::new(_node);
        }
        if tree.nodes_num >= 2000000 {
            return;
        }
        if !tree.nodes.contains_key(&parent_id) {
            tree.nodes.insert(parent_id, Vec::new());
        }
        tree.nodes.get_mut(&parent_id).unwrap().push(Arc::new(TreeNodeV2 {
            fn_id,
            //parent_id,
            node_id,
            slf,
            total,
        }));
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

fn assert_positive(t: &Tree) -> bool{
    for n in t.nodes.keys() {
        for _n in 0..t.nodes.get(&n).unwrap().len() {
            for __n in 0..t.nodes.get(&n).unwrap()[_n].slf.len() {
                if t.nodes.get(&n).unwrap()[_n].slf[__n] < 0 {
                    return false;
                }
            }
        }
    }
    true
}

#[wasm_bindgen]
pub fn merge_prof(id: u32, bytes: &[u8], sample_type: String) {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id, vec![sample_type]);
        let mut tree = ctx.get_mut(&id).unwrap().lock().unwrap();
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
        let mut tree = ctx.get_mut(&id).unwrap().lock().unwrap();
        merge_trie(&mut tree, bytes, &sample_type);
        0
    });
    match result {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn diff_tree(id1: u32, id2: u32, sample_type: String) -> Vec<u8> {
    let mut ctx = CTX.lock().unwrap();
    let _ctx = &mut ctx;
    upsert_tree(_ctx, id1, vec![sample_type.clone()]);
    upsert_tree(_ctx, id2, vec![sample_type.clone()]);
    let mut t1 = _ctx.get(&id1).unwrap().lock().unwrap();
    let mut t2 = _ctx.get(&id2).unwrap().lock().unwrap();
    let mut is_positive = assert_positive(&t1);
    if !is_positive {
        panic!("Tree 1 is not positive");
    }
    is_positive = assert_positive(&t2);
    if!is_positive {
        panic!("Tree 2 is not positive");
    }


    for n in t1.names_map.keys() {
        if !t2.names_map.contains_key(&n) {
            t2.names.push(t1.names[*t1.names_map.get(&n).unwrap()].clone());
            let idx = t2.names.len() - 1;
            t2.names_map.insert(*n, idx);
        }
    }
    for n in t2.names_map.keys() {
        if !t1.names_map.contains_key(&n) {
            let idx = t2.names_map.get(&n).unwrap().clone();
            t1.names.push(t2.names[idx].clone());
            let idx2 = t1.names.len() - 1;
            t1.names_map.insert(*n, idx2);
        }
    }

    let keys = t1.nodes.keys().map(|x| (*x).clone()).collect::<Vec<_>>();
    for n in  keys {
        if !t2.nodes.contains_key(&n) {
            t2.nodes.insert(n, vec![]);
        }
        let lnodes = t1.nodes.get_mut(&n).unwrap();
        let rnodes = t2.nodes.get_mut(&n).unwrap();
        lnodes.sort_by(|x, y|
            if x.node_id < y.node_id { Ordering::Less } else { Ordering::Greater });
        rnodes.sort_by(|x, y|
            if x.node_id < y.node_id { Ordering::Less } else { Ordering::Greater });
        let mut i = 0;
        let mut j = 0;
        let mut new_t1_nodes: Vec<Arc<TreeNodeV2>> = vec![];
        let mut new_t2_nodes: Vec<Arc<TreeNodeV2>> = vec![];
        let t1_nodes = t1.nodes.get(&n).unwrap();
        let t2_nodes = t2.nodes.get(&n).unwrap();
        while i < t1_nodes.len() && j < t2_nodes.len() {
            if n == 0 {
                println!("{:?}:{:?} - {:?}:{:?}",
                         t1_nodes[i].node_id,
                    t1.names[*t1.names_map.get(&t1_nodes[i].fn_id).unwrap() as usize],
                         t2_nodes[j].node_id,
                         t2.names[*t2.names_map.get(&t2_nodes[j].fn_id).unwrap() as usize]
                )
            }

            if t1_nodes[i].node_id == t2_nodes[j].node_id {
                new_t1_nodes.push(t1_nodes[i].clone());
                new_t2_nodes.push(t2_nodes[j].clone());
                i += 1;
                j += 1;
                continue;
            }
            if t1_nodes[i].node_id < t2_nodes[j].node_id {
                new_t1_nodes.push(t1_nodes[i].clone());
                new_t2_nodes.push(Arc::new(TreeNodeV2{
                    node_id: t1_nodes[i].node_id,
                    fn_id: t1_nodes[i].fn_id,
                    slf: vec![0],
                    total: vec![0],
                }));
                i += 1;
            } else {
                new_t2_nodes.push(t2_nodes[j].clone());
                new_t1_nodes.push(Arc::new(TreeNodeV2{
                    node_id: t2_nodes[j].node_id,
                    fn_id: t2_nodes[j].fn_id,
                    slf: vec![0],
                    total: vec![0],
                }));
                j += 1;
            }
        }
        while i < t1_nodes.len() {
            new_t1_nodes.push(t1_nodes[i].clone());
            new_t2_nodes.push(Arc::new(TreeNodeV2{
                node_id: t1_nodes[i].node_id,
                fn_id: t1_nodes[i].fn_id,
                slf: vec![0],
                total: vec![0],
            }));
            i += 1;
        }
        while j < t2_nodes.len() {
            new_t2_nodes.push(t2_nodes[j].clone());
            new_t1_nodes.push(Arc::new(TreeNodeV2{
                node_id: t2_nodes[j].node_id,
                fn_id: t2_nodes[j].fn_id,
                slf: vec![0],
                total: vec![0],
            }));
            j+=1;
        }
        t1.nodes.insert(n, new_t1_nodes);
        t2.nodes.insert(n, new_t2_nodes);
    }

    for n in t2.nodes.keys().clone() {
        if!t1.nodes.contains_key(&n) {
            let mut new_t1_nodes: Vec<Arc<TreeNodeV2>> = vec![];
            for _n in t2.nodes.get(&n).unwrap() {
                new_t1_nodes.push(Arc::new(TreeNodeV2{
                    node_id: _n.node_id,
                    fn_id: _n.fn_id,
                    slf: vec![0],
                    total: vec![0],
                }))
            }
            t1.nodes.insert(*n, new_t1_nodes);
        }
    }

    let total_left = t1.total();
    let total_right = t2.total();
    let mut min_val = 0 as i64;
    let tn = Arc::new(TreeNodeV2{
        fn_id: 0,
        node_id: 0,
        slf: vec![0],
        total: vec![total_left],
    });
    let mut left_nodes = vec![tn];
    let tn2 = Arc::new(TreeNodeV2{
        fn_id: 0,
        node_id: 0,
        slf: vec![0],
        total: vec![total_right],
    });
    let mut right_nodes = vec![tn2];

    let mut x_left_offsets = vec![0 as i64];
    let mut x_right_offsets = vec![0 as i64];
    let mut levels = vec![0 as i64];
    let mut name_location_cache: HashMap<String, i64> = HashMap::new();
    let mut res = FlameGraphDiff::default();
    res.left_ticks = total_left;
    res.right_ticks = total_right;
    res.total = total_left + total_right;
    while left_nodes.len() > 0 {
        let left = left_nodes.pop().unwrap();
        let right = right_nodes.pop().unwrap();
        let mut x_left_offset = x_left_offsets.pop().unwrap();
        let mut x_right_offset = x_right_offsets.pop().unwrap();
        let level = levels.pop().unwrap();
        let mut name: String = "total".to_string();
        if left.fn_id != 0 {
            name = t1.names[t1.names_map.get(&left.fn_id).unwrap().clone() as usize].clone();
        }
        if left.total[0] >= min_val || right.total[0] >= min_val || name == "other" {
            let mut i = 0 as i64;
            if !name_location_cache.contains_key(&name) {
                res.names.push(name.clone().to_string());
                name_location_cache.insert(name, (res.names.len() - 1) as i64);
                i = res.names.len() as i64 - 1;
            } else {
                i = *name_location_cache.get(name.as_str()).unwrap();
            }
            if level == res.levels.len() as i64 {
                res.levels.push(Level::default())
            }
            if res.max_self < left.slf[0] {
                res.max_self = left.slf[0];
            }
            if res.max_self < right.slf[0] {
                res.max_self = right.slf[0];
            }
            let mut values = vec![x_left_offset, left.total[0], left.slf[0],
                                  x_right_offset, right.total[0], right.slf[0], i];
            res.levels[level as usize].values.extend(values);
            let mut other_left_total = 0 as i64;
            let mut other_right_total = 0 as i64;
            let mut nodes_len = 0;
            if t1.nodes.contains_key(&left.node_id) {
                nodes_len = t1.nodes.get(&left.node_id).unwrap().len().clone();
            }
            for j in 0..nodes_len {
                let _left = t1.nodes.get(&left.node_id).unwrap()[j].clone();
                let _right = t2.nodes.get(&left.node_id).unwrap()[j].clone();
                if _left.total[0] >= min_val || _right.total[0] >= min_val {
                    levels.insert(0, level + 1);
                    x_left_offsets.insert(0, x_left_offset);
                    x_right_offsets.insert(0, x_right_offset);
                    x_left_offset += _left.total[0].clone() as i64;
                    x_right_offset += _right.total[0].clone() as i64;
                    left_nodes.insert(0, _left.clone());
                    right_nodes.insert(0, _right.clone());
                } else {
                    other_left_total += _left.total[0] as i64;
                    other_right_total += _right.total[0] as i64;
                }
                if other_left_total > 0 || other_right_total > 0 {
                    levels.insert(0, level + 1);
                    t1.add_name("other".to_string(), 1);
                    x_left_offsets.insert(0, x_left_offset);
                    left_nodes.insert(0, Arc::new(TreeNodeV2{
                        fn_id: 1,
                        node_id: 1,
                        slf: vec![other_left_total as i64],
                        total: vec![other_left_total as i64],
                    }));
                    t2.add_name("other".to_string(), 1);
                    x_right_offsets.insert(0, x_right_offset);
                    right_nodes.insert(0, Arc::new(TreeNodeV2{
                        fn_id: 1,
                        node_id: 1,
                        slf: vec![other_right_total as i64],
                        total: vec![other_right_total as i64],
                    }));
                }
            }
        }

    }
    for i in 0..res.levels.len() {
        let mut j = 0;
        let mut prev = 0 as i64;
        while j < res.levels[i].values.len() {
            res.levels[i].values[j] -= prev;
            prev += res.levels[i].values[j] + res.levels[i].values[j+1];
            j += 7;
        }
        prev = 0;
        j = 3;
        while j < res.levels[i].values.len() {
            res.levels[i].values[j] -= prev;
            prev += res.levels[i].values[j] + res.levels[i].values[j+1];
            j += 7;
        }
    }

    res.encode_to_vec()
}



#[wasm_bindgen]
pub fn export_tree(id: u32, sample_type: String) -> Vec<u8> {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        let mut res = SelectMergeStacktracesResponse::default();
        upsert_tree(&mut ctx, id, vec![sample_type.clone()]);
        let tree = ctx.get_mut(&id).unwrap().lock().unwrap();
        let mut fg = FlameGraph::default();
        fg.names = tree.names.clone();
        fg.max_self = tree.max_self[0 /* TODO */];
        fg.total = 0;
        let mut root_children: &Vec<Arc<TreeNodeV2>> = &vec![];
        if tree.nodes.contains_key(&(0u64)) {
            root_children = tree.nodes.get(&(0u64)).unwrap();
        }
        for n in root_children.iter() {
            fg.total += n.total[0 /*TODO*/] as i64;
        }
        bfs(&tree, &mut fg.levels, sample_type.clone());
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
            if bin_prof.len() >= 2 && bin_prof[0] == 0x1f && bin_prof[1] == 0x8b {
                let mut decompressed = Vec::new();
                let mut decoder = flate2::read::GzDecoder::new(&bin_prof[..]);
                decoder.read_to_end(&mut decompressed).unwrap();
                let mut prof = Profile::decode(std::io::Cursor::new(decompressed)).unwrap();
                merger.merge(&mut prof);
            }else {
                let mut prof = Profile::decode(bin_prof).unwrap();
                merger.merge(&mut prof);
            }

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
