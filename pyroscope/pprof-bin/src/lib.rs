#![allow(unused_assignments)]
mod ch64;
mod merge;
pub mod utest;

use ch64::city_hash_64;
use ch64::read_uint64_le;
use lazy_static::lazy_static;
use pprof_pb::google::v1::Function;
use pprof_pb::google::v1::Location;
use pprof_pb::google::v1::Profile;
use pprof_pb::google::v1::Sample;
use pprof_pb::querier::v1::FlameGraph;
use pprof_pb::querier::v1::FlameGraphDiff;
use pprof_pb::querier::v1::Level;
use pprof_pb::querier::v1::SelectMergeStacktracesResponse;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::panic;
use std::sync::Arc;
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
    pprof: Profile,
}

impl Tree {
    pub fn total(&self) -> Vec<i64> {
        if let Some(children) = self.nodes.get(&0) {
            let mut total = vec![0; children[0].total.len()];
            for child in children.iter() {
                for (t, &child_total) in total.iter_mut().zip(&child.total) {
                    *t += child_total;
                }
            }
            total
        } else {
            Vec::new()
        }
    }

    pub fn add_name(&mut self, name: String, name_hash: u64) {
        if let std::collections::hash_map::Entry::Vacant(entry) = self.names_map.entry(name_hash) {
            self.names.push(name);
            entry.insert(self.names.len() - 1);
        }
    }
}

fn find_node(id: u64, nodes: &[Arc<TreeNodeV2>]) -> Option<usize> {
    nodes.iter().position(|node| node.node_id == id)
}

fn get_node_id(parent_id: u64, name_hash: u64, level: u16) -> u64 {
    let mut node_bytes = [0u8; 16];
    node_bytes[..8].copy_from_slice(&parent_id.to_le_bytes());
    node_bytes[8..].copy_from_slice(&name_hash.to_le_bytes());

    let adjusted_level = level.min(511);
    (city_hash_64(&node_bytes) >> 9) | ((adjusted_level as u64) << 55)
}

struct MergeTotalsProcessor {
    from_idx: Vec<Option<usize>>,
}

impl MergeTotalsProcessor {
    fn new(tree: &Tree, p: &Profile) -> MergeTotalsProcessor {
        let from_idx: Vec<Option<usize>> = tree
            .sample_types
            .iter()
            .map(|sample_type_to| {
                p.sample_type.iter().position(|sample_type| {
                    let sample_type_from = format!(
                        "{}:{}",
                        p.string_table[sample_type.r#type as usize],
                        p.string_table[sample_type.unit as usize]
                    );
                    sample_type_from == *sample_type_to
                })
            })
            .collect();

        MergeTotalsProcessor { from_idx }
    }

    fn merge_totals(
        &self,
        node: Arc<TreeNodeV2>,
        max_self: &mut Vec<i64>,
        sample: &Sample,
        merge_self: bool,
    ) -> TreeNodeV2 {
        let mut res: TreeNodeV2 = TreeNodeV2 {
            fn_id: node.fn_id,
            node_id: node.node_id,
            slf: vec![0; node.slf.len()],
            total: vec![0; node.slf.len()],
        };

        for (i, opt_idx) in self.from_idx.iter().enumerate() {
            if let Some(from_idx) = opt_idx {
                res.total[i] += sample.value[*from_idx];
                if merge_self {
                    res.slf[i] += sample.value[*from_idx];
                    if max_self[i] < node.slf[i] {
                        max_self[i] = node.slf[i];
                    }
                }
            }
        }

        res
    }
}

fn merge(tree: &mut Tree, p: &Profile) {
    let functions: HashMap<u64, &Function> = p.function.iter().map(|f| (f.id, f)).collect();
    let locations: HashMap<u64, &Location> = p.location.iter().map(|l| (l.id, l)).collect();

    let merge_processor = MergeTotalsProcessor::new(tree, p);

    for location in &p.location {
        if let Some(function) = functions.get(&location.line[0].function_id) {
            let line = &p.string_table[function.name as usize];
            let line_hash = city_hash_64(line.as_bytes());

            if let std::collections::hash_map::Entry::Vacant(entry) =
                tree.names_map.entry(line_hash)
            {
                tree.names.push(line.clone());
                entry.insert(tree.names.len() - 1);
            }
        }
    }

    for sample in &p.sample {
        let mut parent_id: u64 = 0;

        for (i, &location_id) in sample.location_id.iter().enumerate().rev() {
            if let Some(location) = locations.get(&location_id) {
                if let Some(function) = functions.get(&location.line[0].function_id) {
                    let name = &p.string_table[function.name as usize];
                    let name_hash = city_hash_64(name.as_bytes());
                    let node_id =
                        get_node_id(parent_id, name_hash, (sample.location_id.len() - i) as u16);

                    let children = tree.nodes.entry(parent_id).or_insert_with(Vec::new);

                    match find_node(node_id, children) {
                        Some(index) => {
                            if tree.nodes_num < 2_000_000 {
                                let updated_node = merge_processor.merge_totals(
                                    children[index].clone(),
                                    &mut tree.max_self,
                                    sample,
                                    i == 0,
                                );
                                children[index] = Arc::new(updated_node);
                                tree.nodes_num += 1;
                            }
                        }
                        None => {
                            if tree.nodes_num < 2_000_000 {
                                let new_node = TreeNodeV2 {
                                    fn_id: name_hash,
                                    node_id,
                                    slf: vec![0; tree.sample_types.len()],
                                    total: vec![0; tree.sample_types.len()],
                                };

                                let new_node_arc = Arc::new(new_node);
                                let updated_node = merge_processor.merge_totals(
                                    new_node_arc.clone(),
                                    &mut tree.max_self,
                                    sample,
                                    i == 0,
                                );

                                children.push(Arc::new(updated_node));
                                tree.nodes_num += 1;
                            }
                        }
                    }

                    parent_id = node_id;
                }
            }
        }
    }
}

fn read_uleb128(bytes: &[u8]) -> (usize, usize) {
    let mut result = 0usize;
    let mut shift = 0;

    for (index, &byte) in bytes.iter().enumerate() {
        result |= ((byte & 0x7f) as usize) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return (result, index + 1);
        }
    }

    (result, bytes.len())
}

fn bfs(t: &Tree, res: &mut Vec<Level>, sample_type: String) {
    let sample_type_index = match t.sample_types.iter().position(|x| x == &sample_type) {
        Some(index) => index,
        None => return,
    };

    let empty_vec = Vec::new();
    let root_children = t.nodes.get(&0u64).unwrap_or(&empty_vec);

    let total: i64 = root_children
        .iter()
        .map(|child| child.total[sample_type_index])
        .sum();

    res.push(Level {
        values: vec![0, total, 0, 0],
    });

    let mut totals = vec![0; t.sample_types.len()];
    totals[sample_type_index] = total;

    let total_node = TreeNodeV2 {
        slf: vec![0; t.sample_types.len()],
        total: totals,
        node_id: 0,
        fn_id: 0,
    };

    let mut prepend_map: HashMap<u64, i64> = HashMap::new();
    let mut reviewed: HashSet<u64> = HashSet::new();

    let mut current_level_nodes = vec![&total_node];

    while !current_level_nodes.is_empty() {
        let mut next_level_nodes = Vec::new();
        let mut prepend: i64 = 0;
        let mut lvl = Level::default();

        for parent in current_level_nodes {
            prepend += *prepend_map.get(&parent.node_id).unwrap_or(&0);

            if let Some(children) = t.nodes.get(&parent.node_id) {
                for child in children {
                    if !reviewed.insert(child.node_id) {
                        // Loop detected, exit early
                        return;
                    }

                    prepend_map.insert(child.node_id, prepend);
                    next_level_nodes.push(child.as_ref());

                    lvl.values.extend_from_slice(&[
                        prepend,
                        child.total[sample_type_index],
                        child.slf[sample_type_index],
                        *t.names_map.get(&child.fn_id).unwrap_or(&1) as i64,
                    ]);

                    prepend = 0;
                }
            } else {
                prepend += parent.total[sample_type_index];
                continue;
            }

            prepend += parent.slf[sample_type_index];
        }

        res.push(lvl);
        current_level_nodes = next_level_nodes;
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
                pprof: Profile::default(),
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
        res.0.clone()
    }

    fn read_string(&mut self) -> String {
        let size = self.read_size();
        let string = String::from_utf8_lossy(&self.bytes[self.offs..self.offs + size]).to_string();
        self.offs += size;
        string
    }

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
    fn read_blob(&mut self) -> &[u8] {
        let size = self.read_size();
        let string = &self.bytes[self.offs..self.offs + size];
        self.offs += size;
        string
    }
    fn read_blob_list(&mut self) -> Vec<&[u8]> {
        let mut res = Vec::new();
        while self.offs < self.bytes.len() {
            let uleb = read_uleb128(&self.bytes[self.offs..]);
            self.offs += uleb.1;
            let _size = uleb.0;
            let string = &self.bytes[self.offs..self.offs + _size];
            self.offs += _size;
            res.push(string);
        }
        res
    }
}

fn merge_trie(tree: &mut Tree, bytes: &[u8], sample_type: &str) {
    let sample_type_index = match tree.sample_types.iter().position(|x| x == sample_type) {
        Some(index) => index,
        None => return,
    };

    let mut reader = TrieReader::new(bytes);

    for _ in 0..reader.read_size() {
        let id = reader.read_uint64_le();
        let func = reader.read_string();
        if tree.names_map.len() < 2_000_000 {
            if !tree.names_map.contains_key(&id) {
                tree.names.push(func);
                tree.names_map.insert(id, tree.names.len() - 1);
            }
        }
    }

    for _ in 0..reader.read_size() {
        let parent_id = reader.read_uint64_le();
        let fn_id = reader.read_uint64_le();
        let node_id = reader.read_uint64_le();
        let slf_value = reader.read_uint64_le() as i64;
        let total_value = reader.read_uint64_le() as i64;

        if tree.max_self[sample_type_index] < slf_value {
            tree.max_self[sample_type_index] = slf_value;
        }

        let mut slf = vec![0; tree.sample_types.len()];
        slf[sample_type_index] = slf_value;

        let mut total = vec![0; tree.sample_types.len()];
        total[sample_type_index] = total_value;

        if let Some(children) = tree.nodes.get_mut(&parent_id) {
            if let Some(pos) = find_node(node_id, children) {
                let node_arc = &children[pos];
                let mut node = node_arc.as_ref().clone();

                node.slf[sample_type_index] += slf_value;
                node.total[sample_type_index] += total_value;

                children[pos] = Arc::new(node);
                continue;
            }
        }

        if tree.nodes_num >= 2_000_000 {
            return;
        }

        let children = tree.nodes.entry(parent_id).or_insert_with(Vec::new);
        children.push(Arc::new(TreeNodeV2 {
            fn_id,
            node_id,
            slf,
            total,
        }));

        tree.nodes_num += 1;
    }
}

fn assert_positive(t: &Tree) -> bool {
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
    upsert_tree(&mut ctx, id1, vec![sample_type.clone()]);
    upsert_tree(&mut ctx, id2, vec![sample_type.clone()]);

    let mut t1 = ctx.get(&id1).unwrap().lock().unwrap();
    let mut t2 = ctx.get(&id2).unwrap().lock().unwrap();

    assert_tree_positive(&t1, "Tree 1");
    assert_tree_positive(&t2, "Tree 2");

    synchronize_names(&mut t1, &mut t2);
    merge_nodes(&mut t1, &mut t2);

    let flame_graph_diff = compute_flame_graph_diff(&t1, &t2);

    flame_graph_diff.encode_to_vec()
}

fn assert_tree_positive(tree: &Tree, tree_name: &str) {
    if !assert_positive(tree) {
        panic!("{} is not positive", tree_name);
    }
}

fn synchronize_names(t1: &mut Tree, t2: &mut Tree) {
    let mut names_to_add_to_t2 = vec![];
    for (&id, &idx) in &t1.names_map {
        if !t2.names_map.contains_key(&id) {
            names_to_add_to_t2.push((id, t1.names[idx].clone()));
        }
    }

    for (id, name) in names_to_add_to_t2 {
        let idx = t2.names.len();
        t2.names.push(name);
        t2.names_map.insert(id, idx);
    }

    let mut names_to_add_to_t1 = vec![];
    for (&id, &idx) in &t2.names_map {
        if !t1.names_map.contains_key(&id) {
            names_to_add_to_t1.push((id, t2.names[idx].clone()));
        }
    }

    for (id, name) in names_to_add_to_t1 {
        let idx = t1.names.len();
        t1.names.push(name);
        t1.names_map.insert(id, idx);
    }
}

fn merge_nodes(t1: &mut Tree, t2: &mut Tree) {
    let keys: Vec<u64> = t1.nodes.keys().cloned().collect();

    for key in keys {
        let t1_children = t1.nodes.entry(key).or_insert_with(Vec::new);
        let t2_children = t2.nodes.entry(key).or_insert_with(Vec::new);

        t1_children.sort_by_key(|n| n.node_id);
        t2_children.sort_by_key(|n| n.node_id);

        let (new_t1_nodes, new_t2_nodes) = merge_children(t1_children, t2_children);
        t1.nodes.insert(key, new_t1_nodes);
        t2.nodes.insert(key, new_t2_nodes);
    }
}

fn merge_children(
    t1_nodes: &[Arc<TreeNodeV2>],
    t2_nodes: &[Arc<TreeNodeV2>],
) -> (Vec<Arc<TreeNodeV2>>, Vec<Arc<TreeNodeV2>>) {
    let mut new_t1_nodes = Vec::new();
    let mut new_t2_nodes = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while i < t1_nodes.len() && j < t2_nodes.len() {
        if t1_nodes[i].node_id == t2_nodes[j].node_id {
            new_t1_nodes.push(t1_nodes[i].clone());
            new_t2_nodes.push(t2_nodes[j].clone());
            i += 1;
            j += 1;
        } else if t1_nodes[i].node_id < t2_nodes[j].node_id {
            new_t1_nodes.push(t1_nodes[i].clone());
            new_t2_nodes.push(create_empty_node(&t1_nodes[i]));
            i += 1;
        } else {
            new_t2_nodes.push(t2_nodes[j].clone());
            new_t1_nodes.push(create_empty_node(&t2_nodes[j]));
            j += 1;
        }
    }

    while i < t1_nodes.len() {
        new_t1_nodes.push(t1_nodes[i].clone());
        new_t2_nodes.push(create_empty_node(&t1_nodes[i]));
        i += 1;
    }

    while j < t2_nodes.len() {
        new_t2_nodes.push(t2_nodes[j].clone());
        new_t1_nodes.push(create_empty_node(&t2_nodes[j]));
        j += 1;
    }

    (new_t1_nodes, new_t2_nodes)
}

fn create_empty_node(node: &Arc<TreeNodeV2>) -> Arc<TreeNodeV2> {
    Arc::new(TreeNodeV2 {
        node_id: node.node_id,
        fn_id: node.fn_id,
        slf: vec![0],
        total: vec![0],
    })
}

fn compute_flame_graph_diff(t1: &Tree, t2: &Tree) -> FlameGraphDiff {
    let mut res = FlameGraphDiff::default();
    res.left_ticks = t1.total()[0];
    res.right_ticks = t2.total()[0];
    res.total = res.left_ticks + res.right_ticks;

    let mut left_nodes = vec![Arc::new(TreeNodeV2 {
        fn_id: 0,
        node_id: 0,
        slf: vec![0],
        total: vec![res.left_ticks],
    })];

    let mut right_nodes = vec![Arc::new(TreeNodeV2 {
        fn_id: 0,
        node_id: 0,
        slf: vec![0],
        total: vec![res.right_ticks],
    })];

    let mut levels = vec![0];
    let mut x_left_offsets = vec![0];
    let mut x_right_offsets = vec![0];
    let mut name_location_cache: HashMap<String, i64> = HashMap::new();

    while let (Some(left), Some(right)) = (left_nodes.pop(), right_nodes.pop()) {
        let x_left_offset = x_left_offsets.pop().unwrap();
        let x_right_offset = x_right_offsets.pop().unwrap();
        let level = levels.pop().unwrap();

        let name = if left.fn_id == 0 {
            "total".to_string()
        } else {
            t1.names[*t1.names_map.get(&left.fn_id).unwrap()].clone()
        };

        let name_idx = *name_location_cache.entry(name.clone()).or_insert_with(|| {
            res.names.push(name);
            (res.names.len() - 1) as i64
        });

        if res.levels.len() <= level {
            res.levels.push(Level::default());
        }

        res.levels[level].values.extend_from_slice(&[
            x_left_offset,
            left.total[0],
            left.slf[0],
            x_right_offset,
            right.total[0],
            right.slf[0],
            name_idx,
        ]);

        if let Some(children_left) = t1.nodes.get(&left.node_id) {
            let empty_vec = Vec::new();
            let children_right = t2.nodes.get(&right.node_id).unwrap_or(&empty_vec);
            for (child_left, child_right) in children_left.iter().zip(children_right.iter()) {
                left_nodes.push(child_left.clone());
                right_nodes.push(child_right.clone());
                x_left_offsets.push(x_left_offset + child_left.total[0]);
                x_right_offsets.push(x_right_offset + child_right.total[0]);
                levels.push(level + 1);
            }
        }
    }

    res
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
pub fn merge_trees_pprof(id: u32, payload: &[u8]) {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id, vec![]);
        let mut tree = ctx.get_mut(&id).unwrap().lock().unwrap();
        let mut reader = TrieReader::new(payload);
        let bin_profs = reader.read_blob_list();
        let mut merger = merge::ProfileMerge::new();
        merger.merge(&mut tree.pprof);
        for bin_prof in bin_profs {
            if bin_prof.len() >= 2 && bin_prof[0] == 0x1f && bin_prof[1] == 0x8b {
                let mut decompressed = Vec::new();
                let mut decoder = flate2::read::GzDecoder::new(&bin_prof[..]);
                decoder.read_to_end(&mut decompressed).unwrap();
                let mut prof = Profile::decode(std::io::Cursor::new(decompressed)).unwrap();
                merger.merge(&mut prof);
            } else {
                let mut prof = Profile::decode(bin_prof).unwrap();
                merger.merge(&mut prof);
            }
        }
        let res = merger.profile();
        tree.pprof = res;
    });
    match p {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

#[wasm_bindgen]
pub fn export_trees_pprof(id: u32) -> Vec<u8> {
    let mut ctx = CTX.lock().unwrap();
    upsert_tree(&mut ctx, id, vec![]);
    let tree = ctx.get_mut(&id).unwrap().lock().unwrap();
    tree.pprof.encode_to_vec()
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
