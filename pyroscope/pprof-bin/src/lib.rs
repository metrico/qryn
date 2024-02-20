#![allow(unused_assignments)]
mod utils;
mod ch64;

use lazy_static::lazy_static;
use pprof_pb::google::v1::Function;
use pprof_pb::google::v1::Location;
use pprof_pb::google::v1::Profile;
use pprof_pb::querier::v1::Level;
use pprof_pb::querier::v1::FlameGraph;
use pprof_pb::querier::v1::SelectMergeStacktracesResponse;
use std::panic;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::slice::SliceIndex;
use std::sync::Mutex;
use std::vec::Vec;
use wasm_bindgen::prelude::*;
use ch64::city_hash_64;
use ch64::read_uint64_le;

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
    parent_id: u64,
    fn_id: u64,
    node_id: u64,
    slf: u64,
    total: u64
}

struct Tree {
    names: Vec<String>,
    names_map: HashMap<u64, usize>,
    nodes: HashMap<u64,Vec<TreeNodeV2>>,
    sample_type: String,
    max_self: i64,
    nodes_num: i32
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
        node_bytes[i+8] = ((name_hash >> (i * 8)) & 0xFF) as u8;
    }
    let mut _level = level;
    if _level > 511 {
        _level = 511;
    }
    (city_hash_64(&node_bytes[0..]) >> 9) | ((_level as u64) << 55)
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

    let mut value_idx: i32 = -1;
    for i in 0..p.sample_type.len() {
        let sample_type = format!(
            "{}:{}",
            p.string_table[p.sample_type[i].r#type as usize],
            p.string_table[p.sample_type[i].unit as usize]
        );
        if tree.sample_type == sample_type {
            value_idx = i as i32;
            break;
        }
    }

    if value_idx == -1 {
        return;
    }
    let u_value_idx = value_idx as usize;
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
            let node_id = get_node_id(
                parent_id, name_hash,(s.location_id.len() - i) as u16
            );
            if !tree.nodes.contains_key(&parent_id) && tree.nodes_num < 2000000{
                tree.nodes.insert(parent_id, Vec::new());
            }
            let mut slf: u64 = 0;
            if i == 0 {
                slf = s.value[u_value_idx] as u64;
            }
            if tree.max_self < slf as i64 {
                tree.max_self = slf as i64;
            }
            let mut fake_children: Vec<TreeNodeV2> = Vec::new();
            let mut children = tree.nodes
                .get_mut(&parent_id)
                .unwrap_or(&mut fake_children);
            let n = find_node(node_id, children);
            if n == -1 {
                children.push(TreeNodeV2 {
                    parent_id,
                    fn_id: name_hash,
                    node_id,
                    slf,
                    total: s.value[u_value_idx] as u64
                });
            } else if tree.nodes_num < 2000000 {
                children.get_mut(n as usize).unwrap().total += s.value[u_value_idx] as u64;
                children.get_mut(n as usize).unwrap().slf += slf;
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



fn bfs(t: &Tree, res: &mut Vec<Level>) {
    let mut total: u64 = 0;
    let mut root_children: &Vec<TreeNodeV2> = &Vec::new();
    if t.nodes.contains_key(&(0u64)) {
        root_children = t.nodes.get(&(0u64)).unwrap();
    }
    for i in root_children.iter() {
        total += i.total;
    }
    let mut lvl = Level::default();
    lvl.values.extend([0, total as i64, 0, 0]);
    res.push(lvl);

    let totalNode: TreeNodeV2 = TreeNodeV2 {
        slf: 0,
        total: total,
        node_id: 0,
        fn_id: 0,
        parent_id: 0
    };
    let mut prepend_map: HashMap<u64, u64> = HashMap::new();

    let mut reviewed: HashSet<u64> = HashSet::new();

    let mut refs: Vec<&TreeNodeV2> = vec![&totalNode];
    let mut refLen: usize = 1;
    let mut i = 0;
    while refLen > 0 {
        i+=1;
        let mut prepend: u64 = 0;
        let _refs = refs.clone();
        refs.clear();
        lvl = Level::default();
        for parent in _refs.iter() {
            prepend += prepend_map.get(&parent.node_id).unwrap_or(&0);
            let opt = t.nodes.get(&parent.node_id);

            if opt.is_none() {
                prepend += parent.total;
                continue;
            }
            let mut totalSum: u64 = 0;
            for n in opt.unwrap().iter() {
                if reviewed.contains(&n.node_id) {
                    // PANIC!!! WE FOUND A LOOP
                    return;
                } else {
                    reviewed.insert(n.node_id);
                }
                prepend_map.insert(n.node_id, prepend);
                refs.push(n);
                totalSum += n.total;
                lvl.values.extend(
                    [
                        prepend as i64,
                        n.total as i64,
                        n.slf as i64,
                        *t.names_map.get(&n.fn_id).unwrap_or(&1) as i64
                    ]
                );
                prepend = 0;
            }
            prepend += parent.slf;
        }
        res.push(lvl.clone());
        refLen = refs.len();
    }
}

lazy_static! {
    static ref CTX: Mutex<HashMap<u32, Tree>> = Mutex::new(HashMap::new());
}

fn upsert_tree(ctx: &mut HashMap<u32,Tree>, id: u32) {
    if !ctx.contains_key(&id) {
        ctx.insert(
            id,
            Tree {
                names: vec!["total".to_string(), "n/a".to_string()],
                names_map: HashMap::new(),
                nodes: HashMap::new(),
                sample_type: "".to_string(),
                max_self: 0,
                nodes_num: 1
            },
        );
    }
}

fn merge_trie(tree: &mut Tree, bytes: &[u8]) {
    let mut size = 0;
    let mut offs = 0;
    (size, offs) = read_uleb128(bytes);
    for _i in 0..size {
        let id = read_uint64_le(&bytes[offs..]);
        offs += 8;
        let mut _offs: usize = 0;
        let mut _size: usize = 0;
        (_size, _offs) = read_uleb128(&bytes[offs..]);
        offs += _offs;
        if !tree.names_map.contains_key(&id) && tree.names.len() < 2000000 {
            tree.names.push(String::from_utf8_lossy(&bytes[offs..offs + _size]).to_string());
            tree.names_map.insert(id, tree.names.len() - 1);
        }
        offs += _size;
    }

    let mut _offs: usize = 0;
    (size, _offs) = read_uleb128(&bytes[offs..]);
    offs += _offs;
    for _i in 0..size {
        let parent_id = read_uint64_le(&bytes[offs..]);
        offs += 8;
        let fn_id = read_uint64_le(&bytes[offs..]);
        offs += 8;
        let node_id = read_uint64_le(&bytes[offs..]);
        offs += 8;
        let slf = read_uint64_le(&bytes[offs..]);
        offs += 8;
        let total = read_uint64_le(&bytes[offs..]);
        if tree.max_self < slf as i64 {
            tree.max_self = slf as i64;
        }
        offs += 8;
        if tree.nodes.contains_key(&parent_id) {
            let n = find_node(node_id, tree.nodes.get(&parent_id).unwrap());
            if n != -1 {
                tree.nodes.get_mut(&parent_id).unwrap().get_mut(n as usize).unwrap().total += total;
                tree.nodes.get_mut(&parent_id).unwrap().get_mut(n as usize).unwrap().slf += slf;
            } else if tree.nodes_num < 2000000 {
                tree.nodes.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
                    fn_id,
                    parent_id,
                    node_id,
                    slf,
                    total
                });
                tree.nodes_num+=1;
            }

        } else if tree.nodes_num < 2000000 {
            tree.nodes.insert(parent_id, Vec::new());
            tree.nodes.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
                fn_id,
                parent_id,
                node_id,
                slf,
                total
            });
            tree.nodes_num+=1;
        }
    }
}

#[wasm_bindgen]
pub fn merge_prof(id: u32, bytes: &[u8], sample_type: String) {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id);
        let mut tree = ctx.get_mut(&id).unwrap();
        tree.sample_type = sample_type;
        let prof = Profile::decode(bytes).unwrap();
        merge(&mut tree, &prof);
    });
    match p {
        Ok(res) => {}
        Err(err) => panic!(err)

    }
}

#[wasm_bindgen]
pub fn merge_tree(id: u32, bytes: &[u8]) {
    let result = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        upsert_tree(&mut ctx, id);
        let mut tree = ctx.get_mut(&id).unwrap();
        merge_trie(&mut tree, bytes);
        0
    });
    match result {
        Ok(res) => {}
        Err(err) => panic!(err)
    }
}

#[wasm_bindgen]
pub fn export_tree(id: u32) -> Vec<u8> {
    let p = panic::catch_unwind(|| {
        let mut ctx = CTX.lock().unwrap();
        let mut res = SelectMergeStacktracesResponse::default();
        upsert_tree(&mut ctx, id);
        let mut tree = ctx.get_mut(&id).unwrap();
        let mut fg = FlameGraph::default();
        fg.names = tree.names.clone();
        fg.max_self = tree.max_self;
        fg.total = 0;
        let mut root_children: &Vec<TreeNodeV2> = &vec![];
        if tree.nodes.contains_key(&(0u64)) {
            root_children = tree.nodes.get(&(0u64)).unwrap();
        }
        for n in root_children.iter() {
            fg.total += n.total as i64;
        }
        bfs(tree, &mut fg.levels);
        res.flamegraph = Some(fg);
        return  res.encode_to_vec();
    });
    match p {
        Ok(res) => return res,
        Err(err) => panic!(err)
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
