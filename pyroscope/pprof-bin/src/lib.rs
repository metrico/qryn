#![allow(unused_assignments)]
mod utils;

use lazy_static::lazy_static;
use pprof_pb::google::v1::Function;
use pprof_pb::google::v1::Location;
use pprof_pb::google::v1::Profile;
use pprof_pb::querier::v1::FlameGraph;
use pprof_pb::querier::v1::Level;
use pprof_pb::querier::v1::SelectMergeStacktracesResponse;
use prost::Message;
use std::collections::HashMap;
use std::sync::Mutex;
use std::vec::Vec;
use wasm_bindgen::prelude::*;
use cityhash::cityhash_1::city_hash_64;

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
}

fn hash_128_to_64(l: u64, h: u64) -> u64 {
    let kMul: u64 = 0x9ddfea08eb382d69;
    let mut a = (l ^ h) * kMul;
    a ^= (a >> 47);
    let mut b = (h ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    b
}

unsafe fn merge(tree: &mut Tree, p: &Profile) {
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
            let name_hash = city_hash_64(
                p.string_table[functions[&location.line[0].function_id].name as usize].as_bytes()
            );
            let node_id = hash_128_to_64(parent_id, name_hash);
            if !tree.nodes.contains_key(&parent_id) {
                tree.nodes.insert(parent_id, Vec::new());
            }
            let mut slf: u64 = 0;
            if i == 0 {
                slf = s.value[u_value_idx] as u64;
            }
            if tree.max_self < slf as i64 {
                tree.max_self = slf as i64;
            }
            tree.nodes.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
                parent_id,
                fn_id: name_hash,
                node_id,
                slf,
                total: s.value[u_value_idx] as u64
            });
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

fn read_uint64(bytes: &[u8]) -> (u64) {
    let mut res: u64 = 0;
    for i in 0..8 {
        res |= (bytes[i] as u64) << (i * 8);
    }
    res
}


unsafe fn bfs(t: &Tree, res: &mut Vec<Level>) {


    let mut total: u64 = 0;
    for i in t.nodes.get(&(0u64)).unwrap().iter() {
        total += i.total;
    }
    let mut lvl = Level::default();
    lvl.values.extend([0, total, 0, 0]);
    res.push(lvl);

    let totalNode: TreeNodeV2 = TreeNodeV2 {
        slf: 0,
        total: total,
        node_id: 0,
        fn_id: 0,
        parent_id: 0
    };
    let mut prepend_map: HashMap<u64, u64> = HashMap::new();

    let mut refs: Vec<&TreeNodeV2> = vec![&totalNode];
    let mut refLen: usize = 1;
    while refLen > 0 {
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
                let current_prepend = (prepend_map.get(&n.node_id).unwrap_or(&0u64) + prepend);
                prepend = 0;
                prepend_map.insert(n.node_id, current_prepend);
                refs.push(n);
                totalSum += n.total;
                lvl.values.extend(
                    [
                        current_prepend as i64,
                        n.total as i64,
                        n.slf as i64,
                        t.names[&n.fn_id] as i64
                    ]
                );
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

fn upsert_tree(id: u32) {
    let mut ctx = CTX.lock().unwrap();
    if !ctx.contains_key(&id) {
        ctx.insert(
            id,
            Tree {
                names: vec!["total".to_string()],
                names_map: HashMap::new(),
                nodes: HashMap::new(),
                sample_type: "".to_string(),
                max_self: 0,
            },
        );
    }
}

#[wasm_bindgen]
pub unsafe fn merge_prof(id: u32, bytes: &[u8], sample_type: String) {
    upsert_tree(id);
    let mut ctx = CTX.lock().unwrap();
    let mut tree = ctx.get_mut(&id).unwrap();
    tree.sample_type = sample_type;

    let prof = Profile::decode(bytes).unwrap();
    merge(&mut tree, &prof);
}

pub unsafe fn merge_tree(id: u32, bytes: &[u8]) {
    upsert_tree(id);
    let mut ctx = CTX.lock().unwrap();
    let mut tree = ctx.get_mut(&id).unwrap();
    let mut size = 0;
    let mut offs = 0;
    (size, offs) = read_uleb128(bytes);
    for _i in 0..size {
        let id = read_uint64(&bytes[offs..]);
        offs += 8;
        let mut _offs: usize = 0;
        let mut _size: usize = 0;
        (_size, _offs) = read_uleb128(&bytes[offs..]);
        offs += _offs;
        if tree.names_map.contains_key(&id) {
            tree.names.push(String::from_utf8_lossy(&bytes[offs..offs + _size]).to_string());
            tree.names_map.insert(id, tree.names.len() - 1);
        }
        offs += _size;
    }

    let mut _offs: usize = 0;
    (size, _offs) = read_uleb128(&bytes[offs..]);
    offs += _offs;
    for _i in 0..size {
        let parent_id = read_uint64(&bytes[offs..]);
        offs += 8;
        let fn_id = read_uint64(&bytes[offs..]);
        offs += 8;
        let node_id = read_uint64(&bytes[offs..]);
        offs += 8;
        let slf = read_uint64(&bytes[offs..]);
        offs += 8;
        let total = read_uint64(&bytes[offs..]);
        if tree.max_self < slf as i64 {
            tree.max_self = slf as i64;
        }
        offs += 8;
        if tree.contains_key(&parent_id) {
            tree.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
                fn_id,
                parent_id,
                node_id,
                slf,
                total
            })
        } else {
            tree.insert(parent_id, Vec::new());
            tree.get_mut(&parent_id).unwrap().push(TreeNodeV2 {
                fn_id,
                parent_id,
                node_id,
                slf,
                total
            });
        }
    }
}

#[wasm_bindgen]
pub unsafe fn export_tree(id: u32, sample_type: String) -> Vec<u8> {
    let mut ctx = CTX.lock().unwrap();
    let mut res = SelectMergeStacktracesResponse::default();
    if !ctx.contains_key(&id) {
        return res.encode_to_vec();
    }
    let tree = ctx.get(&id).unwrap();
    let mut fg = FlameGraph::default();
    fg.names = tree.names.clone();
    fg.max_self = tree.max_self;
    fg.total = 0;
    for n in tree.get(&(0u64)).unwrap().iter() {
        fg.total += n.total as i64;
    }
    bfs(tree, &mut fg.levels);
    res.flamegraph = Some(fg);
    res.encode_to_vec()
}

#[wasm_bindgen]
pub unsafe fn drop_tree(id: u32) {
    let mut ctx = CTX.lock().unwrap();
    if ctx.contains_key(&id) {
        ctx.remove(&id);
    }
}

#[wasm_bindgen]
pub fn init_panic_hook() {
    console_error_panic_hook::set_once();
}

#[cfg(test)]
mod tests {
    use cityhash::cityhash_1::city_hash_64;

    #[test]
    fn it_works() {
        print!("{}", city_hash_64(b"123"))
    }
}
