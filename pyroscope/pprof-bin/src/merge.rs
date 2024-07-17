use crate::ch64::city_hash_64;
use crate::pprof_pb::google::v1::Function;
use crate::pprof_pb::google::v1::Line;
use crate::pprof_pb::google::v1::Location;
use crate::pprof_pb::google::v1::Mapping;
use crate::pprof_pb::google::v1::Sample;
use crate::pprof_pb::google::v1::ValueType;
use crate::pprof_pb::google::v1::{Label, Profile};
use bytemuck;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};


pub struct ProfileMerge {
    prof: Option<Profile>,
    tmp: Vec<u32>,

    string_table: Option<RewriteTable<String, String, String>>,
    function_table: Option<RewriteTable<FunctionKey, Function, Function>>,
    mapping_table: Option<RewriteTable<MappingKey, Mapping, Mapping>>,
    location_table: Option<RewriteTable<LocationKey, Location, Location>>,
    sample_table: Option<RewriteTable<SampleKey, Sample, Sample>>,
}

impl ProfileMerge {
    pub(crate) fn new() -> ProfileMerge {
        ProfileMerge {
            prof: Option::None,
            tmp: Vec::new(),

            string_table: Option::None,
            function_table: Option::None,
            mapping_table: Option::None,
            location_table: Option::None,
            sample_table: Option::None,
        }
    }
    pub fn merge(&mut self, p: &mut Profile) {
        if p.sample.len() == 0 || p.string_table.len() < 2 {
            return;
        }

        sanitize_profile(&mut Some(p));

        let mut initial = false;
        if self.prof.is_none() {
            self.init(p);
            initial = true;
        }

        self.tmp.resize(p.string_table.len(), 0);
        self.string_table
            .as_mut()
            .unwrap()
            .index(&mut self.tmp, &p.string_table);

        rewrite_strings(p, &mut self.tmp);
        if initial {
            rewrite_strings(self.prof.as_mut().unwrap(), &mut self.tmp)
        }

        combine_headers(self.prof.as_mut().unwrap(), p);

        self.tmp.resize(p.function.len(), 0);
        self.function_table
            .as_mut()
            .unwrap()
            .index(&mut self.tmp, &p.function);
        rewrite_functions(p, &mut self.tmp);

        self.tmp.resize(p.mapping.len(), 0);
        self.mapping_table
            .as_mut()
            .unwrap()
            .index(&mut self.tmp, &p.mapping);
        rewrite_mappings(p, &mut self.tmp);

        self.tmp.resize(p.location.len(), 0);
        self.location_table
            .as_mut()
            .unwrap()
            .index(&mut self.tmp, &p.location);
        rewrite_locations(p, &mut self.tmp);

        self.tmp.resize(p.sample.len(), 0);
        self.sample_table
            .as_mut()
            .unwrap()
            .index(&mut self.tmp, &p.sample);

        for i in 0..self.tmp.len() {
            let idx = self.tmp[i];
            let dst = &mut self.sample_table.as_mut().unwrap().s[idx as usize].value;
            let src = p.sample[i as usize].value.clone();
            for j in 0..src.len() {
                dst[j] += src[j];
            }
        }
    }

    fn init(&mut self, p: &mut Profile) {
        let factor = 2;
        self.string_table = Some(RewriteTable::new(
            factor * p.string_table.len(),
            |s| s.clone(),
            |s| s.clone(),
        ));
        self.function_table = Some(RewriteTable::new(
            factor * p.function.len(),
            FunctionKey::get,
            |s| s.clone(),
        ));
        self.mapping_table = Some(RewriteTable::new(
            factor * p.mapping.len(),
            MappingKey::get,
            |s| s.clone(),
        ));
        self.location_table = Some(RewriteTable::new(
            factor * p.location.len(),
            LocationKey::get,
            |s| s.clone(),
        ));
        self.sample_table = Some(RewriteTable::new(
            factor * p.sample.len(),
            SampleKey::get,
            |s| s.clone(),
        ));
        let mut _prof = Profile::default();
        _prof.sample_type = vec![];

        _prof.drop_frames = p.drop_frames.clone();
        _prof.keep_frames = p.keep_frames.clone();
        _prof.time_nanos = p.time_nanos.clone();
        _prof.period_type = p.period_type.clone();
        _prof.period = p.period.clone();
        _prof.default_sample_type = p.default_sample_type.clone();
        for s in 0..p.sample_type.len() {
            _prof.sample_type.push(p.sample_type[s].clone());
        }
        self.prof = Some(_prof);
    }

    pub fn profile(&mut self) -> Profile {
        if self.prof.is_none() {
            return Profile::default();
        }
        let mut p = self.prof.as_mut().unwrap().clone();
        p.sample = self.sample_table.as_mut().unwrap().values().clone();
        p.location = self.location_table.as_mut().unwrap().values().clone();
        p.function = self.function_table.as_mut().unwrap().values().clone();
        p.mapping = self.mapping_table.as_mut().unwrap().values().clone();
        p.string_table = self.string_table.as_mut().unwrap().values().clone();
        for i in 0..p.location.len() {
            p.location[i].id = i as u64 + 1;
        }
        for i in 0..p.function.len() {
            p.function[i].id = i as u64 + 1;
        }
        for i in 0..p.mapping.len() {
            p.mapping[i].id = i as u64 + 1;
        }
        return p;
    }
    
}

fn rewrite_strings(p: &mut Profile, n: &Vec<u32>) {
    for i in 0..p.sample_type.len() {
        let t = &mut p.sample_type[i];
        if t.unit != 0 {
            t.unit = n[t.unit as usize] as i64;
        }
        if t.r#type != 0 {
            t.r#type = n[t.r#type as usize] as i64;
        }
    }
    for i in 0..p.sample.len() {
        let s = &mut p.sample[i];
        for j in 0..s.label.len() {
            let l = &mut s.label[j];
            l.key = n[l.key as usize] as i64;
            l.str = n[l.str as usize] as i64;
        }
    }

    for i in 0..p.mapping.len() {
        let m = &mut p.mapping[i];
        m.filename = n[m.filename as usize] as i64;
        m.build_id = n[m.build_id as usize] as i64;
    }

    for i in 0..p.function.len() {
        let f = &mut p.function[i];
        f.name = n[f.name as usize] as i64;
        f.filename = n[f.filename as usize] as i64;
        f.system_name = n[f.system_name as usize] as i64;
    }
    p.drop_frames = n[p.drop_frames as usize] as i64;
    p.keep_frames = n[p.keep_frames as usize] as i64;
    if !p.period_type.is_none() {
        if p.period_type.as_mut().unwrap().r#type != 0 {
            p.period_type.as_mut().unwrap().r#type =
                n[p.period_type.as_mut().unwrap().r#type as usize] as i64;
        }
        if p.period_type.as_mut().unwrap().unit != 0 {
            p.period_type.as_mut().unwrap().unit =
                n[p.period_type.as_mut().unwrap().unit as usize] as i64;
        }
    }

    for i in 0..p.comment.len() {
        let x = p.comment[i];
        p.comment[i] = n[x as usize] as i64;
    }
    p.default_sample_type = n[p.default_sample_type as usize] as i64;
}

fn rewrite_functions(p: &mut Profile, n: &Vec<u32>) {
    for i in 0..p.location.len() {
        let loc = &mut p.location[i];
        for j in 0..loc.line.len() {
            let line = &mut loc.line[j];
            if line.function_id > 0 {
                line.function_id = n[line.function_id as usize - 1] as u64 + 1;
            }
        }
    }
}

fn rewrite_mappings(p: &mut Profile, n: &mut Vec<u32>) {
    for i in 0..p.location.len() {
        let loc = &mut p.location[i];
        if loc.mapping_id > 0 {
            loc.mapping_id = n[loc.mapping_id as usize - 1] as u64 + 1;
        }
    }
}

fn rewrite_locations(p: &mut Profile, n: &mut Vec<u32>) {
    for i in 0..p.sample.len() {
        let s = &mut p.sample[i];
        for j in 0..s.location_id.len() {
            if s.location_id[j] > 0 {
                s.location_id[j] = n[s.location_id[j] as usize - 1] as u64 + 1;
            }
        }
    }
}

fn sanitize_profile(_p: &mut Option<&mut Profile>) {
    if _p.is_none() {
        return;
    }
    let p = _p.as_mut().unwrap();
    let mut ms = p.string_table.len() as i64;
    let mut z: i64 = -1;
    for i in 0..p.string_table.len() {
        let s = &p.string_table[i];
        if s == "" {
            z = i as i64;
            break;
        }
    }
    if z == -1 {
        z = ms;
        p.string_table.push("".to_string());
        ms += 1;
    }
    let tmp = p.string_table[0].clone();
    p.string_table[0] = p.string_table[z as usize].clone();
    p.string_table[z as usize] = tmp;

    let str = |i: i64| -> i64 {
        if i == 0 && z > 0 {
            return z;
        }
        if i == z || i >= ms || i < 0 {
            return 0;
        }
        return i;
    };
    p.sample_type = remove_in_place(&mut p.sample_type, &mut |x, _| -> bool {
        x.r#type = str(x.r#type);
        x.unit = str(x.unit);
        false
    });

    if !p.period_type.is_none() {
        p.period_type.as_mut().unwrap().r#type = str(p.period_type.as_mut().unwrap().r#type);
        p.period_type.as_mut().unwrap().unit = str(p.period_type.as_mut().unwrap().unit);
    }

    p.default_sample_type = str(p.default_sample_type);
    p.drop_frames = str(p.drop_frames);
    p.keep_frames = str(p.keep_frames);
    for i in 0..p.comment.len() {
        p.comment[i] = str(p.comment[i]);
    }

    let mut t: HashMap<u64, u64> = HashMap::new();
    let mut j: u64 = 1;
    p.mapping = remove_in_place(&mut p.mapping, &mut |x, _| -> bool {
        x.build_id = str(x.build_id);
        x.filename = str(x.filename);
        t.insert(x.id, j);
        x.id = j;
        j += 1;
        false
    });

    let mut mapping: Option<Mapping> = Option::None;
    let p_mapping = &mut p.mapping;
    p.location = remove_in_place(&mut p.location, &mut |x, _| -> bool {
        if x.mapping_id == 0 {
            if mapping.is_none() {
                let mut _mapping = Mapping::default();
                _mapping.id = p_mapping.len() as u64 + 1;
                mapping = Some(_mapping.clone());
                p_mapping.push(_mapping);
            }
            x.mapping_id = mapping.as_ref().unwrap().id;
            return false;
        }
        x.mapping_id = t[&x.mapping_id];
        return x.mapping_id == 0;
    });

    t.clear();

    j = 1;
    p.function = remove_in_place(&mut p.function, &mut |x, _| -> bool {
        x.name = str(x.name);
        x.system_name = str(x.system_name);
        x.filename = str(x.filename);
        t.insert(x.id, j);
        x.id = j;
        j += 1;
        false
    });

    p.location = remove_in_place(&mut p.location, &mut |x, _| -> bool {
        for i in 0..x.line.len() {
            let line = &mut x.line[i];
            line.function_id = t[&line.function_id];
            if line.function_id == 0 {
                return true;
            }
        }
        return false;
    });

    t.clear();
    j = 1;
    for i in 0..p.location.len() {
        let x = &mut p.location[i];
        t.insert(x.id, j);
        x.id = j;
        j += 1;
    }

    let vs = p.sample_type.len();
    p.sample = remove_in_place(&mut p.sample, &mut |x, _| -> bool {
        if x.value.len() != vs {
            return true;
        }
        for i in 0..x.location_id.len() {
            x.location_id[i] = t[&x.location_id[i]];
            if x.location_id[i] == 0 {
                return true;
            }
        }
        for i in 0..x.label.len() {
            let l = &mut x.label[i];
            l.key = str(l.key);
            l.str = str(l.str);
            l.num_unit = str(l.num_unit);
        }
        false
    });
}

fn remove_in_place<T: Clone, F: FnMut(&mut T, i64) -> bool>(
    collection: &mut Vec<T>,
    predicate: &mut F,
) -> Vec<T> {
    let mut i: usize = 0;
    for j in 0..collection.len() {
        if !predicate(&mut collection[j], j as i64) {
            let tmp = collection[i].clone();
            collection[i] = collection[j].clone();
            collection[j] = tmp;
            i += 1;
        }
    }
    return collection[..i].to_vec();
    /*
        i := 0
    for j, x := range collection {
    if !predicate(x, j) {
    collection[j], collection[i] = collection[i], collection[j]
    i++
    }
    }
    return collection[:i]

      */
}

fn combine_headers(a: &mut Profile, b: &Profile) {
    compatible(a, b);
    if a.time_nanos == 0 || b.time_nanos < a.time_nanos {
        a.time_nanos = b.time_nanos
    }
    a.duration_nanos += b.duration_nanos;
    if a.period == 0 || a.period < b.period {
        a.period = b.period
    }
    if a.default_sample_type == 0 {
        a.default_sample_type = b.default_sample_type
    }
}
fn compatible(a: &Profile, b: &Profile) {
    if !equal_value_type(&a.period_type, &b.period_type) {
        panic!(
            "incompatible period types {:?} and {:?}",
            a.period_type, b.period_type
        );
    }
    if b.sample_type.len() != a.sample_type.len() {
        panic!(
            "incompatible sample types {:?} and {:?}",
            a.sample_type, b.sample_type
        );
    }
    for i in 0..a.sample_type.len() {
        if !equal_value_type(
            &Some(a.sample_type[i].clone()),
            &Some(b.sample_type[i].clone()),
        ) {
            panic!(
                "incompatible sample types {:?} and {:?}",
                a.sample_type, b.sample_type
            );
        }
    }
}

fn equal_value_type(st1: &Option<ValueType>, st2: &Option<ValueType>) -> bool {
    if st1.is_none() || st2.is_none() {
        return false;
    }
    return st1.as_ref().unwrap().r#type == st2.as_ref().unwrap().r#type
        && st1.as_ref().unwrap().unit == st2.as_ref().unwrap().unit;
}

struct FunctionKey {
    start_line: u32,
    name: u32,
    system_name: u32,
    file_name: u32,
}

impl FunctionKey {
    fn get(f: &Function) -> FunctionKey {
        return FunctionKey {
            start_line: f.start_line as u32,
            name: f.name as u32,
            system_name: f.system_name as u32,
            file_name: f.filename as u32,
        };
    }
}

impl PartialEq<Self> for FunctionKey {
    fn eq(&self, other: &Self) -> bool {
        return self.name == other.name
            && self.system_name == other.system_name
            && self.file_name == other.file_name
            && self.start_line == other.start_line;
    }
}

impl Eq for FunctionKey {}

impl Hash for FunctionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.name);
        state.write_u32(self.system_name);
        state.write_u32(self.file_name);
        state.write_u32(self.start_line);
    }
}

struct MappingKey {
    size: u64,
    offset: u64,
    build_id_or_file: i64,
}

impl MappingKey {
    fn get(m: &Mapping) -> MappingKey {
        let mapsize_rounding = 0x1000;
        let mut size = m.memory_limit - m.memory_start;
        size = size + mapsize_rounding - 1;
        size = size - (size % mapsize_rounding);
        let mut k = MappingKey {
            size: size,
            offset: m.file_offset,
            build_id_or_file: 0,
        };
        if m.build_id != 0 {
            k.build_id_or_file = m.build_id;
        }
        if m.filename != 0 {
            k.build_id_or_file = m.filename;
        }
        k
    }
}

impl PartialEq<Self> for MappingKey {
    fn eq(&self, other: &Self) -> bool {
        return self.build_id_or_file == other.build_id_or_file
            && self.offset == other.offset
            && self.size == other.size;
    }
}

impl Eq for MappingKey {}

impl Hash for MappingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.build_id_or_file);
        state.write_u64(self.offset);
        state.write_u64(self.size);
    }
}

struct LocationKey {
    addr: u64,
    lines: u64,
    mapping_id: u64,
}

impl LocationKey {
    fn get(l: &Location) -> LocationKey {
        return LocationKey {
            addr: l.address,
            lines: hash_lines(&l.line),
            mapping_id: l.mapping_id,
        };
    }
}

impl PartialEq<Self> for LocationKey {
    fn eq(&self, other: &Self) -> bool {
        return self.lines == other.lines
            && self.mapping_id == other.mapping_id
            && self.addr == other.addr;
    }
}

impl Eq for LocationKey {}

impl Hash for LocationKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.lines);
        state.write_u64(self.mapping_id);
        state.write_u64(self.addr);
    }
}

fn hash_lines(s: &Vec<Line>) -> u64 {
    let mut x = vec![0 as u64; s.len()];
    for i in 0..s.len() {
        x[i] = s[i].function_id | ((s[i].line as u64) << 32)
    }
    let u64_arr = x.as_slice();
    let u8_arr: &[u8] = bytemuck::cast_slice(u64_arr);
    return city_hash_64(u8_arr);
}

struct SampleKey {
    locations: u64,
    labels: u64,
}

impl SampleKey {
    fn get(s: &Sample) -> SampleKey {
        return SampleKey {
            locations: hash_locations(&s.location_id),
            labels: hash_labels(&s.label),
        };
    }
}

impl PartialEq<Self> for SampleKey {
    fn eq(&self, other: &Self) -> bool {
        return self.locations == other.locations && self.labels == other.labels;
    }
}

impl Eq for SampleKey {}

impl Hash for SampleKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.locations);
        state.write_u64(self.labels);
    }
}

fn hash_labels(labels: &Vec<Label>) -> u64 {
    if labels.len() == 0 {
        return 0;
    }
    let mut _labels = labels.clone();
    _labels.sort_by(|a: &Label, b: &Label| -> Ordering {
        if a.key < b.key || a.str < b.str {
            return Ordering::Less;
        }
        Ordering::Greater
    });
    let mut arr = vec![0 as u64; labels.len()];
    for i in 0..labels.len() {
        arr[i] = (labels[i].key | labels[i].str << 32) as u64;
    }
    city_hash_64(bytemuck::cast_slice(&arr))
}

fn hash_locations(p0: &Vec<u64>) -> u64 {
    let u8_arr: &[u8] = bytemuck::cast_slice(p0.as_slice());
    return city_hash_64(u8_arr);
}

struct RewriteTable<K, V, M> {
    k: fn(&V) -> K,
    v: fn(&V) -> M,
    t: HashMap<K, usize>,
    s: Vec<M>,
}

impl<K: std::cmp::Eq + std::hash::Hash, V, M> RewriteTable<K, V, M> {
    fn new(size: usize, k: fn(&V) -> K, v: fn(&V) -> M) -> RewriteTable<K, V, M> {
        RewriteTable {
            k,
            v,
            t: HashMap::with_capacity(size),
            s: Vec::new(),
        }
    }

    fn index(&mut self, dst: &mut Vec<u32>, values: &Vec<V>) {
        for i in 0..values.len() {
            let k = (self.k)(&values[i]);
            let mut n = self.t.get(&k);
            let _len = self.s.len().clone();
            if n.is_none() {
                n = Some(&_len);
                self.s.push((self.v)(&values[i]));
                self.t.insert(k, *n.unwrap());
            }
            dst[i] = *n.unwrap() as u32;
        }
    }

    /*fn append(&mut self, values: Vec<V>) {
        for i in 0..values.len() {
            let k = (self.k)(&values[i]);
            let n = self.s.len();
            self.s.push((self.v)(&values[i]));
            self.t.insert(k, n);
        }
    }*/
    fn values(&self) -> &Vec<M> {
        return &self.s;
    }
}

