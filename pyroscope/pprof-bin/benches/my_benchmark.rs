use pprof_bin::merge_prof;
use pprof_bin::utest::get_test_pprof_data;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn merge_bench(pprofs: &Vec<Vec<u8>>) {

    for pprof in pprofs {
        merge_prof(0, pprof.as_slice(), "process_cpu:samples:count:cpu:nanoseconds".to_string());
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let pprofs = get_test_pprof_data();
    c.bench_function("merge", |b| b.iter(|| merge_bench(&pprofs)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);