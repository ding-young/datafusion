use arrow::{
    array::{Float32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use futures::executor::block_on;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn query(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    // execute the query
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(array_len: usize, batch_size: usize) -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32", DataType::Float32, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("s_long", DataType::Utf8, false),
    ]));
    
    // define data.
    let batches = (0..array_len / batch_size)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                    Arc::new(StringArray::from(vec![format!("value_{}", i); batch_size])),
                    Arc::new(StringArray::from(vec![format!("value_{}{}", i, "x".repeat(1000)); batch_size])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 524_288; // 2^19
    let batch_size = 4096; // 2^12
    let rt = Runtime::new().unwrap();

    c.bench_function("select_f32_eq_f32", |b| {
        let ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| {
            block_on(query(&ctx, &rt,"SELECT count(*) FROM t WHERE f32 = f32"))
        })
    });

    c.bench_function("select_str_eq_str", |b| {
        let ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| {
            block_on(query(&ctx, &rt,"SELECT count(*) FROM t WHERE s = s"))
        })
    });

    c.bench_function("select_str_eq_str_long", |b| {
        let ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| {
            block_on(query(&ctx, &rt,"SELECT count(*) FROM t WHERE s_long = s_long"))
        })
    });

    c.bench_function("select_CASE_str_eq_str_long", |b| {
        let ctx = create_context(array_len, batch_size).unwrap();
        b.iter(|| {
            block_on(query(&ctx, &rt,"SELECT count(*) FROM t WHERE 
            CASE WHEN s_long IS NOT NULL THEN true END"))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
