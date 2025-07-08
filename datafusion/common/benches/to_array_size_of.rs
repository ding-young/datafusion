use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Int32Type};
use arrow::array::{Array, ArrayRef, BooleanArray, FixedSizeListBuilder, GenericListArray, Int16Array, Int32Array, Int32Builder, Int64Array, LargeListBuilder, ListArray, ListBuilder, MapArray, MapBuilder, StringArray, StringBuilder, StructArray};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::ScalarValue;

pub fn scalar_value_to_size_one_array(c: &mut Criterion) {

let cases: Vec<(&str, ArrayRef)> = vec![    
    {
        // struct array
        let fields = vec![Field::new("a", DataType::Int32, false), Field::new("b", DataType::Utf8, false)];
        let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec!["foo"])),
        ];
        let nulls = None;
        ("struct", Arc::new(StructArray::new(fields.into(), arrays, nulls)))
    },
    // list array
    {
        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);
        // [A, B]
        builder.values().append_value("A");
        builder.values().append_value("B");
        builder.append(true);
        // [ ] (empty list)
        builder.append(true);
        // Null
        builder.values().append_value("?"); // irrelevant
        builder.append(false);
        ("list", Arc::new(builder.finish()))
    },
    {
    // large list array
        let values_builder = StringBuilder::new();
        let mut builder = LargeListBuilder::new(values_builder);
        // [A, B]
        builder.values().append_value("A");
        builder.values().append_value("B");
        builder.append(true);
        ("large_list", Arc::new(builder.finish()))
    },
    {
    // fixed size list array
        let values_builder = Int32Builder::new();
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);

        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        ("fixed_size_list", Arc::new(builder.finish()))
    },
    {
    // map
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);

        let mut builder = MapBuilder::new(None, string_builder, int_builder);
        // {"joe": 1}
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        ("map", Arc::new(builder.finish()))
    }];

    let mut group = c.benchmark_group("scalar_value_to_array_size_1");

    for (name, arr) in cases {
        let scalar = ScalarValue::try_from_array(&arr, 0).unwrap();
        group.bench_function(BenchmarkId::new(name, "arc_clone"), |b| {
            b.iter(|| {
                let result = scalar
                .to_array_of_size(1)
                .expect("Failed to convert to array of size");
                assert_eq!(result.len(), 1);
                assert_eq!(result.data_type(), arr.data_type());
            });
        });
    }


    group.finish();
}

criterion_group!(benches, scalar_value_to_size_one_array);
criterion_main!(benches);
