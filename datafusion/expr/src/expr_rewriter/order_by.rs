// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rewrite for order by expressions

use crate::expr::Alias;
use crate::expr_rewriter::normalize_col;
use crate::{expr::Sort, Cast, Expr, LogicalPlan, TryCast};

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{Column, Result};

/// Rewrite sort on aggregate expressions to sort on the column of aggregate output
/// For example, `max(x)` is written to `col("max(x)")`
pub fn rewrite_sort_cols_by_aggs(
    sorts: impl IntoIterator<Item = impl Into<Sort>>,
    plan: &LogicalPlan,
) -> Result<Vec<Sort>> {
    sorts
        .into_iter()
        .map(|e| {
            let sort = e.into();
            Ok(Sort::new(
                rewrite_sort_col_by_aggs(sort.expr, plan)?,
                sort.asc,
                sort.nulls_first,
            ))
        })
        .collect()
}

fn rewrite_sort_col_by_aggs(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    let plan_inputs = plan.inputs();

    // Joins, and Unions are not yet handled (should have a projection
    // on top of them)
    if plan_inputs.len() == 1 {
        let proj_exprs = plan.expressions();
        rewrite_in_terms_of_projection(expr, proj_exprs, plan_inputs[0])
    } else {
        Ok(expr)
    }
}

/// Rewrites a sort expression in terms of the output of the previous [`LogicalPlan`]
///
/// Example:
///
/// Given an input expression such as `col(a) + col(b) + col(c)`
///
/// into `col(a) + col("b + c")`
///
/// Remember that:
/// 1. given a projection with exprs: [a, b + c]
/// 2. t produces an output schema with two columns "a", "b + c"
fn rewrite_in_terms_of_projection(
    expr: Expr,
    proj_exprs: Vec<Expr>,
    input: &LogicalPlan,
) -> Result<Expr> {
    // assumption is that each item in exprs, such as "b + c" is
    // available as an output column named "b + c"
    expr.transform(|expr| {
        // search for unnormalized names first such as "c1" (such as aliases)
        if let Some(found) = proj_exprs.iter().find(|a| (**a) == expr) {
            let (qualifier, field_name) = found.qualified_name();
            let col = Expr::Column(Column::new(qualifier, field_name));
            return Ok(Transformed::yes(col));
        }

        // if that doesn't work, try to match the expression as an
        // output column -- however first it must be "normalized"
        // (e.g. "c1" --> "t.c1") because that normalization is done
        // at the input of the aggregate.

        let normalized_expr = if let Ok(e) = normalize_col(expr.clone(), input) {
            e
        } else {
            // The expr is not based on Aggregate plan output. Skip it.
            return Ok(Transformed::no(expr));
        };

        // expr is an actual expr like min(t.c2), but we are looking
        // for a column with the same "MIN(C2)", so translate there
        let name = normalized_expr.schema_name().to_string();

        let search_col = Expr::Column(Column::new_unqualified(name));

        // look for the column named the same as this expr
        let mut found = None;
        for proj_expr in &proj_exprs {
            proj_expr.apply(|e| {
                if expr_match(&search_col, e) {
                    found = Some(e.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }

        if let Some(found) = found {
            return Ok(Transformed::yes(match normalized_expr {
                Expr::Cast(Cast { expr: _, data_type }) => Expr::Cast(Cast {
                    expr: Box::new(found),
                    data_type,
                }),
                Expr::TryCast(TryCast { expr: _, data_type }) => Expr::TryCast(TryCast {
                    expr: Box::new(found),
                    data_type,
                }),
                _ => found,
            }));
        }

        Ok(Transformed::no(expr))
    })
    .data()
}

/// Does the underlying expr match e?
/// so avg(c) as average will match avgc
fn expr_match(needle: &Expr, expr: &Expr) -> bool {
    // check inside aliases
    if let Expr::Alias(Alias { expr, .. }) = &expr {
        expr.as_ref() == needle
    } else {
        expr == needle
    }
}

#[cfg(test)]
mod test {
    use std::ops::Add;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::{
        cast, col, lit, logical_plan::builder::LogicalTableSource, try_cast,
        LogicalPlanBuilder,
    };

    use super::*;
    use crate::test::function_stub::avg;
    use crate::test::function_stub::min;

    #[test]
    fn rewrite_sort_cols_by_agg() {
        //  gby c1, agg: min(c2)
        let agg = make_input()
            .aggregate(
                // gby: c1
                vec![col("c1")],
                // agg: min(c2)
                vec![min(col("c2"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> c1",
                input: sort(col("c1")),
                expected: sort(col("c1")),
            },
            TestCase {
                desc: "c1 + c2 --> c1 + c2",
                input: sort(col("c1") + col("c1")),
                expected: sort(col("c1") + col("c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "min(c2)"#,
                input: sort(min(col("c2"))),
                expected: sort(min(col("c2"))),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "c1 + min(c2)"#,
                input: sort(col("c1") + min(col("c2"))),
                expected: sort(col("c1") + min(col("c2"))),
            },
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    #[test]
    fn rewrite_sort_cols_by_agg_alias() {
        let agg = make_input()
            .aggregate(
                // gby c1
                vec![col("c1")],
                // agg: min(c2), avg(c3)
                vec![min(col("c2")), avg(col("c3"))],
            )
            .unwrap()
            //  projects out an expression "c1" that is different than the column "c1"
            .project(vec![
                // c1 + 1 as c1,
                col("c1").add(lit(1)).alias("c1"),
                // min(c2)
                min(col("c2")),
                // avg("c3") as average
                avg(col("c3")).alias("average"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "c1 --> c1  -- column *named* c1 that came out of the projection, (not t.c1)",
                input: sort(col("c1")),
                // should be "c1" not t.c1
                expected: sort(col("c1")),
            },
            TestCase {
                desc: r#"min(c2) --> "min(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(min(col("c2"))),
                expected: sort(col("min(t.c2)")),
            },
            TestCase {
                desc: r#"c1 + min(c2) --> "c1 + min(c2)" -- (column *named* "min(t.c2)"!)"#,
                input: sort(col("c1") + min(col("c2"))),
                // should be "c1" not t.c1
                expected: sort(col("c1") + col("min(t.c2)")),
            },
            TestCase {
                desc: r#"avg(c3) --> "avg(t.c3)" as average (column *named* "avg(t.c3)", aliased)"#,
                input: sort(avg(col("c3"))),
                expected: sort(col("avg(t.c3)").alias("average")),
            },
        ];

        for case in cases {
            case.run(&agg)
        }
    }

    #[test]
    fn preserve_cast() {
        let plan = make_input()
            .project(vec![col("c2").alias("c2")])
            .unwrap()
            .project(vec![col("c2").alias("c2")])
            .unwrap()
            .build()
            .unwrap();

        let cases = vec![
            TestCase {
                desc: "Cast is preserved by rewrite_sort_cols_by_aggs",
                input: sort(cast(col("c2"), DataType::Int64)),
                expected: sort(cast(col("c2").alias("c2"), DataType::Int64)),
            },
            TestCase {
                desc: "TryCast is preserved by rewrite_sort_cols_by_aggs",
                input: sort(try_cast(col("c2"), DataType::Int64)),
                expected: sort(try_cast(col("c2").alias("c2"), DataType::Int64)),
            },
        ];

        for case in cases {
            case.run(&plan)
        }
    }

    struct TestCase {
        desc: &'static str,
        input: Sort,
        expected: Sort,
    }

    impl TestCase {
        /// calls rewrite_sort_cols_by_aggs for expr and compares it to expected_expr
        fn run(self, input_plan: &LogicalPlan) {
            let Self {
                desc,
                input,
                expected,
            } = self;

            println!("running: '{desc}'");
            let mut exprs =
                rewrite_sort_cols_by_aggs(vec![input.clone()], input_plan).unwrap();

            assert_eq!(exprs.len(), 1);
            let rewritten = exprs.pop().unwrap();

            assert_eq!(
                rewritten, expected,
                "\n\ninput:{input:?}\nrewritten:{rewritten:?}\nexpected:{expected:?}\n"
            );
        }
    }

    /// Scan of a table: t(c1 int, c2 varchar, c3 float)
    fn make_input() -> LogicalPlanBuilder {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Float64, true),
        ]));
        let projection = None;
        LogicalPlanBuilder::scan(
            "t",
            Arc::new(LogicalTableSource::new(schema)),
            projection,
        )
        .unwrap()
    }

    fn sort(expr: Expr) -> Sort {
        let asc = true;
        let nulls_first = true;
        expr.sort(asc, nulls_first)
    }
}
