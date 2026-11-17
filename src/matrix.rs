//! `strategy.matrix` expansion, following GitHub's rules for `include` / `exclude`.

use anyhow::{bail, Result};
use serde_json::{Map, Value};

pub fn yaml_to_json(v: &serde_yaml::Value) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

/// Expand a matrix (already a JSON object) into its combinations.
/// An absent matrix is one empty combination.
pub fn expand(m: &Value) -> Result<Vec<Map<String, Value>>> {
    expand_with_limit(m, None, None)
}

/// Bound intermediate Cartesian allocations as well as final include growth.
/// Excludes do not permit an oversized intermediate product.
pub fn expand_bounded(m: &Value, max_combinations: usize) -> Result<Vec<Map<String, Value>>> {
    if max_combinations == 0 {
        return Err(crate::expr::EvaluationBudgetExceeded(
            "matrix combination limit must be positive",
        )
        .into());
    }
    expand_with_limit(m, Some(max_combinations), None)
}

pub(crate) fn expand_portable(
    m: &Value,
    max_combinations: usize,
    bytes: usize,
) -> Result<Vec<Map<String, Value>>> {
    if max_combinations == 0 {
        return Err(crate::expr::EvaluationBudgetExceeded(
            "matrix expansion data budget exhausted",
        )
        .into());
    }
    expand_with_limit(m, Some(max_combinations), Some(bytes))
}

fn expand_with_limit(
    m: &Value,
    limit: Option<usize>,
    byte_limit: Option<usize>,
) -> Result<Vec<Map<String, Value>>> {
    let obj = match m {
        Value::Null => return Ok(vec![Map::new()]),
        Value::Object(o) => o,
        other => bail!("matrix is not a mapping: {other}"),
    };
    let mut combos: Vec<Map<String, Value>> = vec![Map::new()];
    let mut original_keys = Vec::new();
    for (k, v) in obj {
        if k == "include" || k == "exclude" {
            continue;
        }
        let cardinality = v.as_array().map_or(1, Vec::len);
        if let Some(limit) = limit {
            if combos
                .len()
                .checked_mul(cardinality)
                .is_none_or(|n| n > limit)
            {
                return Err(crate::expr::EvaluationBudgetExceeded(
                    "matrix Cartesian product exceeds combination limit",
                )
                .into());
            }
        }
        let scalar;
        let values = match v {
            Value::Array(a) => a,
            Value::String(s) if s.contains("${{") => {
                bail!("matrix key `{k}` is an unevaluated expression")
            }
            other => {
                scalar = vec![other.clone()];
                &scalar
            }
        };
        if let Some(bytes) = byte_limit {
            // What the product with this key holds: each combination once per
            // value, each value once per combination, the key in all of them.
            // A size above the budget is an error of its own; the sums saturate.
            let mut old_bytes = 0usize;
            for combo in &combos {
                old_bytes = old_bytes.saturating_add(crate::expr::serialized_size(combo, bytes)?);
            }
            let mut value_bytes = 0usize;
            for value in values {
                value_bytes =
                    value_bytes.saturating_add(crate::expr::serialized_size(value, bytes)?);
            }
            let key_bytes = crate::expr::serialized_size(k, bytes)?;
            let next_bytes = old_bytes
                .saturating_mul(values.len())
                .saturating_add(value_bytes.saturating_mul(combos.len()))
                .saturating_add(
                    combos
                        .len()
                        .saturating_mul(values.len())
                        .saturating_mul(key_bytes.saturating_add(2)),
                );
            if next_bytes > bytes {
                return Err(
                    crate::expr::EvaluationBudgetExceeded("matrix byte budget exceeded").into(),
                );
            }
        }
        original_keys.push(k.clone());
        let mut next = Vec::new();
        for c in &combos {
            for val in values {
                let mut c2 = c.clone();
                c2.insert(k.clone(), val.clone());
                next.push(c2);
            }
        }
        combos = next;
    }
    let had_base = !original_keys.is_empty();
    if !had_base {
        combos.clear();
    }
    if let Some(Value::Array(ex)) = obj.get("exclude") {
        combos.retain(|c| {
            !ex.iter().any(|e| {
                e.as_object()
                    .is_some_and(|e| e.iter().all(|(k, v)| c.get(k) == Some(v)))
            })
        });
    }
    if let Some(Value::Array(inc)) = obj.get("include") {
        let base_len = combos.len();
        for item in inc {
            let Some(item) = item.as_object() else {
                continue;
            };
            if let Some(bytes) = byte_limit {
                let existing = crate::expr::serialized_size(&combos, bytes)?;
                let item_bytes = crate::expr::serialized_size(item, bytes)?;
                let additional = item_bytes
                    .saturating_mul(base_len.max(1))
                    .saturating_add(existing);
                if additional > bytes {
                    return Err(crate::expr::EvaluationBudgetExceeded(
                        "matrix include byte budget exceeded",
                    )
                    .into());
                }
            }
            let mut matched = false;
            for c in combos.iter_mut().take(base_len) {
                // an include entry extends every combination whose original values it does not change
                let compatible = item
                    .iter()
                    .all(|(k, v)| !original_keys.contains(k) || c.get(k) == Some(v));
                if compatible && had_base {
                    for (k, v) in item {
                        c.insert(k.clone(), v.clone());
                    }
                    matched = true;
                }
            }
            if !matched {
                if limit.is_some_and(|n| combos.len() >= n) {
                    return Err(crate::expr::EvaluationBudgetExceeded(
                        "matrix include exceeds combination limit",
                    )
                    .into());
                }
                combos.push(item.clone());
            }
        }
    }
    if combos.is_empty() {
        bail!("matrix has no combinations");
    }
    Ok(combos)
}

/// "integration, 1/2" for display.
pub fn describe(c: &Map<String, Value>) -> String {
    c.values()
        .map(crate::expr::to_string)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cartesian_exclude_include() {
        let m = json!({"os": ["a", "b"], "node": [20, 22], "exclude": [{"os": "b", "node": 20}], "include": [{"os": "a", "extra": true}, {"os": "c", "node": 18}]});
        let c = expand(&m).unwrap();
        assert_eq!(c.len(), 4);
        assert_eq!(
            c[0],
            json!({"os": "a", "node": 20, "extra": true})
                .as_object()
                .unwrap()
                .clone()
        );
        assert_eq!(c[1].get("extra"), Some(&json!(true)));
        assert_eq!(
            c[2],
            json!({"os": "b", "node": 22}).as_object().unwrap().clone()
        );
        assert_eq!(
            c[3],
            json!({"os": "c", "node": 18}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn include_only() {
        let m = json!({"include": [{"name": "int 1/2", "shard": "1/2"}, {"name": "e2e", "shard": "all"}]});
        let c = expand(&m).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(describe(&c[1]), "e2e, all");
    }

    #[test]
    fn empty_and_errors() {
        assert_eq!(expand(&Value::Null).unwrap(), vec![Map::new()]);
        assert!(expand(&json!({"project": []})).is_err());
        assert!(expand(&json!("x")).is_err());
    }

    #[test]
    fn scalar_values_are_single_variants_and_expressions_are_refused() {
        let c = expand(&json!({"os": "linux", "node": 20})).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].get("os"), Some(&json!("linux")));
        assert_eq!(c[0].get("node"), Some(&json!(20)));
        let err = expand(&json!({"os": "${{ fromJSON(x) }}"})).unwrap_err();
        assert!(err
            .to_string()
            .contains("`os` is an unevaluated expression"));
        // a plain string is a value, not an expression
        assert_eq!(expand(&json!({"os": "a$b"})).unwrap().len(), 1);
    }

    #[test]
    fn include_entries_that_are_not_mappings_are_skipped() {
        let c = expand(&json!({"os": ["a"], "include": ["x", 3, {"os": "b"}]})).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[1].get("os"), Some(&json!("b")));
    }
}

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn bounds_intermediate_products_before_exclusion_and_include_growth() {
        let axis = json!((0..1000).collect::<Vec<_>>());
        assert!(expand_bounded(&json!({"a": axis, "b": axis, "c": axis}), 4096).is_err());
        assert!(
            expand_bounded(&json!({"a": [1,2], "b": [1,2], "exclude": [{"a": 1}]}), 3).is_err()
        );
        assert!(expand_bounded(&json!({"include": [{"a":1}, {"a":2}]}), 1).is_err());
        assert!(expand_bounded(&json!({"a": [1], "include": [{"a":2}]}), 1).is_err());
        assert!(expand_bounded(&Value::Null, 0).is_err());
    }
    #[test]
    fn bounded_and_legacy_share_ordered_include_exclude_semantics() {
        for matrix in [
            Value::Null,
            json!({"a":[1,2],"b":[3,4],"exclude":[{"a":1,"b":3}],"include":[{"a":2,"extra":true},{"a":3}]}),
            json!({"include":[{"a":1},{"a":2}]}),
            json!({"a":"scalar"}),
        ] {
            let ordinary = expand(&matrix).unwrap();
            assert_eq!(
                expand_bounded(&matrix, ordinary.len().max(4)).unwrap(),
                ordinary
            );
        }
        assert!(expand_bounded(&json!({"a":[]}), 1).is_err());
        assert!(expand_bounded(&json!({"a":"${{ x }}"}), 1).is_err());
    }
}

#[cfg(test)]
mod output_budget_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn portable_byte_budget_rejects_cartesian_copies_before_construction() {
        let value =
            serde_json::json!({"blob":["x".repeat(16*1024)],"n":(0..32).collect::<Vec<_>>()});
        assert!(expand_portable(&value, 4096, 32 * 1024).is_err());
        assert_eq!(expand_bounded(&value, 4096).unwrap().len(), 32);
    }

    fn budget(result: Result<Vec<Map<String, Value>>>) -> bool {
        result
            .unwrap_err()
            .downcast_ref::<crate::expr::EvaluationBudgetExceeded>()
            .is_some()
    }

    #[test]
    fn a_portable_expansion_needs_a_positive_count_and_fits_its_bytes_exactly() {
        let m = json!({"a": ["x", "y"], "b": [1, 2, 3]});
        assert!(budget(expand_portable(&m, 0, 1 << 20)));
        assert!(budget(expand_portable(&m, 5, 1 << 20)));
        assert_eq!(expand_portable(&m, 6, 1 << 20).unwrap().len(), 6);
        // `a`: one empty combination `{}` (2 bytes) for each of two values,
        // the values `"x"` and `"y"` (3 bytes each), and `"a"` + 2 twice
        let a = 2 * 2 + (3 + 3) + 2 * (3 + 2);
        assert_eq!(a, 20);
        let one = json!({"a": ["x", "y"]});
        assert_eq!(expand_portable(&one, 2, a).unwrap().len(), 2);
        assert!(budget(expand_portable(&one, 2, a - 1)));
        // `b`: the two combinations `{"a":"x"}` (9 bytes each) for each of
        // three values, three one-byte values for each of the two, and
        // `"b"` + 2 six times
        let b = (9 + 9) * 3 + (1 + 1 + 1) * 2 + 6 * (3 + 2);
        assert_eq!(b, 90);
        assert_eq!(expand_portable(&m, 6, b).unwrap().len(), 6);
        assert!(budget(expand_portable(&m, 6, b - 1)));
        // a scalar is one value; a value larger than the budget is refused as such
        assert_eq!(expand_portable(&json!({"a": "x"}), 1, 10).unwrap().len(), 1);
        assert!(budget(expand_portable(&json!({"a": "x"}), 1, 9)));
        assert!(budget(expand_portable(&json!({"a": ["0123456789"]}), 1, 8)));
    }

    #[test]
    fn a_portable_include_is_counted_once_for_each_combination_it_may_extend() {
        // the base `[{"a":"x"},{"a":"y"}]` is 21 bytes, the item `{"c":"z"}` 9
        let m = json!({"a": ["x", "y"], "include": [{"c": "z"}]});
        let need = 21 + 9 * 2;
        let all = expand_portable(&m, 2, need).unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.iter().all(|c| c["c"] == "z"));
        assert!(budget(expand_portable(&m, 2, need - 1)));
        // with no base the item is one combination of its own: `[]` and itself
        let only = json!({"include": [{"c": "z"}]});
        assert_eq!(expand_portable(&only, 1, 2 + 9).unwrap().len(), 1);
        assert!(budget(expand_portable(&only, 1, 2 + 9 - 1)));
        // an item that is not a mapping is passed over, as without a budget
        let odd = json!({"a": ["x"], "include": ["text"]});
        assert_eq!(expand_portable(&odd, 1, 64).unwrap().len(), 1);
    }
}
