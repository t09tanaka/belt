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
        bail!("matrix combination limit must be positive");
    }
    expand_with_limit(m, Some(max_combinations), None)
}

pub(crate) fn expand_portable(
    m: &Value,
    max_combinations: usize,
    bytes: usize,
) -> Result<Vec<Map<String, Value>>> {
    if max_combinations == 0 {
        bail!("matrix expansion data budget exhausted");
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
                bail!("matrix Cartesian product exceeds combination limit");
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
            let old_bytes = combos
                .iter()
                .try_fold(0usize, |sum, combo| -> Result<usize> {
                    sum.checked_add(crate::expr::serialized_size(combo, bytes)?)
                        .ok_or_else(|| anyhow::anyhow!("matrix byte budget exceeded"))
                })?;
            let value_bytes = values
                .iter()
                .try_fold(0usize, |sum, value| -> Result<usize> {
                    sum.checked_add(crate::expr::serialized_size(value, bytes)?)
                        .ok_or_else(|| anyhow::anyhow!("matrix byte budget exceeded"))
                })?;
            let key_bytes = crate::expr::serialized_size(k, bytes)?;
            let next_bytes = old_bytes
                .checked_mul(values.len())
                .and_then(|n| {
                    value_bytes
                        .checked_mul(combos.len())
                        .and_then(|v| n.checked_add(v))
                })
                .and_then(|n| {
                    combos
                        .len()
                        .checked_mul(values.len())
                        .and_then(|count| {
                            key_bytes
                                .checked_add(2)
                                .and_then(|overhead| count.checked_mul(overhead))
                        })
                        .and_then(|overhead| n.checked_add(overhead))
                })
                .ok_or_else(|| anyhow::anyhow!("matrix byte budget exceeded"))?;
            if next_bytes > bytes {
                bail!("matrix byte budget exceeded");
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
                    .checked_mul(base_len.max(1))
                    .and_then(|n| n.checked_add(existing));
                if additional.is_none_or(|n| n > bytes) {
                    bail!("matrix include byte budget exceeded");
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
                    bail!("matrix include exceeds combination limit");
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
    #[test]
    fn portable_byte_budget_rejects_cartesian_copies_before_construction() {
        let value =
            serde_json::json!({"blob":["x".repeat(16*1024)],"n":(0..32).collect::<Vec<_>>()});
        assert!(expand_portable(&value, 4096, 32 * 1024).is_err());
        assert_eq!(expand_bounded(&value, 4096).unwrap().len(), 32);
    }
}
