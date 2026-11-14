//! A small evaluator for GitHub Actions expressions (`${{ ... }}` and `if:`).
//!
//! Values are JSON values. Anything the evaluator does not know (an unknown
//! context, an unknown function) is an error, never a silent `false`, so the
//! caller can report the job as unsupported instead of quietly skipping it.

use anyhow::{bail, Result};
use serde_json::{Map, Value};
use std::cell::Cell;

/// Job status the status functions (`success()`, `failure()`, ...) answer from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Success,
    Failure,
    Cancelled,
}

/// What `success()`, `failure()` and `cancelled()` answer. A step is in one of
/// the three ([`Status`]); a job may be in none (a job it needs was skipped:
/// neither `success()` nor `failure()`), and `failure()` there also covers the
/// failure of any ancestor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusFns {
    pub success: bool,
    pub failure: bool,
    pub cancelled: bool,
}

impl From<Status> for StatusFns {
    fn from(s: Status) -> Self {
        StatusFns {
            success: s == Status::Success,
            failure: s == Status::Failure,
            cancelled: s == Status::Cancelled,
        }
    }
}

pub struct Ctx<'a> {
    /// Top-level contexts: github, env, matrix, steps, needs, job, runner, inputs, vars, strategy.
    pub contexts: &'a Map<String, Value>,
    pub status: StatusFns,
    /// Set when the expression touched `secrets.*` (or `github.token`) and no
    /// value could be supplied.
    pub used_secrets: Cell<bool>,
    /// Where the value of `github.token` / `secrets.GITHUB_TOKEN` comes from,
    /// when this host can supply it.
    token_from: Option<&'a crate::context::ExecContext>,
    output_limit: Option<usize>,
}

impl<'a> Ctx<'a> {
    pub fn new(contexts: &'a Map<String, Value>, status: Status) -> Self {
        Self::with_fns(contexts, status.into())
    }

    pub fn with_fns(contexts: &'a Map<String, Value>, status: StatusFns) -> Self {
        Ctx {
            contexts,
            status,
            used_secrets: Cell::new(false),
            token_from: None,
            output_limit: None,
        }
    }

    /// Supply the value of `github.token` and `secrets.GITHUB_TOKEN` from
    /// `ctx` (this machine's `gh auth token`) instead of flagging them as
    /// secrets, as GitHub Actions does: the expression's result holds the
    /// value itself. A caller must not write such a result to a file; a script
    /// that holds it is handed to the shell over a pipe (see `exec`).
    /// Other secrets stay flagged.
    pub fn with_token(mut self, ctx: &'a crate::context::ExecContext) -> Self {
        self.token_from = Some(ctx);
        self
    }

    /// Opt in to bounded intermediate/output values; ordinary evaluation is unlimited.
    pub fn with_output_limit(mut self, bytes: usize) -> Self {
        self.output_limit = Some(bytes);
        self
    }
    pub(crate) fn check_value<T: serde::Serialize>(&self, value: &T) -> Result<()> {
        if let Some(limit) = self.output_limit {
            serialized_size(value, limit)?;
        }
        Ok(())
    }
    fn check_len(&self, bytes: usize) -> Result<()> {
        if self.output_limit.is_some_and(|limit| bytes > limit) {
            bail!("expression output byte budget exceeded");
        }
        Ok(())
    }
    fn token(&self) -> Option<String> {
        self.token_from?.github_token().map(str::to_string)
    }
}

/// Count serialized bytes without allocating a second serialized value.
pub(crate) fn serialized_size<T: serde::Serialize>(value: &T, limit: usize) -> Result<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("serialized byte budget exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| anyhow::anyhow!("serialized byte budget exceeded"))?;
    Ok(counter.bytes)
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Str(String),
    Ident(String),
    Op(&'static str),
}

fn lex(src: &str) -> Result<Vec<Tok>> {
    let c: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        if ch == '\'' {
            let mut s = String::new();
            i += 1;
            loop {
                if i >= c.len() {
                    bail!("unterminated string in `{src}`");
                }
                if c[i] == '\'' {
                    if i + 1 < c.len() && c[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(c[i]);
                i += 1;
            }
            out.push(Tok::Str(s));
            continue;
        }
        if ch.is_ascii_digit()
            || (ch == '-'
                && i + 1 < c.len()
                && c[i + 1].is_ascii_digit()
                && prev_allows_number(&out))
        {
            let start = i;
            i += 1;
            while i < c.len() && (c[i].is_ascii_alphanumeric() || c[i] == '.') {
                i += 1;
            }
            let text: String = c[start..i].iter().collect();
            let n = if let Some(hex) = text.strip_prefix("0x") {
                i64::from_str_radix(hex, 16).map(|v| v as f64).ok()
            } else {
                text.parse::<f64>().ok()
            };
            match n {
                Some(n) => out.push(Tok::Num(n)),
                None => bail!("bad number `{text}` in `{src}`"),
            }
            continue;
        }
        if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_' || c[i] == '-') {
                i += 1;
            }
            out.push(Tok::Ident(c[start..i].iter().collect()));
            continue;
        }
        let two: String = c[i..(i + 2).min(c.len())].iter().collect();
        let op2 = ["==", "!=", "<=", ">=", "&&", "||"]
            .into_iter()
            .find(|o| *o == two);
        if let Some(o) = op2 {
            out.push(Tok::Op(o));
            i += 2;
            continue;
        }
        let op1 = ["!", "<", ">", "(", ")", "[", "]", ".", ",", "*"]
            .into_iter()
            .find(|o| o.starts_with(ch));
        match op1 {
            Some(o) => {
                out.push(Tok::Op(o));
                i += 1;
            }
            None => bail!("unexpected `{ch}` in `{src}`"),
        }
    }
    Ok(out)
}

fn prev_allows_number(out: &[Tok]) -> bool {
    !matches!(
        out.last(),
        Some(Tok::Num(_))
            | Some(Tok::Str(_))
            | Some(Tok::Ident(_))
            | Some(Tok::Op(")"))
            | Some(Tok::Op("]"))
    )
}

#[derive(Debug, Clone)]
enum Ast {
    Lit(Value),
    Ctx(String),
    Prop(Box<Ast>, Box<Ast>),
    Star(Box<Ast>),
    Call(String, Vec<Ast>),
    Not(Box<Ast>),
    Bin(&'static str, Box<Ast>, Box<Ast>),
}

struct Parser<'s> {
    toks: Vec<Tok>,
    pos: usize,
    src: &'s str,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn eat_op(&mut self, op: &str) -> bool {
        if let Some(Tok::Op(o)) = self.peek() {
            if *o == op {
                self.pos += 1;
                return true;
            }
        }
        false
    }
    fn expect_op(&mut self, op: &str) -> Result<()> {
        if !self.eat_op(op) {
            bail!("expected `{op}` in `{}`", self.src);
        }
        Ok(())
    }
    fn binary(&mut self, level: usize) -> Result<Ast> {
        const LEVELS: [&[&str]; 4] = [&["||"], &["&&"], &["==", "!="], &["<", "<=", ">", ">="]];
        if level == LEVELS.len() {
            return self.unary();
        }
        let mut lhs = self.binary(level + 1)?;
        loop {
            let op = match self.peek() {
                Some(Tok::Op(o)) if LEVELS[level].contains(o) => *o,
                _ => break,
            };
            self.pos += 1;
            let rhs = self.binary(level + 1)?;
            lhs = Ast::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }
    fn unary(&mut self) -> Result<Ast> {
        if self.eat_op("!") {
            return Ok(Ast::Not(Box::new(self.unary()?)));
        }
        self.postfix()
    }
    fn postfix(&mut self) -> Result<Ast> {
        let mut e = self.primary()?;
        loop {
            if self.eat_op(".") {
                match self.toks.get(self.pos).cloned() {
                    Some(Tok::Ident(name)) => {
                        self.pos += 1;
                        e = Ast::Prop(Box::new(e), Box::new(Ast::Lit(Value::String(name))));
                    }
                    Some(Tok::Op("*")) => {
                        self.pos += 1;
                        e = Ast::Star(Box::new(e));
                    }
                    _ => bail!("expected a property name after `.` in `{}`", self.src),
                }
            } else if self.eat_op("[") {
                if self.eat_op("*") {
                    self.expect_op("]")?;
                    e = Ast::Star(Box::new(e));
                } else {
                    let idx = self.binary(0)?;
                    self.expect_op("]")?;
                    e = Ast::Prop(Box::new(e), Box::new(idx));
                }
            } else {
                break;
            }
        }
        Ok(e)
    }
    fn primary(&mut self) -> Result<Ast> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        match t {
            Some(Tok::Num(n)) => Ok(Ast::Lit(num(n))),
            Some(Tok::Str(s)) => Ok(Ast::Lit(Value::String(s))),
            Some(Tok::Ident(id)) => match id.as_str() {
                "true" => Ok(Ast::Lit(Value::Bool(true))),
                "false" => Ok(Ast::Lit(Value::Bool(false))),
                "null" => Ok(Ast::Lit(Value::Null)),
                _ => {
                    if self.eat_op("(") {
                        let mut args = Vec::new();
                        if !self.eat_op(")") {
                            loop {
                                args.push(self.binary(0)?);
                                if self.eat_op(")") {
                                    break;
                                }
                                self.expect_op(",")?;
                            }
                        }
                        Ok(Ast::Call(id.to_ascii_lowercase(), args))
                    } else {
                        Ok(Ast::Ctx(id))
                    }
                }
            },
            Some(Tok::Op("(")) => {
                let e = self.binary(0)?;
                self.expect_op(")")?;
                Ok(e)
            }
            _ => bail!("unexpected end or token in `{}`", self.src),
        }
    }
}

fn parse(src: &str) -> Result<Ast> {
    let mut p = Parser {
        toks: lex(src)?,
        pos: 0,
        src,
    };
    let e = p.binary(0)?;
    if p.pos != p.toks.len() {
        bail!("trailing tokens in `{src}`");
    }
    Ok(e)
}

fn num(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9e15 {
        Value::from(n as i64)
    } else {
        serde_json::Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn to_num(v: &Value) -> f64 {
    match v {
        Value::Null => 0.0,
        Value::Bool(b) => *b as i32 as f64,
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::String(s) => {
            let t = s.trim();
            if t.is_empty() {
                0.0
            } else if let Some(h) = t.strip_prefix("0x") {
                i64::from_str_radix(h, 16)
                    .map(|v| v as f64)
                    .unwrap_or(f64::NAN)
            } else {
                t.parse().unwrap_or(f64::NAN)
            }
        }
        _ => f64::NAN,
    }
}

/// GitHub's loose equality: same type compares directly (strings ignore case),
/// different primitive types compare as numbers, objects/arrays by identity
/// (here: never equal unless both are the same JSON).
fn loose_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(x), Value::String(y)) => x.eq_ignore_ascii_case(y),
        (Value::Array(_), _)
        | (Value::Object(_), _)
        | (_, Value::Array(_))
        | (_, Value::Object(_)) => a == b,
        _ if std::mem::discriminant(a) == std::mem::discriminant(b) => a == b,
        _ => to_num(a) == to_num(b),
    }
}

fn cmp(op: &str, a: &Value, b: &Value) -> bool {
    if let (Value::String(x), Value::String(y)) = (a, b) {
        let (x, y) = (x.to_ascii_lowercase(), y.to_ascii_lowercase());
        return match op {
            "<" => x < y,
            "<=" => x <= y,
            ">" => x > y,
            _ => x >= y,
        };
    }
    let (x, y) = (to_num(a), to_num(b));
    match op {
        "<" => x < y,
        "<=" => x <= y,
        ">" => x > y,
        _ => x >= y,
    }
}

pub fn to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => serde_json::to_string_pretty(other).unwrap_or_default(),
    }
}

fn get_prop(base: &Value, key: &Value) -> Value {
    match base {
        Value::Object(m) => {
            let k = to_string(key);
            m.iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&k))
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null)
        }
        Value::Array(a) => {
            if let Value::Number(_) = key {
                let i = to_num(key);
                if i >= 0.0 {
                    return a.get(i as usize).cloned().unwrap_or(Value::Null);
                }
                return Value::Null;
            }
            // property of a filtered array (`foo.*.bar`) maps over the elements
            Value::Array(
                a.iter()
                    .map(|e| get_prop(e, key))
                    .filter(|v| !v.is_null())
                    .collect(),
            )
        }
        _ => Value::Null,
    }
}

fn eval(ast: &Ast, ctx: &Ctx) -> Result<Value> {
    let result = match ast {
        Ast::Lit(v) => {
            ctx.check_value(v)?;
            v.clone()
        }
        Ast::Ctx(name) => {
            let lower = name.to_ascii_lowercase();
            if lower == "secrets" {
                ctx.used_secrets.set(true);
                return Ok(Value::Object(Map::new()));
            }
            match ctx.contexts.get(&lower) {
                Some(v) => {
                    ctx.check_value(v)?;
                    v.clone()
                }
                None => bail!("unsupported context `{name}`"),
            }
        }
        Ast::Prop(b, k) => {
            let key = eval(k, ctx)?;
            let name = to_string(&key);
            // `secrets.GITHUB_TOKEN` and `github.token` are the same credential
            let secrets = matches!(b.as_ref(), Ast::Ctx(n) if n.eq_ignore_ascii_case("secrets"));
            let github = matches!(b.as_ref(), Ast::Ctx(n) if n.eq_ignore_ascii_case("github"));
            if secrets && name.eq_ignore_ascii_case("GITHUB_TOKEN") {
                if let Some(t) = ctx.token() {
                    return Ok(Value::String(t));
                }
            }
            let base = eval(b, ctx)?;
            if github && name.eq_ignore_ascii_case("token") {
                match ctx.token() {
                    Some(t) => return Ok(Value::String(t)),
                    None => ctx.used_secrets.set(true),
                }
            }
            get_prop(&base, &key)
        }
        Ast::Star(b) => match eval(b, ctx)? {
            Value::Object(m) => Value::Array(m.values().cloned().collect()),
            Value::Array(a) => Value::Array(a),
            _ => Value::Array(vec![]),
        },
        Ast::Not(e) => Value::Bool(!truthy(&eval(e, ctx)?)),
        Ast::Bin(op, l, r) => {
            let lv = eval(l, ctx)?;
            match *op {
                "&&" => {
                    if !truthy(&lv) {
                        lv
                    } else {
                        eval(r, ctx)?
                    }
                }
                "||" => {
                    if truthy(&lv) {
                        lv
                    } else {
                        eval(r, ctx)?
                    }
                }
                "==" => Value::Bool(loose_eq(&lv, &eval(r, ctx)?)),
                "!=" => Value::Bool(!loose_eq(&lv, &eval(r, ctx)?)),
                o => Value::Bool(cmp(o, &lv, &eval(r, ctx)?)),
            }
        }
        Ast::Call(name, args) => call(name, args, ctx)?,
    };
    ctx.check_value(&result)?;
    Ok(result)
}

fn call(name: &str, args: &[Ast], ctx: &Ctx) -> Result<Value> {
    let mut a = Vec::new();
    let mut argument_bytes = 0usize;
    for argument in args {
        let value = eval(argument, ctx)?;
        if let Some(limit) = ctx.output_limit {
            argument_bytes = argument_bytes
                .checked_add(serialized_size(&value, limit)?)
                .ok_or_else(|| anyhow::anyhow!("expression argument byte budget exceeded"))?;
            ctx.check_len(argument_bytes)?;
        }
        a.push(value);
    }
    let s = |i: usize| a.get(i).map(to_string).unwrap_or_default();
    Ok(match name {
        "success" => Value::Bool(ctx.status.success),
        "failure" => Value::Bool(ctx.status.failure),
        "cancelled" => Value::Bool(ctx.status.cancelled),
        "always" => Value::Bool(true),
        // Cache keys only; every cache step is skipped locally.
        "hashfiles" => Value::String(String::new()),
        "contains" => match a.first() {
            Some(Value::Array(items)) => Value::Bool(
                items
                    .iter()
                    .any(|i| loose_eq(i, a.get(1).unwrap_or(&Value::Null))),
            ),
            _ => Value::Bool(s(0).to_lowercase().contains(&s(1).to_lowercase())),
        },
        "startswith" => Value::Bool(s(0).to_lowercase().starts_with(&s(1).to_lowercase())),
        "endswith" => Value::Bool(s(0).to_lowercase().ends_with(&s(1).to_lowercase())),
        "format" => {
            let mut out = s(0);
            for i in 1..a.len() {
                let needle = format!("{{{}}}", i - 1);
                let replacement = s(i);
                if ctx.output_limit.is_some() {
                    let count = out.matches(&needle).count();
                    let bytes = count
                        .checked_mul(needle.len())
                        .and_then(|removed| out.len().checked_sub(removed))
                        .and_then(|base| {
                            count
                                .checked_mul(replacement.len())
                                .and_then(|added| base.checked_add(added))
                        })
                        .ok_or_else(|| anyhow::anyhow!("expression output byte budget exceeded"))?;
                    ctx.check_len(bytes)?;
                }
                out = out.replace(&needle, &replacement);
            }
            Value::String(out.replace("{{", "{").replace("}}", "}"))
        }
        "join" => {
            let sep = if a.len() > 1 { s(1) } else { ",".into() };
            match a.first() {
                Some(Value::Array(items)) => {
                    let parts: Vec<_> = items.iter().map(to_string).collect();
                    if ctx.output_limit.is_some() {
                        let bytes = parts
                            .iter()
                            .try_fold(0usize, |n, part| n.checked_add(part.len()))
                            .and_then(|n| {
                                sep.len()
                                    .checked_mul(parts.len().saturating_sub(1))
                                    .and_then(|separators| n.checked_add(separators))
                            })
                            .ok_or_else(|| {
                                anyhow::anyhow!("expression output byte budget exceeded")
                            })?;
                        ctx.check_len(bytes)?;
                    }
                    Value::String(parts.join(&sep))
                }
                _ => Value::String(s(0)),
            }
        }
        "tojson" => {
            let value = a.first().unwrap_or(&Value::Null);
            if let Some(limit) = ctx.output_limit {
                struct Limited {
                    output: Vec<u8>,
                    limit: usize,
                }
                impl std::io::Write for Limited {
                    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                        if self
                            .output
                            .len()
                            .checked_add(bytes.len())
                            .is_none_or(|n| n > self.limit)
                        {
                            return Err(std::io::Error::other(
                                "expression output byte budget exceeded",
                            ));
                        }
                        self.output.extend_from_slice(bytes);
                        Ok(bytes.len())
                    }
                    fn flush(&mut self) -> std::io::Result<()> {
                        Ok(())
                    }
                }
                let mut writer = Limited {
                    output: Vec::new(),
                    limit,
                };
                serde_json::to_writer_pretty(&mut writer, value)
                    .map_err(|_| anyhow::anyhow!("expression output byte budget exceeded"))?;
                Value::String(String::from_utf8(writer.output)?)
            } else {
                Value::String(serde_json::to_string_pretty(value)?)
            }
        }
        "fromjson" => serde_json::from_str(&s(0)).map_err(|e| anyhow::anyhow!("fromJSON: {e}"))?,
        other => bail!("unsupported function `{other}()`"),
    })
}

/// Evaluate a bare expression (the inside of `${{ }}`).
pub fn evaluate(src: &str, ctx: &Ctx) -> Result<Value> {
    eval(&parse(src)?, ctx)
}

/// Replace every `${{ expr }}` in `s`.
pub fn interpolate(s: &str, ctx: &Ctx) -> Result<String> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(start) = rest.find("${{") {
        ctx.check_len(
            out.len()
                .checked_add(start)
                .ok_or_else(|| anyhow::anyhow!("expression output byte budget exceeded"))?,
        )?;
        out.push_str(&rest[..start]);
        let after = &rest[start + 3..];
        let end =
            find_close(after).ok_or_else(|| anyhow::anyhow!("unterminated `${{{{` in `{s}`"))?;
        let value = to_string(&evaluate(&after[..end], ctx)?);
        ctx.check_len(
            out.len()
                .checked_add(value.len())
                .ok_or_else(|| anyhow::anyhow!("expression output byte budget exceeded"))?,
        )?;
        out.push_str(&value);
        rest = &after[end + 2..];
    }
    ctx.check_len(
        out.len()
            .checked_add(rest.len())
            .ok_or_else(|| anyhow::anyhow!("expression output byte budget exceeded"))?,
    )?;
    out.push_str(rest);
    ctx.check_value(&out)?;
    Ok(out)
}

/// Like `interpolate`, but a string that is exactly one `${{ expr }}` keeps the
/// expression's type (needed for `strategy.matrix: ${{ fromJSON(...) }}`).
pub fn interpolate_value(s: &str, ctx: &Ctx) -> Result<Value> {
    let t = s.trim();
    if let Some(inner) = t.strip_prefix("${{").and_then(|r| r.strip_suffix("}}")) {
        if !inner.contains("${{") && find_close(inner).is_none() {
            return evaluate(inner, ctx);
        }
    }
    Ok(Value::String(interpolate(s, ctx)?))
}

/// Find the `}}` that closes an expression, skipping over quoted strings.
fn find_close(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    while i < b.len() {
        if in_str {
            if b[i] == b'\'' {
                if i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
        } else if b[i] == b'\'' {
            in_str = true;
        } else if b[i] == b'}' && i + 1 < b.len() && b[i + 1] == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Portable evaluation stages used before a worker has been selected.
/// Ordinary Runner evaluation does not opt into this stricter policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortableStage {
    Matrix,
    JobIf,
    RunsOn,
    Name,
}

/// Check availability using the same parsed AST as expression execution.
/// `bare` matches job `if:` syntax; other fields only evaluate `${{ }}`.
/// This checks context availability, not the authenticity of supplied facts.
pub fn validate_portable(text: &str, bare: bool, stage: PortableStage) -> Result<()> {
    validate_portable_context(text, bare, stage, None)
}

/// Additionally reject github properties missing from explicit portable facts.
pub fn validate_portable_context(
    text: &str,
    bare: bool,
    stage: PortableStage,
    contexts: Option<&Map<String, Value>>,
) -> Result<()> {
    let mut rest = text;
    let mut found = false;
    while let Some(start) = rest.find("${{") {
        found = true;
        let after = &rest[start + 3..];
        let end =
            find_close(after).ok_or_else(|| anyhow::anyhow!("unterminated portable expression"))?;
        // condition() reparses mixed interpolation output as an expression.
        // Its runtime-generated AST cannot be approved before evaluation.
        if bare && (!text[..start].trim().is_empty() || !after[end + 2..].trim().is_empty()) {
            bail!("portable if requires one complete expression");
        }
        portable_ast(&parse(&after[..end])?, stage, contexts)?;
        rest = &after[end + 2..];
    }
    if bare && !found && !text.trim().is_empty() {
        portable_ast(&parse(text)?, stage, contexts)?;
    }
    Ok(())
}

fn portable_ast(
    ast: &Ast,
    stage: PortableStage,
    contexts: Option<&Map<String, Value>>,
) -> Result<()> {
    // A github chain must select a literal portable property before the
    // object is read. Reject computed indices and whole-object serialization,
    // even in dead branches; no guessed worker workspace or token is supplied.
    let mut base = ast;
    let mut keys = Vec::new();
    loop {
        match base {
            Ast::Prop(parent, key) => {
                keys.push(Some(key.as_ref()));
                base = parent;
            }
            Ast::Star(parent) => {
                keys.push(None);
                base = parent;
            }
            _ => break,
        }
    }
    if matches!(base, Ast::Ctx(name) if name.eq_ignore_ascii_case("github")) {
        let Some(Some(Ast::Lit(Value::String(first)))) = keys.last() else {
            bail!("portable github requires a literal property");
        };
        if !matches!(
            first.to_ascii_lowercase().as_str(),
            "event_name"
                | "event"
                | "sha"
                | "ref"
                | "ref_name"
                | "head_ref"
                | "base_ref"
                | "repository"
                | "repository_owner"
                | "workflow"
                | "job"
                | "run_id"
                | "run_number"
                | "run_attempt"
                | "actor"
                | "server_url"
                | "api_url"
        ) {
            bail!("github property is unavailable before worker selection");
        }
        if !keys
            .iter()
            .all(|k| matches!(k, Some(Ast::Lit(Value::String(_)))))
        {
            bail!("computed github properties are unavailable before worker selection");
        }
        if let Some(contexts) = contexts {
            let mut value = contexts
                .get("github")
                .ok_or_else(|| anyhow::anyhow!("github facts unavailable"))?
                .clone();
            for key in keys.iter().rev() {
                let Some(Ast::Lit(Value::String(key))) = key else {
                    unreachable!()
                };
                value = get_prop(&value, &Value::String(key.clone()));
                if value.is_null() {
                    bail!("github property unavailable in supplied facts");
                }
            }
        }
        return Ok(());
    }
    match ast {
        Ast::Lit(_) => Ok(()),
        Ast::Ctx(name) => {
            let name = name.to_ascii_lowercase();
            if matches!(name.as_str(), "needs" | "vars" | "inputs")
                || (matches!(stage, PortableStage::RunsOn | PortableStage::Name)
                    && matches!(name.as_str(), "matrix" | "strategy"))
            {
                Ok(())
            } else {
                bail!("context is unavailable at portable {stage:?} stage")
            }
        }
        Ast::Prop(base, key) => {
            portable_ast(base, stage, contexts)?;
            portable_ast(key, stage, contexts)
        }
        Ast::Star(base) | Ast::Not(base) => portable_ast(base, stage, contexts),
        Ast::Bin(_, left, right) => {
            portable_ast(left, stage, contexts)?;
            portable_ast(right, stage, contexts)
        }
        Ast::Call(name, args) => {
            let ordinary = matches!(
                name.as_str(),
                "contains" | "startswith" | "endswith" | "format" | "join" | "tojson" | "fromjson"
            );
            let status = stage == PortableStage::JobIf
                && matches!(
                    name.as_str(),
                    "success" | "failure" | "cancelled" | "always"
                );
            if !ordinary && !status {
                bail!("function is unavailable at portable {stage:?} stage");
            }
            for arg in args {
                portable_ast(arg, stage, contexts)?;
            }
            Ok(())
        }
    }
}

/// What an expression takes from the `needs` context.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum NeedsRef {
    /// `needs.<job>.outputs.<name>`, however it is spelled.
    Output { job: String, name: String },
    /// Outputs of one job taken without naming one: `needs.<job>`,
    /// `needs.<job>.outputs`, `needs.<job>.outputs.*`, `needs.<job>.outputs[x]`.
    JobOutputs(String),
    /// `needs` taken whole or through a computed job: `toJSON(needs)`,
    /// `needs[matrix.job]`.
    All,
}

enum Seg {
    Name(String),
    Star,
    Dyn,
}

fn collect_needs(ast: &Ast, out: &mut Vec<NeedsRef>) {
    let mut segs: Vec<Seg> = Vec::new();
    let mut computed: Vec<&Ast> = Vec::new();
    let mut cur = ast;
    loop {
        match cur {
            Ast::Prop(base, key) => {
                match &**key {
                    Ast::Lit(Value::String(s)) => segs.push(Seg::Name(s.clone())),
                    other => {
                        segs.push(Seg::Dyn);
                        computed.push(other);
                    }
                }
                cur = base;
            }
            Ast::Star(base) => {
                segs.push(Seg::Star);
                cur = base;
            }
            _ => break,
        }
    }
    segs.reverse();
    match cur {
        Ast::Ctx(name) if name.eq_ignore_ascii_case("needs") => {
            let is = |s: Option<&Seg>, w: &str| matches!(s, Some(Seg::Name(n)) if n.eq_ignore_ascii_case(w));
            match segs.first() {
                None | Some(Seg::Star) | Some(Seg::Dyn) => out.push(NeedsRef::All),
                Some(Seg::Name(job)) => {
                    let job = job.clone();
                    match segs.get(1) {
                        None => out.push(NeedsRef::JobOutputs(job)),
                        Some(_) if is(segs.get(1), "outputs") => match segs.get(2) {
                            Some(Seg::Name(name)) => out.push(NeedsRef::Output {
                                job,
                                name: name.clone(),
                            }),
                            _ => out.push(NeedsRef::JobOutputs(job)),
                        },
                        // `result`, or a name computed: it could be `outputs`
                        Some(Seg::Dyn) => out.push(NeedsRef::JobOutputs(job)),
                        Some(_) => {}
                    }
                }
            }
        }
        Ast::Call(_, args) => args.iter().for_each(|a| collect_needs(a, out)),
        Ast::Not(a) => collect_needs(a, out),
        Ast::Bin(_, a, b) => {
            collect_needs(a, out);
            collect_needs(b, out);
        }
        _ => {}
    }
    for k in computed {
        collect_needs(k, out);
    }
}

/// What `text` takes from `needs`. `${{ }}` expressions in it are read; with
/// `bare`, the whole text is an expression too (an `if:`). Text that mentions
/// `needs` but cannot be read counts as taking all of it.
pub fn needs_refs(text: &str, bare: bool) -> Vec<NeedsRef> {
    let mut exprs: Vec<&str> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("${{") {
        let after = &rest[start + 3..];
        match find_close(after) {
            Some(end) => {
                exprs.push(&after[..end]);
                rest = &after[end + 2..];
            }
            None => {
                exprs.push(after);
                break;
            }
        }
    }
    if bare && exprs.is_empty() {
        exprs.push(text);
    }
    let mut out = Vec::new();
    for e in exprs {
        match parse(e) {
            Ok(ast) => collect_needs(&ast, &mut out),
            Err(_) if e.to_ascii_lowercase().contains("needs") => out.push(NeedsRef::All),
            Err(_) => {}
        }
    }
    out
}

/// Does an `if:` condition call a status function? If not, GitHub wraps it in `success() && (...)`.
fn has_status_fn(src: &str) -> bool {
    let l = src.to_ascii_lowercase();
    ["success(", "failure(", "always(", "cancelled("]
        .iter()
        .any(|f| l.contains(f))
}

/// Does an `if:` value leave a job to the default `success()` gate, so that it
/// is skipped when a job it needs did not succeed? False if it calls
/// `failure()`, `cancelled()` or `always()`.
pub(crate) fn success_gated(raw: Option<&str>) -> bool {
    let l = raw.unwrap_or("").to_ascii_lowercase();
    !["failure(", "cancelled(", "always("]
        .iter()
        .any(|f| l.contains(f))
}

/// Evaluate an `if:` value (with or without `${{ }}`); `None` means the default `success()`.
pub fn condition(raw: Option<&str>, ctx: &Ctx) -> Result<bool> {
    let raw = match raw {
        None => return Ok(ctx.status.success),
        Some(r) => r.trim(),
    };
    let inner = match raw.strip_prefix("${{").and_then(|r| r.strip_suffix("}}")) {
        Some(i) if find_close(i).is_none() => i.trim().to_string(),
        _ if raw.contains("${{") => interpolate(raw, ctx)?,
        _ => raw.to_string(),
    };
    if inner.is_empty() {
        return Ok(ctx.status.success);
    }
    let v = truthy(&evaluate(&inner, ctx)?);
    if has_status_fn(&inner) {
        Ok(v)
    } else {
        Ok(ctx.status.success && v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx_map() -> Map<String, Value> {
        let v = json!({
            "github": {"event_name": "pull_request", "event": {"pull_request": {"draft": false}}, "sha": "abc"},
            "matrix": {"type": "e2e", "port": 33306, "check_generated": true, "shard": "1/2"},
            "steps": {"prepare": {"outcome": "success"}},
            "env": {"node-version": "22.1.0"},
            "needs": {"list": {"outputs": {"shards": "{\"project\":[\"a\",\"b\"]}"}}}
        });
        v.as_object().unwrap().clone()
    }

    #[test]
    fn job_level_draft_check() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        assert!(condition(Some("github.event.pull_request.draft == false"), &c).unwrap());
        assert!(condition(Some("${{ github.event_name == 'pull_request' }}"), &c).unwrap());
    }

    #[test]
    fn status_functions_and_steps() {
        let m = ctx_map();
        let failed = Ctx::new(&m, Status::Failure);
        assert!(condition(
            Some("${{ !cancelled() && steps.prepare.outcome == 'success' }}"),
            &failed
        )
        .unwrap());
        assert!(!condition(Some("matrix.type == 'e2e'"), &failed).unwrap());
        assert!(condition(Some("always()"), &failed).unwrap());
        assert!(!condition(None, &failed).unwrap());
    }

    #[test]
    fn matrix_bools_and_numbers() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        assert!(condition(Some("${{ matrix.check_generated }}"), &c).unwrap());
        assert_eq!(
            interpolate("DB_PORT=${{ matrix.port }} x", &c).unwrap(),
            "DB_PORT=33306 x"
        );
        assert_eq!(
            interpolate("${{ env.node-version }}", &c).unwrap(),
            "22.1.0"
        );
        assert!(condition(Some("matrix.type == 'E2E'"), &c).unwrap());
    }

    #[test]
    fn from_json_keeps_type() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        let v = interpolate_value("${{ fromJSON(needs.list.outputs.shards) }}", &c).unwrap();
        assert_eq!(v, json!({"project": ["a", "b"]}));
    }

    #[test]
    fn functions_and_operators() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        let t = |s: &str| truthy(&evaluate(s, &c).unwrap());
        assert!(t("contains('Hello', 'ell')"));
        assert!(t("contains(fromJSON('[\"a\",\"b\"]'), 'b')"));
        assert!(t("startsWith('refs/heads/x', 'refs/')"));
        assert!(t("format('{0}-{1}', 'a', 1) == 'a-1'"));
        assert!(t("1 < 2 && '' == 0 && null == 0"));
        assert!(!t("'a' && ''"));
        assert_eq!(
            interpolate("${{ github.head_ref || 'main' }}", &c).unwrap(),
            "main"
        );
        assert_eq!(interpolate("k-${{ hashFiles('a/**') }}", &c).unwrap(), "k-");
    }

    #[test]
    fn unknown_things_are_errors() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        assert!(evaluate("nope.x", &c).is_err());
        assert!(evaluate("frob(1)", &c).is_err());
        assert!(evaluate("1 +", &c).is_err());
    }

    #[test]
    fn secrets_are_flagged() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        assert_eq!(interpolate("${{ secrets.TOKEN }}", &c).unwrap(), "");
        assert!(c.used_secrets.get());
        let c = Ctx::new(&m, Status::Success);
        interpolate("${{ github.token }}", &c).unwrap();
        assert!(c.used_secrets.get());
        let c = Ctx::new(&m, Status::Success);
        interpolate("${{ github.sha }}", &c).unwrap();
        assert!(!c.used_secrets.get());
    }

    #[test]
    fn strings_with_braces() {
        let m = ctx_map();
        let c = Ctx::new(&m, Status::Success);
        assert_eq!(interpolate("${{ format('{{x}}') }}", &c).unwrap(), "{x}");
    }
}

#[cfg(test)]
mod portable_tests {
    use super::*;
    #[test]
    fn stages_validate_actual_ast_and_fail_closed_in_dead_branches() {
        for expression in [
            "fromJSON(needs.seed.outputs.matrix)",
            "inputs[vars.key]",
            "toJSON(needs)",
            "github.event.pull_request.head.ref",
            "github['repository']",
        ] {
            validate_portable(expression, true, PortableStage::Matrix).unwrap();
        }
        for stage in [
            PortableStage::Matrix,
            PortableStage::JobIf,
            PortableStage::RunsOn,
            PortableStage::Name,
        ] {
            for expression in [
                "runner.os",
                "env.CACHE",
                "secrets.X",
                "steps.out.outputs.x",
                "github.workspace",
                "github.token",
                "github[inputs.key]",
                "github.event[inputs.key]",
                "toJSON(github)",
                "github.*",
                "hashFiles('**')",
                "false && runner.os",
                "true || secrets.X",
                "fromJSON(toJSON(github)).workspace",
            ] {
                assert!(
                    validate_portable(expression, true, stage).is_err(),
                    "{stage:?}: {expression}"
                );
            }
        }
        assert!(validate_portable("matrix.os", true, PortableStage::Matrix).is_err());
        assert!(validate_portable("strategy.job-total", true, PortableStage::Matrix).is_err());
        assert!(validate_portable("matrix.os", true, PortableStage::JobIf).is_err());
        assert!(validate_portable("strategy.job-total", true, PortableStage::JobIf).is_err());
        for stage in [PortableStage::RunsOn, PortableStage::Name] {
            validate_portable(
                "format('{0}-{1}', matrix.os, strategy.job-index)",
                true,
                stage,
            )
            .unwrap();
        }
        validate_portable(
            "always() && needs.seed.result == 'success'",
            true,
            PortableStage::JobIf,
        )
        .unwrap();
        assert!(validate_portable("success()", true, PortableStage::Matrix).is_err());
    }
    #[test]
    fn portable_fact_lookup_shares_case_insensitive_property_semantics() {
        let contexts = json_context_for_portable();
        validate_portable_context(
            "github.ACTOR == 'known'",
            true,
            PortableStage::JobIf,
            Some(&contexts),
        )
        .unwrap();
        validate_portable_context(
            "${{ github.event.COMMITS.message }}",
            false,
            PortableStage::Name,
            Some(&contexts),
        )
        .unwrap();
        assert!(validate_portable_context(
            "github.base_ref",
            true,
            PortableStage::JobIf,
            Some(&contexts)
        )
        .is_err());
    }
    fn json_context_for_portable() -> Map<String, Value> {
        [("github".into(), serde_json::json!({"actor":"known", "event":{"commits":[{"message":"first"},{"message":"second"}]}}))].into_iter().collect()
    }
    #[test]
    fn interpolation_does_not_reparse_unvalidated_generated_if() {
        validate_portable("plain runner.os", false, PortableStage::RunsOn).unwrap();
        validate_portable("worker-${{ matrix.os }}", false, PortableStage::RunsOn).unwrap();
        validate_portable(
            " ${{ needs.seed.result == 'success' }} ",
            true,
            PortableStage::JobIf,
        )
        .unwrap();
        for text in [
            "runner.${{ inputs.key }}",
            "${{ inputs.root }}.os",
            "${{ inputs.a }} == ${{ inputs.b }}",
            "${{ runner.os",
            "${{ needs[ }}",
        ] {
            assert!(
                validate_portable(text, true, PortableStage::JobIf).is_err(),
                "{text}"
            );
        }
    }
}

#[cfg(test)]
mod output_budget_tests {
    use super::*;
    #[test]
    fn opt_in_bounds_format_join_and_json_without_changing_default_results() {
        let contexts: Map<String,Value> = [("vars".into(),serde_json::json!({"blob":"x".repeat(32), "items":["a","b","c"], "nested":{"a":{"b":{"c":{"d":"value"}}}}}))].into_iter().collect();
        let ordinary = Ctx::new(&contexts, Status::Success);
        let limited = Ctx::new(&contexts, Status::Success).with_output_limit(256);
        let text = format!("format('{}', vars.blob)", "{0}".repeat(32));
        assert_eq!(
            evaluate(&text, &ordinary).unwrap(),
            Value::String("x".repeat(1024))
        );
        assert!(evaluate(&text, &limited)
            .unwrap_err()
            .to_string()
            .contains("budget"));
        let join = format!("join(vars.items, '{}')", "x".repeat(128));
        assert_eq!(
            evaluate(&join, &ordinary).unwrap().as_str().unwrap().len(),
            259
        );
        assert!(evaluate(&join, &limited).is_err());
        let tiny = Ctx::new(&contexts, Status::Success).with_output_limit(256);
        let interpolation = format!("{}${{{{ vars.blob }}}}", "a".repeat(240));
        assert!(interpolate(&interpolation, &tiny).is_err());
        let nested: Value = (0..12).fold(
            Value::String("value".into()),
            |inner, _| serde_json::json!({"a":inner}),
        );
        let small: Map<String, Value> = [("vars".into(), serde_json::json!({"nested":nested}))]
            .into_iter()
            .collect();
        assert!(
            evaluate("toJSON(vars.nested)", &Ctx::new(&small, Status::Success))
                .unwrap()
                .as_str()
                .unwrap()
                .len()
                > 128
        );
        assert!(evaluate(
            "toJSON(vars.nested)",
            &Ctx::new(&small, Status::Success).with_output_limit(128)
        )
        .is_err());
    }
}
