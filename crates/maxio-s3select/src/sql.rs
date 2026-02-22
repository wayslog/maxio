use crate::error::{Result, S3SelectError};

#[derive(Debug, Clone)]
pub struct Query {
    pub select: SelectClause,
    pub from: String,
    pub where_clause: Option<Expr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SelectClause {
    Star,
    Columns(Vec<ColumnRef>),
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(String),
    Literal(Literal),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Like,
}

pub fn parse(sql: &str) -> Result<Query> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();

    if !upper.starts_with("SELECT") {
        return Err(S3SelectError::InvalidSql(
            "query must start with SELECT".to_string(),
        ));
    }

    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| S3SelectError::InvalidSql("missing FROM clause".to_string()))?;

    let select_part = &sql[6..from_pos].trim();
    let select = parse_select_clause(select_part)?;

    let after_from = &sql[from_pos + 6..];
    let (from, rest) = parse_from_clause(after_from)?;

    let (where_clause, rest) = parse_where_clause(rest)?;
    let limit = parse_limit_clause(rest)?;

    Ok(Query {
        select,
        from,
        where_clause,
        limit,
    })
}

fn parse_select_clause(s: &str) -> Result<SelectClause> {
    let s = s.trim();
    if s == "*" {
        return Ok(SelectClause::Star);
    }

    let columns: Vec<ColumnRef> = s
        .split(',')
        .map(|col| {
            let col = col.trim();
            let parts: Vec<&str> = col.splitn(2, " AS ").collect();
            ColumnRef {
                name: normalize_column_name(parts[0].trim()),
                alias: parts.get(1).map(|s| s.trim().to_string()),
            }
        })
        .collect();

    Ok(SelectClause::Columns(columns))
}

fn normalize_column_name(name: &str) -> String {
    let name = name.trim();
    if name.starts_with("s.") || name.starts_with("S.") {
        name[2..].to_string()
    } else {
        name.to_string()
    }
}

fn parse_from_clause(s: &str) -> Result<(String, &str)> {
    let s = s.trim();
    let upper = s.to_uppercase();

    let end = upper
        .find(" WHERE ")
        .or_else(|| upper.find(" LIMIT "))
        .unwrap_or(s.len());

    let from = s[..end].trim().to_string();
    Ok((from, &s[end..]))
}

fn parse_where_clause(s: &str) -> Result<(Option<Expr>, &str)> {
    let s = s.trim();
    let upper = s.to_uppercase();

    if !upper.starts_with("WHERE ") {
        return Ok((None, s));
    }

    let after_where = &s[6..];
    let upper_rest = after_where.to_uppercase();
    let end = upper_rest.find(" LIMIT ").unwrap_or(after_where.len());

    let condition = &after_where[..end].trim();
    let expr = parse_expr(condition)?;

    Ok((Some(expr), &after_where[end..]))
}

fn parse_expr(s: &str) -> Result<Expr> {
    let s = s.trim();
    let upper = s.to_uppercase();

    if let Some(pos) = find_logical_op(&upper, " OR ") {
        let left = parse_expr(&s[..pos])?;
        let right = parse_expr(&s[pos + 4..])?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        });
    }

    if let Some(pos) = find_logical_op(&upper, " AND ") {
        let left = parse_expr(&s[..pos])?;
        let right = parse_expr(&s[pos + 5..])?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        });
    }

    parse_comparison(s)
}

fn find_logical_op(s: &str, op: &str) -> Option<usize> {
    let mut depth = 0;
    let mut i = 0;
    let bytes = s.as_bytes();

    while i < s.len() {
        if bytes[i] == b'(' {
            depth += 1;
        } else if bytes[i] == b')' {
            depth -= 1;
        } else if depth == 0 && s[i..].starts_with(op) {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_comparison(s: &str) -> Result<Expr> {
    let s = s.trim();
    let upper = s.to_uppercase();

    let ops = [
        (">=", BinaryOp::Ge),
        ("<=", BinaryOp::Le),
        ("!=", BinaryOp::Ne),
        ("<>", BinaryOp::Ne),
        ("=", BinaryOp::Eq),
        (">", BinaryOp::Gt),
        ("<", BinaryOp::Lt),
    ];

    for (op_str, op) in ops {
        if let Some(pos) = s.find(op_str) {
            let left = parse_atom(&s[..pos])?;
            let right = parse_atom(&s[pos + op_str.len()..])?;
            return Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
    }

    if upper.contains(" LIKE ") {
        let pos = upper.find(" LIKE ").unwrap();
        let left = parse_atom(&s[..pos])?;
        let right = parse_atom(&s[pos + 6..])?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Like,
            right: Box::new(right),
        });
    }

    if upper.ends_with(" IS NULL") {
        let col = &s[..s.len() - 8];
        return Ok(Expr::IsNull(Box::new(parse_atom(col)?)));
    }

    if upper.ends_with(" IS NOT NULL") {
        let col = &s[..s.len() - 12];
        return Ok(Expr::IsNotNull(Box::new(parse_atom(col)?)));
    }

    parse_atom(s)
}

fn parse_atom(s: &str) -> Result<Expr> {
    let s = s.trim();

    if s.starts_with('(') && s.ends_with(')') {
        return parse_expr(&s[1..s.len() - 1]);
    }

    if s.starts_with('\'') && s.ends_with('\'') {
        return Ok(Expr::Literal(Literal::String(
            s[1..s.len() - 1].to_string(),
        )));
    }

    if s.starts_with('"') && s.ends_with('"') {
        return Ok(Expr::Literal(Literal::String(
            s[1..s.len() - 1].to_string(),
        )));
    }

    if let Ok(n) = s.parse::<f64>() {
        return Ok(Expr::Literal(Literal::Number(n)));
    }

    let upper = s.to_uppercase();
    if upper == "TRUE" {
        return Ok(Expr::Literal(Literal::Bool(true)));
    }
    if upper == "FALSE" {
        return Ok(Expr::Literal(Literal::Bool(false)));
    }
    if upper == "NULL" {
        return Ok(Expr::Literal(Literal::Null));
    }

    Ok(Expr::Column(normalize_column_name(s)))
}

fn parse_limit_clause(s: &str) -> Result<Option<usize>> {
    let s = s.trim();
    let upper = s.to_uppercase();

    if !upper.starts_with("LIMIT ") {
        return Ok(None);
    }

    let num_str = s[6..].trim();
    let limit = num_str
        .parse::<usize>()
        .map_err(|_| S3SelectError::InvalidSql(format!("invalid LIMIT value: {num_str}")))?;

    Ok(Some(limit))
}

pub fn evaluate(query: &Query, records: Vec<Vec<String>>) -> Result<Vec<Vec<String>>> {
    if records.is_empty() {
        return Ok(records);
    }

    let headers = &records[0];
    let data_rows = &records[1..];

    let filtered: Vec<&Vec<String>> = data_rows
        .iter()
        .filter(|row| {
            query
                .where_clause
                .as_ref()
                .map(|expr| eval_expr(expr, headers, row).unwrap_or(false))
                .unwrap_or(true)
        })
        .collect();

    let limited: Vec<&Vec<String>> = match query.limit {
        Some(n) => filtered.into_iter().take(n).collect(),
        None => filtered,
    };

    let result_headers = match &query.select {
        SelectClause::Star => headers.clone(),
        SelectClause::Columns(cols) => cols
            .iter()
            .map(|c| c.alias.clone().unwrap_or_else(|| c.name.clone()))
            .collect(),
    };

    let mut result = vec![result_headers];

    for row in limited {
        let projected = match &query.select {
            SelectClause::Star => row.clone(),
            SelectClause::Columns(cols) => cols
                .iter()
                .map(|c| get_column_value(&c.name, headers, row))
                .collect(),
        };
        result.push(projected);
    }

    Ok(result)
}

fn get_column_value(name: &str, headers: &[String], row: &[String]) -> String {
    if let Some(idx) = name.strip_prefix('_') {
        if let Ok(i) = idx.parse::<usize>() {
            return row.get(i.saturating_sub(1)).cloned().unwrap_or_default();
        }
    }

    headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case(name))
        .and_then(|i| row.get(i))
        .cloned()
        .unwrap_or_default()
}

fn eval_expr(expr: &Expr, headers: &[String], row: &[String]) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_value(left, headers, row)?;
            let rval = eval_value(right, headers, row)?;
            Ok(compare(&lval, op, &rval))
        }
        Expr::Not(inner) => Ok(!eval_expr(inner, headers, row)?),
        Expr::IsNull(inner) => {
            let val = eval_value(inner, headers, row)?;
            Ok(val.is_empty())
        }
        Expr::IsNotNull(inner) => {
            let val = eval_value(inner, headers, row)?;
            Ok(!val.is_empty())
        }
        Expr::Literal(Literal::Bool(b)) => Ok(*b),
        _ => Ok(true),
    }
}

fn eval_value(expr: &Expr, headers: &[String], row: &[String]) -> Result<String> {
    match expr {
        Expr::Column(name) => Ok(get_column_value(name, headers, row)),
        Expr::Literal(lit) => Ok(literal_to_string(lit)),
        _ => Ok(String::new()),
    }
}

fn literal_to_string(lit: &Literal) -> String {
    match lit {
        Literal::String(s) => s.clone(),
        Literal::Number(n) => n.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Null => String::new(),
    }
}

fn compare(left: &str, op: &BinaryOp, right: &str) -> bool {
    match op {
        BinaryOp::Eq => left == right,
        BinaryOp::Ne => left != right,
        BinaryOp::Lt => compare_numeric_or_string(left, right, |a, b| a < b, |a, b| a < b),
        BinaryOp::Le => compare_numeric_or_string(left, right, |a, b| a <= b, |a, b| a <= b),
        BinaryOp::Gt => compare_numeric_or_string(left, right, |a, b| a > b, |a, b| a > b),
        BinaryOp::Ge => compare_numeric_or_string(left, right, |a, b| a >= b, |a, b| a >= b),
        BinaryOp::And => left == "true" && right == "true",
        BinaryOp::Or => left == "true" || right == "true",
        BinaryOp::Like => like_match(left, right),
    }
}

fn compare_numeric_or_string<F, G>(left: &str, right: &str, num_cmp: F, str_cmp: G) -> bool
where
    F: Fn(f64, f64) -> bool,
    G: Fn(&str, &str) -> bool,
{
    if let (Ok(l), Ok(r)) = (left.parse::<f64>(), right.parse::<f64>()) {
        num_cmp(l, r)
    } else {
        str_cmp(left, right)
    }
}

fn like_match(value: &str, pattern: &str) -> bool {
    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");

    let full_pattern = format!("^{}$", regex_pattern);

    value.len() <= 10000 && {
        let mut chars = full_pattern.chars().peekable();
        let mut val_chars = value.chars();
        simple_match(&mut chars, &mut val_chars)
    }
}

fn simple_match(
    pattern: &mut std::iter::Peekable<std::str::Chars>,
    value: &mut std::str::Chars,
) -> bool {
    while let Some(p) = pattern.next() {
        match p {
            '^' => continue,
            '$' => return value.next().is_none(),
            '.' if pattern.peek() == Some(&'*') => {
                pattern.next();
                loop {
                    let mut val_clone = value.clone();
                    let mut pat_clone = pattern.clone();
                    if simple_match(&mut pat_clone, &mut val_clone) {
                        return true;
                    }
                    if value.next().is_none() {
                        return pattern.clone().all(|c| c == '$');
                    }
                }
            }
            '.' => {
                if value.next().is_none() {
                    return false;
                }
            }
            c => {
                if value.next() != Some(c) {
                    return false;
                }
            }
        }
    }
    value.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = parse("SELECT * FROM s3object").unwrap();
        assert!(matches!(query.select, SelectClause::Star));
        assert_eq!(query.from, "s3object");
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = parse("SELECT name, age FROM s3object WHERE age > 18").unwrap();
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_evaluate_filter() {
        let query = parse("SELECT * FROM s3object WHERE age > 20").unwrap();
        let records = vec![
            vec!["name".to_string(), "age".to_string()],
            vec!["Alice".to_string(), "25".to_string()],
            vec!["Bob".to_string(), "18".to_string()],
            vec!["Charlie".to_string(), "30".to_string()],
        ];
        let result = evaluate(&query, records).unwrap();
        assert_eq!(result.len(), 3);
    }
}
