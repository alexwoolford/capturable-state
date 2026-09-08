use anyhow::{bail, Context, Result};
use rusqlite::Connection;

const OUTBOX: &str = "_outbox";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureMode {
    /// Insert: after. Update: before+after. Delete: before.
    Full,
    /// Insert/update: after only. Delete: key only.
    After,
    /// Identity only on every op.
    Key,
}

#[derive(Debug, Clone)]
pub struct TableSpec<'a> {
    pub name: &'a str,
    pub mode: CaptureMode,
    pub exclude: &'a [&'a str],
}

impl<'a> TableSpec<'a> {
    pub fn new(name: &'a str, mode: CaptureMode) -> Self {
        Self {
            name,
            mode,
            exclude: &[],
        }
    }

    pub const fn exclude(mut self, cols: &'a [&'a str]) -> Self {
        self.exclude = cols;
        self
    }
}

#[derive(Debug)]
struct Col {
    name: String,
    pk: i64,
}

pub fn install_triggers(conn: &Connection, tables: &[TableSpec<'_>]) -> Result<()> {
    for spec in tables {
        if spec.name == OUTBOX {
            bail!("refusing to capture {OUTBOX}");
        }
        validate_ident(spec.name)?;
        for c in spec.exclude {
            validate_ident(c)?;
        }
        let cols = table_columns(conn, spec.name)?;
        if cols.is_empty() {
            bail!("table {} has no columns (missing?)", spec.name);
        }
        let pk = pk_columns(&cols);
        let without_rowid = is_without_rowid(conn, spec.name)?;
        if pk.is_empty() && without_rowid {
            bail!("{} is WITHOUT ROWID and has no PRIMARY KEY", spec.name);
        }
        drop_capture_triggers(conn, spec.name)?;
        conn.execute_batch(&trigger_sql(spec, &cols, &pk, without_rowid)?)
            .with_context(|| format!("install triggers for {}", spec.name))?;
    }
    Ok(())
}

pub fn assert_triggers(conn: &Connection, tables: &[TableSpec<'_>]) -> Result<()> {
    for spec in tables {
        let cols = table_columns(conn, spec.name)?;
        let payload = payload_columns(&cols, spec.exclude);
        let pk = pk_columns(&cols);
        let without_rowid = is_without_rowid(conn, spec.name)?;
        let key_names: Vec<String> = if pk.is_empty() {
            vec!["rowid".into()]
        } else {
            pk.clone()
        };

        for op in ["I", "U", "D"] {
            let name = trigger_name(op, spec.name);
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
                    [&name],
                    |row| row.get(0),
                )
                .with_context(|| format!("missing trigger {name}"))?;

            for k in &key_names {
                if !sql.contains(&format!("'{k}'")) {
                    bail!("trigger {name} missing key column {k}");
                }
            }

            let expect_after = matches!(
                (op, spec.mode),
                ("I" | "U", CaptureMode::Full | CaptureMode::After)
            );
            let expect_before = matches!((op, spec.mode), ("U" | "D", CaptureMode::Full));

            if expect_after {
                for c in &payload {
                    if !sql.contains(&format!("NEW.\"{c}\"")) && !sql.contains(&format!("NEW.{c}"))
                    {
                        bail!("trigger {name} missing NEW.{c}; regenerate after schema change");
                    }
                }
            }
            if expect_before {
                for c in &payload {
                    if !sql.contains(&format!("OLD.\"{c}\"")) && !sql.contains(&format!("OLD.{c}"))
                    {
                        bail!("trigger {name} missing OLD.{c}; regenerate after schema change");
                    }
                }
            }

            for ex in spec.exclude {
                if key_names.iter().any(|k| k == ex) {
                    continue;
                }
                if sql.contains(&format!("NEW.\"{ex}\"")) || sql.contains(&format!("NEW.{ex}")) {
                    bail!("trigger {name} still captures excluded column {ex}");
                }
            }

            if without_rowid && sql.contains("NEW.rowid") {
                bail!("trigger {name} uses NEW.rowid on WITHOUT ROWID table");
            }
        }
    }
    Ok(())
}

fn trigger_sql(
    spec: &TableSpec<'_>,
    cols: &[Col],
    pk: &[String],
    without_rowid: bool,
) -> Result<String> {
    let payload = payload_columns(cols, spec.exclude);
    let key_new = key_expr(pk, "NEW", without_rowid)?;
    let key_old = key_expr(pk, "OLD", without_rowid)?;
    let after_new = json_object_expr(&payload, "NEW");
    let before_old = json_object_expr(&payload, "OLD");

    let null = "NULL".to_string();
    let (ins_before, ins_after) = match spec.mode {
        CaptureMode::Full | CaptureMode::After => (null.clone(), after_new.clone()),
        CaptureMode::Key => (null.clone(), null.clone()),
    };
    let (upd_before, upd_after) = match spec.mode {
        CaptureMode::Full => (before_old.clone(), after_new),
        CaptureMode::After => (null.clone(), after_new),
        CaptureMode::Key => (null.clone(), null.clone()),
    };
    let (del_before, del_after) = match spec.mode {
        CaptureMode::Full => (before_old, null),
        CaptureMode::After | CaptureMode::Key => (null.clone(), null),
    };

    let t = spec.name;
    Ok(format!(
        r#"
CREATE TRIGGER {ti} AFTER INSERT ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'I', {key_new}, {ins_before}, {ins_after});
END;
CREATE TRIGGER {tu} AFTER UPDATE ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'U', {key_new}, {upd_before}, {upd_after});
END;
CREATE TRIGGER {td} AFTER DELETE ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'D', {key_old}, {del_before}, {del_after});
END;
"#,
        ti = trigger_name("I", t),
        tu = trigger_name("U", t),
        td = trigger_name("D", t),
        out = OUTBOX,
    ))
}

fn trigger_name(op: &str, table: &str) -> String {
    format!("_cap_{op}_{table}")
}

fn drop_capture_triggers(conn: &Connection, table: &str) -> Result<()> {
    for op in ["I", "U", "D"] {
        let name = trigger_name(op, table);
        conn.execute(&format!("DROP TRIGGER IF EXISTS \"{name}\""), [])
            .with_context(|| format!("drop {name}"))?;
    }
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<Col>> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .with_context(|| format!("table_info {table}"))?;
    let cols = stmt
        .query_map([], |row| {
            Ok(Col {
                name: row.get(1)?,
                pk: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(cols)
}

fn pk_columns(cols: &[Col]) -> Vec<String> {
    let mut pk: Vec<(i64, String)> = cols
        .iter()
        .filter(|c| c.pk > 0)
        .map(|c| (c.pk, c.name.clone()))
        .collect();
    pk.sort_by_key(|(n, _)| *n);
    pk.into_iter().map(|(_, n)| n).collect()
}

fn payload_columns(cols: &[Col], exclude: &[&str]) -> Vec<String> {
    cols.iter()
        .filter(|c| !exclude.iter().any(|e| *e == c.name))
        .map(|c| c.name.clone())
        .collect()
}

fn key_expr(pk: &[String], prefix: &str, without_rowid: bool) -> Result<String> {
    if pk.is_empty() {
        if without_rowid {
            bail!("cannot use rowid on WITHOUT ROWID table");
        }
        return Ok(format!("json_object('rowid', {prefix}.rowid)"));
    }
    Ok(json_object_expr(pk, prefix))
}

fn json_object_expr(cols: &[String], prefix: &str) -> String {
    if cols.is_empty() {
        return "NULL".into();
    }
    let parts: Vec<String> = cols
        .iter()
        .map(|c| format!("'{c}', {prefix}.\"{c}\""))
        .collect();
    format!("json_object({})", parts.join(", "))
}

fn is_without_rowid(conn: &Connection, table: &str) -> Result<bool> {
    let sql: Option<String> = match conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get::<_, String>(0),
    ) {
        Ok(s) => Some(s),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("without-rowid check"),
    };
    Ok(sql.is_some_and(|s| {
        s.to_ascii_uppercase()
            .replace(['\n', '\t'], " ")
            .contains("WITHOUT ROWID")
    }))
}

fn validate_ident(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("invalid SQL identifier {name:?}")
    }
}
