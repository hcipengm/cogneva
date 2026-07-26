//! SQL migration runner used by the `cog-migrate` binary and tests.
//! The runner reads migration files from a directory and applies the ones
//! that have not yet been recorded in the `schema_migrations` table. Files
//! must be named `NNN_<name>.up.sql` (or `.down.sql`) — the leading number
//! determines ordering. The runner is idempotent and re-runnable.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use sqlx::{Executor, Row};
use tracing::{info, warn};

/// Database driver supported by the migrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    Postgres,
    Mysql,
}

impl std::str::FromStr for Driver {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Driver::Postgres),
            "mysql" | "mariadb" => Ok(Driver::Mysql),
            other => Err(anyhow!("unsupported driver: {other}")),
        }
    }
}

impl Driver {
    fn migrations_subdir(&self) -> &'static str {
        match self {
            Driver::Postgres => "postgres",
            Driver::Mysql => "mysql",
        }
    }
}

/// A single discovered migration file.
#[derive(Debug, Clone)]
pub struct Migration {
    pub version: i64,
    pub name: String,
    pub path: PathBuf,
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// Discover migrations under `migrations_dir/<driver>/`. Returns up-migrations
/// sorted ascending by version. Down-migrations are kept for cli use but are
/// not returned from this fn.
pub fn discover_up_migrations(migrations_dir: &Path, driver: Driver) -> Result<Vec<Migration>> {
    let dir = migrations_dir.join(driver.migrations_subdir());
    if !dir.exists() {
        return Err(anyhow!(
            "migrations directory does not exist: {}",
            dir.display()
        ));
    }

    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if let Some(m) = parse_migration_filename(&file_name, &path) {
            if m.direction == Direction::Up {
                out.push(m);
            }
        }
    }
    out.sort_by_key(|m| m.version);
    Ok(out)
}

fn parse_migration_filename(name: &str, path: &Path) -> Option<Migration> {
    // Expect format: NNN_label.{up,down}.sql
    let stem = name.strip_suffix(".sql")?;
    let (rest, direction) = if let Some(r) = stem.strip_suffix(".up") {
        (r, Direction::Up)
    } else {
        let r = stem.strip_suffix(".down")?;
        (r, Direction::Down)
    };

    let mut split = rest.splitn(2, '_');
    let version_str = split.next()?;
    let label = split.next().unwrap_or("");
    let version: i64 = version_str.parse().ok()?;
    Some(Migration {
        version,
        name: label.to_string(),
        path: path.to_path_buf(),
        direction,
    })
}

/// Top-level migrator object. Holds the connection details only — open the
/// pool inside `apply_*` so callers can choose lazy vs eager connect.
pub struct Migrator {
    pub migrations_dir: PathBuf,
}

impl Default for Migrator {
    fn default() -> Self {
        Self::new("crates/cog-storage/migrations")
    }
}

impl Migrator {
    pub fn new(migrations_dir: impl Into<PathBuf>) -> Self {
        Self {
            migrations_dir: migrations_dir.into(),
        }
    }

    /// Apply pending up-migrations against the given URL, dispatching by driver.
    pub async fn run(&self, database_url: &str) -> Result<()> {
        let driver = detect_driver(database_url)?;
        match driver {
            Driver::Postgres => self.run_postgres(database_url, false).await,
            Driver::Mysql => self.run_mysql(database_url, false).await,
        }
    }

    /// Same as [`run`] but only logs the SQL it *would* apply.
    pub async fn run_dry(&self, database_url: &str) -> Result<()> {
        let driver = detect_driver(database_url)?;
        match driver {
            Driver::Postgres => self.run_postgres(database_url, true).await,
            Driver::Mysql => self.run_mysql(database_url, true).await,
        }
    }

    async fn run_postgres(&self, url: &str, dry_run: bool) -> Result<()> {
        let migrations = discover_up_migrations(&self.migrations_dir, Driver::Postgres)?;
        if migrations.is_empty() {
            warn!(
                "no Postgres migrations found under {}",
                self.migrations_dir.display()
            );
            return Ok(());
        }
        info!("found {} postgres migrations", migrations.len());

        if dry_run {
            for m in &migrations {
                info!("[dry-run] would apply postgres {}: {}", m.version, m.name);
            }
            return Ok(());
        }

        let pool = sqlx::PgPool::connect(url)
            .await
            .with_context(|| "connect to postgres failed")?;

        // schema_migrations table for tracking applied versions
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                name    VARCHAR(255) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            "#,
        )
        .await
        .with_context(|| "create schema_migrations failed")?;

        for m in migrations {
            let row = sqlx::query("SELECT 1 FROM schema_migrations WHERE version = $1")
                .bind(m.version)
                .fetch_optional(&pool)
                .await?;
            if row.is_some() {
                continue;
            }
            let sql = std::fs::read_to_string(&m.path)
                .with_context(|| format!("read migration {}", m.path.display()))?;
            info!("applying postgres migration {}: {}", m.version, m.name);
            pool.execute(sql.as_str())
                .await
                .with_context(|| format!("apply migration {}", m.version))?;
            sqlx::query(
                "INSERT INTO schema_migrations (version, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(m.version)
            .bind(&m.name)
            .execute(&pool)
            .await?;
        }
        Ok(())
    }

    async fn run_mysql(&self, url: &str, dry_run: bool) -> Result<()> {
        let migrations = discover_up_migrations(&self.migrations_dir, Driver::Mysql)?;
        if migrations.is_empty() {
            warn!(
                "no MySQL migrations found under {}",
                self.migrations_dir.display()
            );
            return Ok(());
        }
        info!("found {} mysql migrations", migrations.len());

        if dry_run {
            for m in &migrations {
                info!("[dry-run] would apply mysql {}: {}", m.version, m.name);
            }
            return Ok(());
        }

        let pool = sqlx::MySqlPool::connect(url)
            .await
            .with_context(|| "connect to mysql failed")?;

        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                name    VARCHAR(255) NOT NULL,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            "#,
        )
        .await
        .with_context(|| "create schema_migrations failed")?;

        for m in migrations {
            let row = sqlx::query("SELECT 1 FROM schema_migrations WHERE version = ?")
                .bind(m.version)
                .fetch_optional(&pool)
                .await?;
            if row.is_some() {
                continue;
            }
            let sql = std::fs::read_to_string(&m.path)
                .with_context(|| format!("read migration {}", m.path.display()))?;
            info!("applying mysql migration {}: {}", m.version, m.name);
            // MySQL pool.execute treats the input as one statement; some
            // migration files include several statements. Split on `;` at
            // statement boundaries before sending.
            for stmt in split_sql_statements(&sql) {
                if stmt.trim().is_empty() {
                    continue;
                }
                pool.execute(stmt.as_str())
                    .await
                    .with_context(|| format!("apply migration {} stmt", m.version))?;
            }
            sqlx::query("INSERT IGNORE INTO schema_migrations (version, name) VALUES (?, ?)")
                .bind(m.version)
                .bind(&m.name)
                .execute(&pool)
                .await?;
        }
        Ok(())
    }

    /// List which versions are applied vs pending. Useful for tooling/tests.
    pub async fn status(&self, database_url: &str) -> Result<Vec<MigrationStatus>> {
        let driver = detect_driver(database_url)?;
        let migrations = discover_up_migrations(&self.migrations_dir, driver)?;
        let applied: Vec<i64> = match driver {
            Driver::Postgres => {
                let pool = sqlx::PgPool::connect(database_url).await?;
                pool.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
                ).await?;
                sqlx::query("SELECT version FROM schema_migrations")
                    .fetch_all(&pool)
                    .await?
                    .into_iter()
                    .map(|r| r.try_get::<i64, _>("version").unwrap_or(0))
                    .collect()
            }
            Driver::Mysql => {
                let pool = sqlx::MySqlPool::connect(database_url).await?;
                pool.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB",
                ).await?;
                sqlx::query("SELECT version FROM schema_migrations")
                    .fetch_all(&pool)
                    .await?
                    .into_iter()
                    .map(|r| r.try_get::<i64, _>("version").unwrap_or(0))
                    .collect()
            }
        };

        Ok(migrations
            .into_iter()
            .map(|m| MigrationStatus {
                applied: applied.contains(&m.version),
                version: m.version,
                name: m.name,
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct MigrationStatus {
    pub version: i64,
    pub name: String,
    pub applied: bool,
}

/// Detect driver from a URL. Returns Postgres for `postgres://` /
/// `postgresql://` and Mysql for `mysql://` / `mariadb://`.
pub fn detect_driver(url: &str) -> Result<Driver> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        Ok(Driver::Postgres)
    } else if lower.starts_with("mysql://") || lower.starts_with("mariadb://") {
        Ok(Driver::Mysql)
    } else {
        Err(anyhow!("cannot detect driver from URL prefix: {}", url))
    }
}

/// Naive SQL splitter: splits on `;` at top level. Avoids splitting inside
/// single-quoted strings. Sufficient for our migration files which do not
/// contain `;` inside quoted strings.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_single = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_single = !in_single;
                buf.push(c);
            }
            '-' if !in_single => {
                if let Some(&'-') = chars.peek() {
                    // line comment: skip until newline
                    while let Some(&n) = chars.peek() {
                        chars.next();
                        if n == '\n' {
                            break;
                        }
                    }
                    buf.push(' ');
                } else {
                    buf.push(c);
                }
            }
            ';' if !in_single => {
                if !buf.trim().is_empty() {
                    out.push(buf.trim().to_string());
                }
                buf.clear();
            }
            _ => buf.push(c),
        }
    }
    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_filename_up() {
        let m = parse_migration_filename(
            "020_raw_log_index.up.sql",
            Path::new("/tmp/020_raw_log_index.up.sql"),
        )
        .expect("should parse");
        assert_eq!(m.version, 20);
        assert_eq!(m.name, "raw_log_index");
        assert_eq!(m.direction, Direction::Up);
    }

    #[test]
    fn parse_filename_down() {
        let m =
            parse_migration_filename("001_users.down.sql", Path::new("/tmp/001_users.down.sql"))
                .expect("should parse");
        assert_eq!(m.version, 1);
        assert_eq!(m.direction, Direction::Down);
    }

    #[test]
    fn parse_filename_invalid() {
        assert!(parse_migration_filename("README.md", Path::new("/tmp/README.md")).is_none());
    }

    #[test]
    fn detect_driver_works() {
        assert_eq!(
            detect_driver("postgres://u:p@h/d").unwrap(),
            Driver::Postgres
        );
        assert_eq!(
            detect_driver("postgresql://u:p@h/d").unwrap(),
            Driver::Postgres
        );
        assert_eq!(detect_driver("mysql://u:p@h/d").unwrap(), Driver::Mysql);
        assert_eq!(detect_driver("mariadb://u:p@h/d").unwrap(), Driver::Mysql);
        assert!(detect_driver("oracle://u@h/d").is_err());
    }

    #[test]
    fn split_sql_simple() {
        let sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let parts = split_sql_statements(sql);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].starts_with("CREATE TABLE a"));
    }

    #[test]
    fn split_sql_keeps_quoted_semicolons() {
        let sql = "INSERT INTO t (s) VALUES ('a; b'); SELECT 1;";
        let parts = split_sql_statements(sql);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].contains("'a; b'"));
    }

    #[test]
    fn discover_workspace_postgres_migrations() {
        let dir = std::env::current_dir()
            .unwrap()
            .join("crates/cog-storage/migrations");
        if !dir.exists() {
            // Cargo runs tests from the package directory; recover.
            let alt = std::env::current_dir().unwrap().join("migrations");
            if alt.exists() {
                let migrations = discover_up_migrations(&alt, Driver::Postgres).unwrap();
                assert!(!migrations.is_empty());
                return;
            }
        }
        if dir.exists() {
            let migrations = discover_up_migrations(&dir, Driver::Postgres).unwrap();
            assert!(!migrations.is_empty());
        }
    }
}
