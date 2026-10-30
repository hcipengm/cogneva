use clap::Parser;
use std::path::PathBuf;
use tracing::info;

use cog_storage::migrate::Migrator;

#[derive(Parser)]
#[command(name = "cog-migrate")]
#[command(about = "Cogneva database migration tool")]
struct Args {
    /// Connection URL. Driver is inferred from the prefix.
    ///
    /// 没有默认值：连接串是部署事实（哪个库、哪台机器、哪份凭证），写进二进制
    /// 就跟着部署走不动，而默认一台本机的库在别的机器上只会连不上、或者更糟，
    /// 连上一台碰巧叫这个名字的库。没给就退到部署自己注入的那条
    /// （`COGNEVA_DATABASE_URL`），再没有就报错。
    #[arg(short, long)]
    database_url: Option<String>,

    /// Override-driver knob (defaults to URL detection).
    #[arg(short = 'D', long)]
    driver: Option<String>,

    /// Migrations root directory.
    #[arg(long, default_value = "crates/cog-storage/migrations")]
    migrations_dir: PathBuf,

    /// Print what would be applied without touching the database.
    #[arg(long)]
    dry_run: bool,

    /// Print applied vs pending status without changing anything.
    #[arg(long)]
    status: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let database_url = resolve_database_url(
        args.database_url.as_deref(),
        std::env::var(cog_core::env::DATABASE_URL).ok().as_deref(),
    )?;
    info!(
        "cog-migrate starting (db={} migrations_dir={} dry_run={} status={})",
        redact_url(&database_url),
        args.migrations_dir.display(),
        args.dry_run,
        args.status,
    );

    let migrator = Migrator::new(args.migrations_dir);

    if args.status {
        let entries = migrator.status(&database_url).await?;
        for s in entries {
            println!(
                "{} {:>3} {}",
                if s.applied { "[OK]    " } else { "[PENDING]" },
                s.version,
                s.name,
            );
        }
        return Ok(());
    }

    if args.dry_run {
        migrator.run_dry(&database_url).await?;
    } else {
        migrator.run(&database_url).await?;
    }

    info!("migrations complete");
    Ok(())
}

/// 连接串的来处：先看命令行，再看部署注入的那条；两处都没有就停。
///
/// 停而不是补一个默认值，理由与「镜像仓库不给默认值」同一条：猜一个库地址，
/// 猜对是本机、猜错是一次对着别人的库改表结构。空串按没有处理——`--database-url ""`
/// 不是「连默认库」，是一个写错了的参数。
fn resolve_database_url(from_flag: Option<&str>, from_env: Option<&str>) -> anyhow::Result<String> {
    from_flag
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .or_else(|| from_env.map(str::trim).filter(|v| !v.is_empty()))
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "没有连接串：用 --database-url 给，或让部署把它注入到 {}",
                cog_core::env::DATABASE_URL
            )
        })
}

/// Redact the password segment of a URL for logging.
fn redact_url(url: &str) -> String {
    if let Some(at) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let user_pass_start = scheme_end + 3;
            return format!("{}{}", &url[..user_pass_start], "***@") + &url[at + 1..];
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 连接串只从两个声明处来，猜不得；两个都没有时是一次响亮的失败。
    #[test]
    fn the_connection_string_is_declared_never_guessed() {
        assert_eq!(
            resolve_database_url(Some("mysql://u:p@h/db"), Some("mysql://other")).unwrap(),
            "mysql://u:p@h/db"
        );
        assert_eq!(
            resolve_database_url(None, Some("mysql://from-env")).unwrap(),
            "mysql://from-env"
        );
        assert_eq!(
            resolve_database_url(Some("  "), Some("mysql://from-env")).unwrap(),
            "mysql://from-env",
            "空串是一个写错的参数，不是一个地址"
        );
        let err = resolve_database_url(None, None).unwrap_err();
        assert!(
            err.to_string().contains(cog_core::env::DATABASE_URL),
            "拒绝要说清去哪儿声明它：{err}"
        );
    }
}
