use clap::Parser;
use std::path::PathBuf;
use tracing::info;

use cog_storage::migrate::Migrator;

#[derive(Parser)]
#[command(name = "cog-migrate")]
#[command(about = "Cogneva database migration tool")]
struct Args {
    /// Connection URL. Driver is inferred from the prefix.
    #[arg(short, long, default_value = "mysql://root:rootpass@localhost/cogneva")]
    database_url: String,

    /// Override-driver knob (defaults to URL detection).
    #[arg(short = 'D', long)]
    driver: Option<String>,

    /// Migrations root directory.
    #[arg(long, default_value = "crates/cog-db/migrations")]
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
    info!(
        "cog-migrate starting (db={} migrations_dir={} dry_run={} status={})",
        redact_url(&args.database_url),
        args.migrations_dir.display(),
        args.dry_run,
        args.status,
    );

    let migrator = Migrator::new(args.migrations_dir);

    if args.status {
        let entries = migrator.status(&args.database_url).await?;
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
        migrator.run_dry(&args.database_url).await?;
    } else {
        migrator.run(&args.database_url).await?;
    }

    info!("migrations complete");
    Ok(())
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
