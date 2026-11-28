//! `cogneva table-swap <old> <new> <stage>`：换表走到第几段、能不能往下一段走。
//!
//! 判据是刚才量的一次逐行对照，不是日程：只在旧侧、只在新侧的条数都是零才放行；
//! 差一行就停在当前这一段——不往回退，也不往前推。这个命令不改任何一侧的读写方式，
//! 它只回答那道门：给定这一刻的读数，下一段开不开。

use cog_storage::swap_stage::{advance, Comparison, SwapStage};

use crate::table_digest::{checked_table, diff_counts};

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args()
        .skip_while(|arg| arg != "table-swap")
        .skip(1)
        .collect();
    let old = checked_table(
        args.first()
            .ok_or("usage: cogneva table-swap <old> <new> <stage>")?,
    )?;
    let new = checked_table(
        args.get(1)
            .ok_or("usage: cogneva table-swap <old> <new> <stage>")?,
    )?;
    let stage_word = args
        .get(2)
        .ok_or("usage: cogneva table-swap <old> <new> <stage>")?;
    let stage = SwapStage::parse(stage_word).ok_or_else(|| {
        format!(
            "不认识的段 `{stage_word}`（今天有 double-write、read-new、old-read-only 与 dropped）"
        )
    })?;
    let url = std::env::var(cog_core::env::DATABASE_URL)
        .map_err(|_| "set COGNEVA_DATABASE_URL to the database holding both tables")?;
    let pool = sqlx::PgPool::connect(&url).await?;
    let (only_old, only_new) = diff_counts(&pool, &old, &new).await?;
    println!("only_old\t{only_old}");
    println!("only_new\t{only_new}");
    match advance(stage, &Comparison { only_old, only_new }) {
        Ok(next) => {
            println!(
                "stage\t{}\tverdict\tadvance\tnext\t{}",
                stage.as_str(),
                next.as_str()
            );
            Ok(())
        }
        Err(reason) => {
            println!(
                "stage\t{}\tverdict\tstop\t{}\t{reason}",
                stage.as_str(),
                stage.as_str()
            );
            Err(reason.into())
        }
    }
}
