//! BGE-M3 的稀疏一半，从**同一个**会话的隐状态里算出来。
//!
//! 稀疏向量不是第二张模型算的：同一趟前向得到的 token 隐状态，逐 token 乘上
//! [`crate::bge_m3_sparse_linear`] 那 1024 个数、加偏置、取 ReLU、按 token 取最大，
//! 就是稀疏权重。所以「dense 一个会话、sparse 一个会话」这件事本来不必付：
//! 两个会话读的是同一批权重文件，各把权重读进自己的缓冲区，页不共享。
//!
//! 这里承担的是后一半：自己分词拿到 token id，再把会话吐出来的隐状态变成稀疏向量。
//! 分词必须与真跑前向的那一侧**逐位一致**，否则 token id 与位置对不上、算出来的
//! 稀疏向量看着正常却是错的。做法是照抄同一套设置：同一个 `tokenizer.json`、
//! 同一份 `tokenizer_config.json` 里的 `model_max_length`、同一份 `config.json` 里的
//! `pad_token_id`，`BatchLongest` 补齐、截断到 512，再把 `special_tokens_map.json`
//! 里的特殊 token 逐个登记。批的分法也必须一样（每 [`BATCH`] 条一批），因为
//! `BatchLongest` 决定了这一批补到多长。
//!
//! 对不上时不猜：隐状态的实际长度与本侧分出来的长度不等就直接报错，宁可这一次
//! 稀疏向量缺席，也不写一条静默错位的。

use std::collections::HashMap;
use std::path::Path;

use cog_core::SparseEmbedding;
use tokenizers::{AddedToken, PaddingParams, PaddingStrategy, TruncationParams};

use crate::bge_m3_sparse_linear::{BIAS, WEIGHT};

/// 一批多少条。与建立会话那一侧用的分批大小一致——`BatchLongest` 补齐意味着
/// 换一个批大小，同一句话就可能补到另一个长度。
// cap: fixed — 照抄 fastembed 建立会话那一侧的分批大小；两侧取的是同一个数，那边改了这里要一起看
pub(crate) const BATCH: usize = 256;

/// 权重目录里指向「当前这一版」的那份引用文件名。
///
/// 会话那一侧按同一套缓存布局找权重，读的就是这一个；这里抄成常量而不是就地再写一遍，
/// 是因为两处写成同一段字面量时，改了一处不会让另一处红。
const CURRENT_REF: &str = "refs/main";

/// 截断长度上限。同样取建立会话那一侧的默认值。
// cap: fixed — 与建立会话那一侧取同一个截断长度；两侧必须一致，那边改了这里要一起看
const MAX_LENGTH: usize = 512;

/// BGE-M3 词表里 `<s>`、`<pad>`、`</s>`、`<unk>` 的编号，稀疏权重不计它们。
const SPECIAL_TOKENS: [i64; 4] = [0, 1, 2, 3];

/// 分会词、也会把隐状态变成稀疏向量的那一半。
pub(crate) struct SparseHead {
    tokenizer: tokenizers::Tokenizer,
}

impl SparseHead {
    /// 从权重目录里装分词器，与建立会话那一侧读的是同一批文件。
    ///
    /// 失败是一个**可报的降级**，不是致命错误：调用方据此把这一半标成缺席并给出
    /// 原因，dense 那一半照常工作。
    pub(crate) fn load(cache_dir: Option<&Path>) -> Result<Self, String> {
        let dir = snapshot_dir(cache_dir)?;
        let tokenizer = load_tokenizer(&dir)?;
        Ok(Self { tokenizer })
    }

    /// 把一批文本分成 `(ids, mask, 行数, 每行长度)`，排布与真跑前向那一侧相同。
    pub(crate) fn tokenize(&self, texts: &[&str]) -> Result<Encoded, String> {
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| format!("分词失败：{e}"))?;
        let seq = encodings
            .first()
            .ok_or_else(|| "分词器没有给出任何一行".to_string())?
            .len();
        let rows = encodings.len();

        let mut ids = Vec::with_capacity(rows * seq);
        let mut mask = Vec::with_capacity(rows * seq);
        for encoding in &encodings {
            // 补齐开着，所以每一行都该是同一个长度；不是就说明设置与建立会话那一侧
            // 已经分叉，此时拼成方阵只会得到错位的结果。
            if encoding.len() != seq {
                return Err(format!(
                    "这一批分出来的行长不一致（{} 与 {}），补齐设置与建立会话那一侧不同",
                    encoding.len(),
                    seq
                ));
            }
            ids.extend(encoding.get_ids().iter().map(|&x| x as i64));
            mask.extend(encoding.get_attention_mask().iter().map(|&x| x as i64));
        }
        Ok(Encoded {
            ids,
            mask,
            rows,
            seq,
        })
    }

    /// 把这一批的隐状态变成稀疏向量，一行一条。
    ///
    /// `hidden` 是会话吐出来的那一块，形状必须正好是 `rows × seq × WEIGHT.len()`；
    /// 三个数里任何一个对不上都报错，不按「大概能凑」去乘。
    pub(crate) fn sparse_from_hidden(
        &self,
        encoded: &Encoded,
        shape: &[usize],
        hidden: &[f32],
    ) -> Result<Vec<SparseEmbedding>, String> {
        let expect = [encoded.rows, encoded.seq, WEIGHT.len()];
        if shape != expect {
            return Err(format!(
                "隐状态形状 {shape:?} 与本侧分出来的 {expect:?} 对不上；\
                 对不上就不知道哪一行是哪一句，不往下算"
            ));
        }

        let dim = WEIGHT.len();
        let mut out = Vec::with_capacity(encoded.rows);
        for row in 0..encoded.rows {
            // 同一个 token 可能出现多次，取其中最大的权重——这是这个稀疏头的定义。
            let mut best: HashMap<u32, f32> = HashMap::new();
            for at in 0..encoded.seq {
                let flat = row * encoded.seq + at;
                if encoded.mask[flat] == 0 {
                    continue;
                }
                let token = encoded.ids[flat];
                if SPECIAL_TOKENS.contains(&token) {
                    continue;
                }
                let base = flat * dim;
                let mut weight = BIAS;
                for (d, w) in WEIGHT.iter().enumerate() {
                    weight += hidden[base + d] * w;
                }
                if weight > 0.0 {
                    best.entry(token as u32)
                        .and_modify(|w| {
                            if weight > *w {
                                *w = weight;
                            }
                        })
                        .or_insert(weight);
                }
            }
            let mut indices: Vec<u32> = best.keys().copied().collect();
            indices.sort_unstable();
            let values: Vec<f32> = indices.iter().map(|i| best[i]).collect();
            out.push(SparseEmbedding { indices, values });
        }
        Ok(out)
    }
}

/// 一节课的排布：ids 与 mask 按行优先拼成一条，外加行列数。
pub(crate) struct Encoded {
    pub(crate) ids: Vec<i64>,
    pub(crate) mask: Vec<i64>,
    pub(crate) rows: usize,
    pub(crate) seq: usize,
}

/// 权重目录里那一版快照的路径。
///
/// 会话那一侧按 HuggingFace 的缓存布局找权重（`models--<org>--<name>/snapshots/<rev>`），
/// 这里跟同一套布局，读 `refs/main` 拿到当前那一版。
fn snapshot_dir(cache_dir: Option<&Path>) -> Result<std::path::PathBuf, String> {
    let root = match cache_dir {
        Some(dir) => dir.to_path_buf(),
        // 会话那一侧没被指名的目录时也是这样算出来的：环境变量优先，否则默认路径。
        None => std::path::PathBuf::from(fastembed::get_cache_dir()),
    };
    let repo = root.join("models--BAAI--bge-m3");
    let rev_path = repo.join(CURRENT_REF);
    let rev = std::fs::read_to_string(&rev_path)
        .map_err(|e| format!("读不出 {}：{e}", rev_path.display()))?;
    let dir = repo.join("snapshots").join(rev.trim());
    if !dir.is_dir() {
        return Err(format!("权重快照不在：{}", dir.display()));
    }
    Ok(dir)
}

/// 照建立会话那一侧的设置装分词器。
///
/// 少一样都会让同一句话分出不同的 id，进而让隐状态与 token 错位——而这种错位不会
/// 崩，只会让召回悄悄变差，所以这里的每一项都不是可有可无的。
fn load_tokenizer(dir: &Path) -> Result<tokenizers::Tokenizer, String> {
    let read = |name: &str| -> Result<Vec<u8>, String> {
        let path = dir.join(name);
        std::fs::read(&path).map_err(|e| format!("读不出 {}：{e}", path.display()))
    };
    let parse = |name: &str| -> Result<serde_json::Value, String> {
        serde_json::from_slice(&read(name)?).map_err(|e| format!("{name} 不是 JSON：{e}"))
    };

    let tokenizer_file = read("tokenizer.json")?;
    let config = parse("config.json")?;
    let tokenizer_config = parse("tokenizer_config.json")?;
    let special_tokens_map = parse("special_tokens_map.json")?;

    let mut tokenizer = tokenizers::Tokenizer::from_bytes(tokenizer_file)
        .map_err(|e| format!("tokenizer.json 装不起来：{e}"))?;

    // 模型的上下文窗口：BGE-M3 是 8192，比这里的上限宽，所以实际截断到上限。
    let model_max_length = tokenizer_config["model_max_length"]
        .as_f64()
        .unwrap_or(MAX_LENGTH as f64);
    let max_length = MAX_LENGTH.min(model_max_length as usize);

    tokenizer.with_padding(Some(PaddingParams {
        strategy: PaddingStrategy::BatchLongest,
        // 缺这两项时用 BGE-M3 的补齐符；补出来的位置不参与稀疏权重，但长度要一样。
        pad_token: tokenizer_config["pad_token"]
            .as_str()
            .unwrap_or("<pad>")
            .to_string(),
        pad_id: config["pad_token_id"].as_u64().unwrap_or(1) as u32,
        ..Default::default()
    }));
    tokenizer
        .with_truncation(Some(TruncationParams {
            max_length,
            ..Default::default()
        }))
        .map_err(|e| format!("截断设置装不上：{e}"))?;

    if let serde_json::Value::Object(map) = special_tokens_map {
        for value in map.values() {
            let token = match value {
                serde_json::Value::String(content) => Some(AddedToken {
                    content: content.clone(),
                    special: true,
                    ..Default::default()
                }),
                serde_json::Value::Object(inner) => {
                    inner["content"].as_str().map(|content| AddedToken {
                        content: content.to_string(),
                        special: true,
                        single_word: inner["single_word"].as_bool().unwrap_or(false),
                        lstrip: inner["lstrip"].as_bool().unwrap_or(false),
                        rstrip: inner["rstrip"].as_bool().unwrap_or(false),
                        normalized: inner["normalized"].as_bool().unwrap_or(false),
                    })
                }
                _ => None,
            };
            if let Some(token) = token {
                tokenizer.add_special_tokens(&[token]);
            }
        }
    }

    Ok(tokenizer)
}
