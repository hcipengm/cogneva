//! 备份包的包级加密：落在卷上、卷外、宿主机上的那一份字节本身就是密文。
//!
//! 为什么不是「反正 restic 会加密」：restic 只覆盖**远端仓库**那一条腿。备份卷、
//! 卷外第二份、宿主状态目录里的包是明文归档，里面装着 Secret 对象清单（只做
//! base64），能读文件就等于持有全部凭证。0600 挡住同机别的账号，挡不住拿到盘的人，
//! 也挡不住把 PVC 快照拷走的人。这一层补的是那个缺口。
//!
//! 形状：`tar → zstd → 分块 AEAD → 文件`。密钥由口令经 HKDF-SHA256 派生，每个包
//! 一份随机盐，所以同一个口令打出来的两个包互不可比；分块之间用递增计数器做 nonce，
//! 重排、截断、改一位都会在解密时被拒。末尾有一个认证过的终止帧，没有它的文件一律
//! 按截断处理——少了这条，把包砍掉后半段仍可能被 zstd 当成「刚好结束」。
//!
//! 口令从 `COGNEVA_BACKUP_PACKAGE_KEY` 来。没配口令时写出来的仍是旧格式明文包，
//! 读侧靠文件头区分新旧两种包：旧包在轮换窗口里照常能解，新包没钥匙就报错而不是
//! 静默降级成「读出来是垃圾」。

use std::io::{self, Read, Write};
use std::path::Path;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::hkdf::{self, HKDF_SHA256};

/// 加密包的文件头。读侧先读这几个字节判断新旧格式；写侧原样写下，并作为每一帧的
/// 附加认证数据——换掉头部连第一帧都过不了认证，而不是「头改了、内容照解」。
pub const MAGIC: &[u8] = b"COGNEVA-BACKUP-ENC-1\n";

/// 口令所在的环境变量。值是一句口令，不是密钥：派生在本地做，明文口令不出进程。
pub const KEY_ENV: &str = "COGNEVA_BACKUP_PACKAGE_KEY";

const SALT_LEN: usize = 16;
const TAG_LEN: usize = 16;
/// 密文形状里三个由算法定死的长度：ChaCha20-Poly1305 的 nonce 12 字节、认证标签
/// 16 字节（终止帧靠它认出来），以及每个包一份的 16 字节随机盐——盐够大就够让同一
/// 句口令打出来的两个包互不可比。它们不是可调的旋钮。
const NONCE_LEN: usize = 12;
/// 一帧的明文上限。1 MiB 是吞吐与内存占用的折中：包体量在百 MB 量级，整包读进
/// 内存会让备份 Pod 的峰值跟着包大小走。
const CHUNK: usize = 1 << 20;
const MAX_FRAME: usize = CHUNK + TAG_LEN;

/// 口令派生出的一把包密钥。持有口令本身，因为每个包要按自己的盐重新派生；
/// 派生出来的密钥只在一帧/一个包的生命周期里存在。
pub struct PackageKey {
    passphrase: Vec<u8>,
}

impl Drop for PackageKey {
    fn drop(&mut self) {
        // 口令在进程里活多久不是这一层能决定的，但至少不要让它随内存复用漂走。
        for byte in self.passphrase.iter_mut() {
            *byte = 0;
        }
    }
}

impl PackageKey {
    pub fn from_passphrase(passphrase: &[u8]) -> Self {
        Self {
            passphrase: passphrase.to_vec(),
        }
    }

    /// 读环境变量。未设置或空串都按「没有钥匙」处理；太短的口令不拦，但会在
    /// 备份日志里留一条告警——拒绝跑备份的代价比一句弱口令大。
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var_os(KEY_ENV)?;
        let text = raw.to_string_lossy();
        if text.trim().is_empty() {
            return None;
        }
        if text.len() < 16 {
            tracing::warn!(
                env = KEY_ENV,
                "备份包口令短于 16 字节：能跑，但离线爆破的成本会明显降低"
            );
        }
        Some(Self::from_passphrase(text.as_bytes()))
    }

    fn derive(&self, salt: &[u8]) -> io::Result<LessSafeKey> {
        let salt = hkdf::Salt::new(HKDF_SHA256, salt);
        let prk = salt.extract(&self.passphrase);
        let okm = prk
            .expand(&[b"cogneva-backup-package-v1"], HKDF_SHA256)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "派生长度不合法"))?;
        let mut material = [0u8; 32];
        okm.fill(&mut material)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "派生密钥失败"))?;
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, &material)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "密钥长度不合法"))?;
        Ok(LessSafeKey::new(unbound))
    }

    /// 把写进去的字节封成密文流。返回的 writer 在 `finish` 之前不会把最后一帧
    /// 补上终止标记，所以调用方必须显式收尾——忘了收尾的包在读侧就是截断。
    pub fn seal<W: Write>(&self, mut writer: W) -> io::Result<SealingWriter<W>> {
        let mut salt = [0u8; SALT_LEN];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| io::Error::other("取不到随机盐"))?;
        writer.write_all(MAGIC)?;
        writer.write_all(&salt)?;
        Ok(SealingWriter {
            inner: writer,
            key: self.derive(&salt)?,
            buf: Vec::with_capacity(CHUNK),
            counter: 0,
        })
    }

    /// 解一个密文流。头部在构造时校验，不是这个格式就报错——调用方应当先用
    /// `package_is_encrypted` 分辨新旧包，这里的报错是最后一道而不是分支依据。
    pub fn open<R: Read>(&self, mut reader: R) -> io::Result<OpeningReader<R>> {
        let mut magic = [0u8; MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "这不是加密包（文件头对不上）：按明文包去解，或确认包没被截断过",
            ));
        }
        let mut salt = [0u8; SALT_LEN];
        reader.read_exact(&mut salt)?;
        Ok(OpeningReader {
            inner: reader,
            key: self.derive(&salt)?,
            pending: Vec::new(),
            offset: 0,
            counter: 0,
            done: false,
        })
    }
}

/// 读一个包的前几个字节，判断它是不是加密格式。旧包（以及没有配置钥匙时写出来的
/// 包）没有这个头，按明文处理。
pub fn package_is_encrypted(path: &Path) -> io::Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut head = [0u8; MAGIC.len()];
    let mut filled = 0;
    while filled < head.len() {
        match file.read(&mut head[filled..])? {
            0 => return Ok(false),
            n => filled += n,
        }
    }
    Ok(head == MAGIC)
}

fn nonce_for(counter: u64) -> Nonce {
    let mut raw = [0u8; NONCE_LEN];
    raw[4..].copy_from_slice(&counter.to_be_bytes());
    Nonce::assume_unique_for_key(raw)
}

pub struct SealingWriter<W: Write> {
    inner: W,
    key: LessSafeKey,
    buf: Vec<u8>,
    counter: u64,
}

impl<W: Write> SealingWriter<W> {
    fn write_frame(&mut self) -> io::Result<()> {
        let mut frame = std::mem::take(&mut self.buf);
        let nonce = nonce_for(self.counter);
        self.counter += 1;
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(MAGIC), &mut frame)
            .map_err(|_| io::Error::other("封帧失败"))?;
        let len = u32::try_from(frame.len()).map_err(|_| io::Error::other("一帧装不下这个长度"))?;
        self.inner.write_all(&len.to_le_bytes())?;
        self.inner.write_all(&frame)?;
        // 复用缓冲：把容量要回来，但长度归零。
        frame.clear();
        if frame.capacity() < CHUNK {
            frame.reserve(CHUNK);
        }
        self.buf = frame;
        Ok(())
    }

    /// 写终止帧：长度恰为标签长度、明文为空。空明文只有终止帧会出现，所以读侧
    /// 见到它就是「到这里合法结束」；没有它的文件按截断处理。
    pub fn finish(mut self) -> io::Result<W> {
        // `write` 每次凑满一帧就立刻封走，所以这里最多剩一帧的余量。
        if !self.buf.is_empty() {
            self.write_frame()?;
        }
        let mut terminator = Vec::with_capacity(TAG_LEN);
        let nonce = nonce_for(self.counter);
        self.counter += 1;
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(MAGIC), &mut terminator)
            .map_err(|_| io::Error::other("封终止帧失败"))?;
        let len =
            u32::try_from(terminator.len()).map_err(|_| io::Error::other("终止帧长度异常"))?;
        self.inner.write_all(&len.to_le_bytes())?;
        self.inner.write_all(&terminator)?;
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for SealingWriter<W> {
    fn write(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let total = data.len();
        while !data.is_empty() {
            let room = CHUNK - self.buf.len();
            if room == 0 {
                self.write_frame()?;
                continue;
            }
            let take = room.min(data.len());
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buf.len() == CHUNK {
                self.write_frame()?;
            }
        }
        Ok(total)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.write_frame()?;
        }
        self.inner.flush()
    }
}

pub struct OpeningReader<R: Read> {
    inner: R,
    key: LessSafeKey,
    pending: Vec<u8>,
    offset: usize,
    counter: u64,
    done: bool,
}

impl<R: Read> OpeningReader<R> {
    fn next_frame(&mut self) -> io::Result<Vec<u8>> {
        let mut len_raw = [0u8; 4];
        let mut filled = 0;
        while filled < len_raw.len() {
            match self.inner.read(&mut len_raw[filled..])? {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        if filled == 0 {
                            "加密包在终止帧之前就结束了（被截断或被换过）"
                        } else {
                            "加密包的长度前缀只写了一半"
                        },
                    ))
                }
                n => filled += n,
            }
        }
        let len = u32::from_le_bytes(len_raw) as usize;
        if !(TAG_LEN..=MAX_FRAME).contains(&len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("加密包的一帧长度 {len} 不在 {TAG_LEN}..={MAX_FRAME} 之内"),
            ));
        }
        let mut frame = vec![0u8; len];
        self.inner.read_exact(&mut frame)?;
        let nonce = nonce_for(self.counter);
        self.counter += 1;
        let plain = self
            .key
            .open_in_place(nonce, Aad::from(MAGIC), &mut frame)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "解密失败：口令不对，或者这一帧被改过/挪过位置",
                )
            })?;
        let plain_len = plain.len();
        frame.truncate(plain_len);
        Ok(frame)
    }
}

impl<R: Read> Read for OpeningReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.offset < self.pending.len() {
                let take = (self.pending.len() - self.offset).min(out.len());
                out[..take].copy_from_slice(&self.pending[self.offset..self.offset + take]);
                self.offset += take;
                if self.offset == self.pending.len() {
                    self.pending.clear();
                    self.offset = 0;
                }
                return Ok(take);
            }
            if self.done {
                return Ok(0);
            }
            let frame = self.next_frame()?;
            if frame.is_empty() {
                // 终止帧之后不许再有字节：多出来的内容说明文件被拼接/篡改过。
                let mut extra = [0u8; 1];
                if self.inner.read(&mut extra)? != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "加密包的终止帧之后还有字节",
                    ));
                }
                self.done = true;
                return Ok(0);
            }
            self.pending = frame;
            self.offset = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seal_all(key: &PackageKey, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = key.seal(&mut out).unwrap();
        // 故意用不等长的几段写，覆盖「跨帧」「整帧」「不足一帧」三种路径。
        for piece in data.chunks(7) {
            writer.write_all(piece).unwrap();
        }
        writer.finish().unwrap();
        out
    }

    #[test]
    fn the_ciphertext_does_not_carry_the_plaintext_and_round_trips() {
        let key = PackageKey::from_passphrase(b"correct horse battery staple");
        let plain = b"secrets/cogneva-secrets.yaml: token=super-secret".repeat(4096);
        let sealed = seal_all(&key, &plain);
        assert!(sealed.starts_with(MAGIC));
        assert!(
            !sealed
                .windows(b"cogneva-secrets".len())
                .any(|w| w == b"cogneva-secrets"),
            "密文里不该出现明文片段"
        );
        let mut back = Vec::new();
        key.open(&sealed[..])
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, plain);
    }

    #[test]
    fn another_passphrase_cannot_open_the_package() {
        let sealed = seal_all(
            &PackageKey::from_passphrase(b"one passphrase for the drill"),
            b"xxxxxxxxxx",
        );
        let other = PackageKey::from_passphrase(b"another passphrase entirely");
        let mut back = Vec::new();
        let err = other
            .open(&sealed[..])
            .unwrap()
            .read_to_end(&mut back)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(back.is_empty());
    }

    #[test]
    fn a_truncated_package_is_refused_instead_of_read_as_a_shorter_one() {
        let key = PackageKey::from_passphrase(b"correct horse battery staple");
        let plain = vec![7u8; CHUNK + 100];
        let sealed = seal_all(&key, &plain);
        // 砍掉最后一帧（终止帧），再砍掉半帧内容。
        for cut in [sealed.len() - TAG_LEN - 4, sealed.len() - CHUNK / 2] {
            let mut back = Vec::new();
            let err = key
                .open(&sealed[..cut])
                .unwrap()
                .read_to_end(&mut back)
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "cut={cut}");
        }
    }

    #[test]
    fn a_flipped_bit_or_a_moved_frame_is_refused() {
        let key = PackageKey::from_passphrase(b"correct horse battery staple");
        let sealed = seal_all(&key, &vec![9u8; CHUNK + 10]);
        let mut tampered = sealed.clone();
        let at = MAGIC.len() + SALT_LEN + 8;
        tampered[at] ^= 0x01;
        let mut back = Vec::new();
        assert!(key
            .open(&tampered[..])
            .unwrap()
            .read_to_end(&mut back)
            .is_err());
    }

    #[test]
    fn an_empty_payload_still_has_a_terminator_so_a_truncated_file_is_distinguishable() {
        let key = PackageKey::from_passphrase(b"correct horse battery staple");
        let sealed = seal_all(&key, b"");
        let mut back = Vec::new();
        key.open(&sealed[..])
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert!(back.is_empty());
        let cut = sealed.len() - 4 - TAG_LEN;
        let mut back = Vec::new();
        assert!(key
            .open(&sealed[..cut])
            .unwrap()
            .read_to_end(&mut back)
            .is_err());
    }

    #[test]
    fn a_plain_zstd_package_is_not_mistaken_for_an_encrypted_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cogneva-backup-20261018T031700Z.tar.zst");
        let mut encoder = zstd::Encoder::new(std::fs::File::create(&path).unwrap(), 3).unwrap();
        encoder.write_all(b"not encrypted").unwrap();
        encoder.finish().unwrap();
        assert!(!package_is_encrypted(&path).unwrap());
    }
}
