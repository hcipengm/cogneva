//! Password hashing utilities using Argon2.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

use super::error::{AuthError, AuthResult};

/// Hash a plaintext password using Argon2id.
pub fn hash_password(password: &str) -> AuthResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| AuthError::HashError(e.to_string()))?;
    Ok(password_hash.to_string())
}

/// 定长比较：引导管理员口令是**明文注入的环境变量**（没有哈希可校验），
/// 用 `==` 比会让逐字节提前返回暴露出前缀。长度不同也照样走完整轮。
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() ^ b.len()) as u8;
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

/// Verify a plaintext password against an Argon2 hash string.
pub fn verify_password(password: &str, hash: &str) -> AuthResult<bool> {
    let parsed_hash = PasswordHash::new(hash).map_err(|e| AuthError::HashError(e.to_string()))?;
    let argon2 = Argon2::default();
    match argon2.verify_password(password.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(e) => Err(AuthError::HashError(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_password_ok() {
        let hash = hash_password("my_secret_password").unwrap();
        assert!(verify_password("my_secret_password", &hash).unwrap());
    }

    #[test]
    fn constant_time_compare_matches_only_identical_bytes() {
        assert!(constant_time_eq(b"boot-strap-pass", b"boot-strap-pass"));
        assert!(!constant_time_eq(b"boot-strap-pass", b"boot-strap-pas"));
        assert!(!constant_time_eq(b"boot-strap-pass", b"boot-strap-past"));
        // 长度不同也要走完整轮：前缀相同不算相等。
        assert!(!constant_time_eq(b"boot", b"boot-strap-pass"));
        assert!(!constant_time_eq(b"", b"boot-strap-pass"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn verify_wrong_password_fails() {
        let hash = hash_password("correct_password").unwrap();
        assert!(!verify_password("wrong_password", &hash).unwrap());
    }
}
