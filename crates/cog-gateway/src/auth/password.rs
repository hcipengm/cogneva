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
    fn verify_wrong_password_fails() {
        let hash = hash_password("correct_password").unwrap();
        assert!(!verify_password("wrong_password", &hash).unwrap());
    }
}
