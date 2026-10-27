//! principle: feedback_no_hardcoding_config_surface
//!
//! Two `AuthError` enums exist: the one in the auth crate, used by the session
//! store the auth plugin publishes, and the one the gateway answers requests
//! with. They are the same sixteen variants with the same sixteen error codes,
//! written twice by hand, and nothing compares them.
//!
//! A client switches on the `error` word. Let the two tables drift — rename a
//! code on one side, add a variant to the other — and the gateway starts
//! answering with a word that no consumer of the auth crate recognises. Nothing
//! fails: the request is rejected correctly, under a name that means nothing to
//! the caller, which reads exactly like a rejection for some other reason.
//!
//! So both tables are read here, in the one crate that depends on both.

use cog_auth::AuthError as SessionAuthError;
use cog_gateway::auth::AuthError as GatewayAuthError;

/// 编译期闩：两个 `AuthError` 各新增一个变体，这个 `match` 就不再穷尽，
/// 于是下面两套表必须在同一笔里一起动。
fn gateway_cell(error: &GatewayAuthError) -> (&'static str, u16) {
    match error {
        GatewayAuthError::InvalidToken(_)
        | GatewayAuthError::TokenExpired
        | GatewayAuthError::SessionNotFound(_)
        | GatewayAuthError::UserNotFound(_)
        | GatewayAuthError::PermissionDenied(_)
        | GatewayAuthError::DatabaseError(_)
        | GatewayAuthError::HashError(_)
        | GatewayAuthError::MissingAuthHeader
        | GatewayAuthError::InvalidAuthFormat
        | GatewayAuthError::TokenGenerationFailed(_)
        | GatewayAuthError::RedisError(_)
        | GatewayAuthError::InvalidCredentials
        | GatewayAuthError::UserAlreadyExists(_)
        | GatewayAuthError::AccountDisabled
        | GatewayAuthError::AccountLocked
        | GatewayAuthError::Internal(_) => (error.error_code(), error.status_code().as_u16()),
    }
}

fn session_cell(error: &SessionAuthError) -> (&'static str, u16) {
    match error {
        SessionAuthError::InvalidToken(_)
        | SessionAuthError::TokenExpired
        | SessionAuthError::SessionNotFound(_)
        | SessionAuthError::UserNotFound(_)
        | SessionAuthError::PermissionDenied(_)
        | SessionAuthError::DatabaseError(_)
        | SessionAuthError::HashError(_)
        | SessionAuthError::MissingAuthHeader
        | SessionAuthError::InvalidAuthFormat
        | SessionAuthError::TokenGenerationFailed(_)
        | SessionAuthError::RedisError(_)
        | SessionAuthError::InvalidCredentials
        | SessionAuthError::UserAlreadyExists(_)
        | SessionAuthError::AccountDisabled
        | SessionAuthError::AccountLocked
        | SessionAuthError::Internal(_) => (error.error_code(), error.status_code().as_u16()),
    }
}

fn gateway_cells() -> Vec<(&'static str, u16)> {
    let all = [
        GatewayAuthError::InvalidToken(String::new()),
        GatewayAuthError::TokenExpired,
        GatewayAuthError::SessionNotFound(String::new()),
        GatewayAuthError::UserNotFound(String::new()),
        GatewayAuthError::PermissionDenied(String::new()),
        GatewayAuthError::DatabaseError(String::new()),
        GatewayAuthError::HashError(String::new()),
        GatewayAuthError::MissingAuthHeader,
        GatewayAuthError::InvalidAuthFormat,
        GatewayAuthError::TokenGenerationFailed(String::new()),
        GatewayAuthError::RedisError(String::new()),
        GatewayAuthError::InvalidCredentials,
        GatewayAuthError::UserAlreadyExists(String::new()),
        GatewayAuthError::AccountDisabled,
        GatewayAuthError::AccountLocked,
        GatewayAuthError::Internal(String::new()),
    ];
    all.iter().map(gateway_cell).collect()
}

fn session_cells() -> Vec<(&'static str, u16)> {
    let all = [
        SessionAuthError::InvalidToken(String::new()),
        SessionAuthError::TokenExpired,
        SessionAuthError::SessionNotFound(String::new()),
        SessionAuthError::UserNotFound(String::new()),
        SessionAuthError::PermissionDenied(String::new()),
        SessionAuthError::DatabaseError(String::new()),
        SessionAuthError::HashError(String::new()),
        SessionAuthError::MissingAuthHeader,
        SessionAuthError::InvalidAuthFormat,
        SessionAuthError::TokenGenerationFailed(String::new()),
        SessionAuthError::RedisError(String::new()),
        SessionAuthError::InvalidCredentials,
        SessionAuthError::UserAlreadyExists(String::new()),
        SessionAuthError::AccountDisabled,
        SessionAuthError::AccountLocked,
        SessionAuthError::Internal(String::new()),
    ];
    all.iter().map(session_cell).collect()
}

fn sorted(mut cells: Vec<(&'static str, u16)>) -> Vec<(&'static str, u16)> {
    cells.sort_unstable();
    cells
}

#[test]
fn both_auth_errors_answer_with_the_same_word_and_status() {
    let gateway = sorted(gateway_cells());
    let session = sorted(session_cells());

    let missing_from_gateway: Vec<_> = session.iter().filter(|c| !gateway.contains(c)).collect();
    let missing_from_session: Vec<_> = gateway.iter().filter(|c| !session.contains(c)).collect();

    assert!(
        missing_from_gateway.is_empty() && missing_from_session.is_empty(),
        "两个 AuthError 的码与状态已经对不上了——客户端按 `error` 这个词分支，\
         网关答一个它不认识的词，与「因为别的原因被拒」读起来一模一样。\n\
         网关缺：{missing_from_gateway:?}\n\
         会话侧缺：{missing_from_session:?}"
    );
}

/// 同一个枚举里两个变体共用一个码，客户端就分不开它们。
#[test]
fn no_two_variants_share_a_code() {
    for (side, cells) in [("gateway", gateway_cells()), ("session", session_cells())] {
        let mut codes: Vec<&'static str> = cells.iter().map(|(code, _)| *code).collect();
        let total = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(
            codes.len(),
            total,
            "{side} 侧有两个变体答同一个码，调用方分不开它们"
        );
    }
}
