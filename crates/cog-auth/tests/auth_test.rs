use chrono::Utc;
use cog_auth::{
    jwt::{JwtConfig, JwtManager},
    RoleChecker, SessionManager,
};
use cog_core::{Permission, Role, SessionInfo, User, UserStatus, UserType};
use std::time::Duration;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// JWT tests
// ---------------------------------------------------------------------------

fn test_user() -> User {
    User {
        id: Uuid::new_v4(),
        phone: Some("13800138000".into()),
        email: Some("test@example.com".into()),
        username: "testuser".into(),
        display_name: Some("Test User".into()),
        avatar_url: None,
        status: UserStatus::Active,
        user_type: UserType::Standard,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn jwt_generate_and_verify() {
    let mgr = JwtManager::new(JwtConfig::default());
    let user = test_user();
    let (access, refresh) = mgr
        .generate_token(
            &user,
            vec!["ws-001".into(), "ws-002".into()],
            vec![Permission::AgentRead, Permission::AgentWrite],
        )
        .unwrap();

    assert!(!access.is_empty());
    assert!(!refresh.is_empty());

    let claims = mgr.verify_token(&access).unwrap();
    assert_eq!(claims.sub, user.id.to_string());
    assert_eq!(claims.preferred_username, "testuser");
    assert_eq!(claims.workspace_ids, vec!["ws-001", "ws-002"]);
    assert!(claims.permissions.contains(&Permission::AgentRead));
}

#[test]
fn jwt_refresh_access_token() {
    let mgr = JwtManager::new(JwtConfig::default());
    let user = test_user();
    let (access, refresh) = mgr.generate_token(&user, vec![], vec![]).unwrap();

    // Small sleep to ensure different iat
    std::thread::sleep(Duration::from_millis(10));

    let new_access = mgr.refresh_access_token(&refresh).unwrap();
    assert!(!new_access.is_empty());
    assert_ne!(new_access, access);

    let claims = mgr.verify_token(&new_access).unwrap();
    assert_eq!(claims.sub, user.id.to_string());
}

#[test]
fn jwt_expired_token_fails() {
    let config = JwtConfig {
        access_token_ttl_minutes: -5,
        ..Default::default()
    };
    let mgr = JwtManager::new(config);
    let user = test_user();
    let (access, _) = mgr.generate_token(&user, vec![], vec![]).unwrap();

    let err = mgr.verify_token(&access).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("expired") || msg.contains("Expired"),
        "expected expired error, got: {msg}"
    );
}

#[test]
fn jwt_invalid_token_fails() {
    let mgr = JwtManager::new(JwtConfig::default());
    let err = mgr.verify_token("not-a-valid-jwt").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Invalid") || msg.contains("invalid"),
        "expected invalid token error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Password hashing tests (bcrypt)
// ---------------------------------------------------------------------------

#[test]
fn bcrypt_hash_and_verify() {
    let password = "MyS3cur3P@ssw0rd!";
    let hash = bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap();
    assert!(!hash.is_empty());

    let valid = bcrypt::verify(password, &hash).unwrap();
    assert!(valid);

    let invalid = bcrypt::verify("wrong-password", &hash).unwrap();
    assert!(!invalid);
}

#[test]
fn bcrypt_hash_different_salts() {
    let password = "same-password";
    let hash1 = bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap();
    let hash2 = bcrypt::hash(password, bcrypt::DEFAULT_COST).unwrap();
    assert_ne!(hash1, hash2);

    assert!(bcrypt::verify(password, &hash1).unwrap());
    assert!(bcrypt::verify(password, &hash2).unwrap());
}

// ---------------------------------------------------------------------------
// RBAC tests
// ---------------------------------------------------------------------------

#[test]
fn rbac_super_admin_has_all_permissions() {
    let all = vec![
        Permission::AgentRead,
        Permission::AgentWrite,
        Permission::WorkspaceManageMembers,
        Permission::WorkspaceConfig,
        Permission::QuotaRead,
        Permission::QuotaAdmin,
        Permission::UserAdmin,
    ];
    for p in &all {
        assert!(
            RoleChecker::has_permission(Role::SuperAdmin, *p),
            "SuperAdmin should have {:?}",
            p
        );
    }
}

#[test]
fn rbac_visitor_read_only() {
    assert!(RoleChecker::has_permission(
        Role::Visitor,
        Permission::AgentRead
    ));
    assert!(!RoleChecker::has_permission(
        Role::Visitor,
        Permission::AgentWrite
    ));
    assert!(!RoleChecker::has_permission(
        Role::Visitor,
        Permission::WorkspaceManageMembers
    ));
    assert!(!RoleChecker::has_permission(
        Role::Visitor,
        Permission::QuotaAdmin
    ));
    assert!(!RoleChecker::has_permission(
        Role::Visitor,
        Permission::UserAdmin
    ));
}

#[test]
fn rbac_member_can_use_agents() {
    assert!(RoleChecker::has_permission(
        Role::Member,
        Permission::AgentRead
    ));
    assert!(RoleChecker::has_permission(
        Role::Member,
        Permission::AgentWrite
    ));
    assert!(RoleChecker::has_permission(
        Role::Member,
        Permission::QuotaRead
    ));
    assert!(!RoleChecker::has_permission(
        Role::Member,
        Permission::QuotaAdmin
    ));
    assert!(!RoleChecker::has_permission(
        Role::Member,
        Permission::UserAdmin
    ));
}

#[test]
fn rbac_owner_can_manage_workspace() {
    assert!(RoleChecker::has_permission(
        Role::Owner,
        Permission::WorkspaceManageMembers
    ));
    assert!(RoleChecker::has_permission(
        Role::Owner,
        Permission::WorkspaceConfig
    ));
    assert!(RoleChecker::has_permission(
        Role::Owner,
        Permission::QuotaAdmin
    ));
    assert!(!RoleChecker::has_permission(
        Role::Owner,
        Permission::UserAdmin
    ));
}

#[test]
fn rbac_org_admin_has_user_admin() {
    assert!(RoleChecker::has_permission(
        Role::OrgAdmin,
        Permission::UserAdmin
    ));
    assert!(RoleChecker::has_permission(
        Role::OrgAdmin,
        Permission::QuotaAdmin
    ));
}

// ---------------------------------------------------------------------------
// Session tests with mock Redis
// ---------------------------------------------------------------------------

async fn create_mock_redis() -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open("redis://127.0.0.1:6379/").expect("redis client");
    client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection")
}

#[tokio::test]
async fn session_create_and_get() {
    let redis = create_mock_redis().await;
    let mgr = SessionManager::new(redis);

    let user_id = Uuid::new_v4();
    let session = SessionInfo {
        user_id,
        workspace_id: Some("ws-test".into()),
        login_method: "phone".into(),
        login_ip: "127.0.0.1".into(),
        login_at: Utc::now(),
        last_active: Utc::now(),
        device_info: Some("Mozilla/5.0".into()),
    };

    let session_id = mgr.create(session.clone()).await.unwrap();
    let retrieved = mgr.get(user_id, session_id).await.unwrap();
    assert!(retrieved.is_some());

    let info = retrieved.unwrap();
    assert_eq!(info.user_id, user_id);
    assert_eq!(info.workspace_id, Some("ws-test".into()));
    assert_eq!(info.login_method, "phone");
}

#[tokio::test]
async fn session_refresh_updates_last_active() {
    let redis = create_mock_redis().await;
    let mgr = SessionManager::new(redis);

    let user_id = Uuid::new_v4();
    let session = SessionInfo {
        user_id,
        workspace_id: None,
        login_method: "wechat".into(),
        login_ip: "192.168.1.1".into(),
        login_at: Utc::now(),
        last_active: Utc::now(),
        device_info: None,
    };

    let session_id = mgr.create(session).await.unwrap();
    std::thread::sleep(Duration::from_millis(50));

    mgr.refresh(user_id, session_id).await.unwrap();
    let retrieved = mgr.get(user_id, session_id).await.unwrap().unwrap();
    assert!(retrieved.last_active > retrieved.login_at);
}

#[tokio::test]
async fn session_destroy() {
    let redis = create_mock_redis().await;
    let mgr = SessionManager::new(redis);

    let user_id = Uuid::new_v4();
    let session = SessionInfo {
        user_id,
        workspace_id: None,
        login_method: "ldap".into(),
        login_ip: "10.0.0.1".into(),
        login_at: Utc::now(),
        last_active: Utc::now(),
        device_info: None,
    };

    let session_id = mgr.create(session).await.unwrap();
    assert!(mgr.get(user_id, session_id).await.unwrap().is_some());

    mgr.destroy(user_id, session_id).await.unwrap();
    assert!(mgr.get(user_id, session_id).await.unwrap().is_none());
}

#[tokio::test]
async fn session_destroy_all() {
    let redis = create_mock_redis().await;
    let mgr = SessionManager::new(redis);

    let user_id = Uuid::new_v4();
    let s1 = SessionInfo {
        user_id,
        workspace_id: Some("ws-1".into()),
        login_method: "phone".into(),
        login_ip: "127.0.0.1".into(),
        login_at: Utc::now(),
        last_active: Utc::now(),
        device_info: None,
    };
    let s2 = SessionInfo {
        user_id,
        workspace_id: Some("ws-2".into()),
        login_method: "wechat".into(),
        login_ip: "127.0.0.1".into(),
        login_at: Utc::now(),
        last_active: Utc::now(),
        device_info: None,
    };

    let id1 = mgr.create(s1).await.unwrap();
    let id2 = mgr.create(s2).await.unwrap();

    assert!(mgr.get(user_id, id1).await.unwrap().is_some());
    assert!(mgr.get(user_id, id2).await.unwrap().is_some());

    mgr.destroy_all(user_id).await.unwrap();

    assert!(mgr.get(user_id, id1).await.unwrap().is_none());
    assert!(mgr.get(user_id, id2).await.unwrap().is_none());
}
