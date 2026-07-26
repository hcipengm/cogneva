use cog_core::{Permission, Role};

/// Role-to-permissions mapping.
pub struct RoleChecker;

impl RoleChecker {
    /// Returns `true` if `role` is granted `permission`.
    pub fn has_permission(role: Role, permission: Permission) -> bool {
        let perms = Self::permissions_for(role);
        perms.contains(&permission)
    }

    /// Returns all permissions granted to `role`.
    pub fn permissions_for(role: Role) -> Vec<Permission> {
        match role {
            Role::SuperAdmin => vec![
                Permission::AgentRead,
                Permission::AgentWrite,
                Permission::WorkspaceManageMembers,
                Permission::WorkspaceConfig,
                Permission::QuotaRead,
                Permission::QuotaAdmin,
                Permission::UserAdmin,
            ],
            Role::OrgAdmin => vec![
                Permission::AgentRead,
                Permission::AgentWrite,
                Permission::WorkspaceManageMembers,
                Permission::WorkspaceConfig,
                Permission::QuotaRead,
                Permission::QuotaAdmin,
                Permission::UserAdmin,
            ],
            Role::Owner => vec![
                Permission::AgentRead,
                Permission::AgentWrite,
                Permission::WorkspaceManageMembers,
                Permission::WorkspaceConfig,
                Permission::QuotaRead,
                Permission::QuotaAdmin,
            ],
            Role::Member => vec![
                Permission::AgentRead,
                Permission::AgentWrite,
                Permission::QuotaRead,
            ],
            Role::Visitor => vec![Permission::AgentRead, Permission::QuotaRead],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn super_admin_has_all_permissions() {
        let all = vec![
            Permission::AgentRead,
            Permission::AgentWrite,
            Permission::WorkspaceManageMembers,
            Permission::WorkspaceConfig,
            Permission::QuotaRead,
            Permission::QuotaAdmin,
            Permission::UserAdmin,
        ];
        for p in all {
            assert!(RoleChecker::has_permission(Role::SuperAdmin, p));
        }
    }

    #[test]
    fn visitor_is_read_only() {
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
    }

    #[test]
    fn member_can_write_agents() {
        assert!(RoleChecker::has_permission(
            Role::Member,
            Permission::AgentWrite
        ));
        assert!(!RoleChecker::has_permission(
            Role::Member,
            Permission::QuotaAdmin
        ));
    }
}
