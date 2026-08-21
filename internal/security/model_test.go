package security

import "testing"

func TestRolePermissions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		role       Role
		permission Permission
		want       bool
	}{
		{RoleMember, PermissionWorkflowWrite, true},
		{RoleMember, PermissionRunRead, true},
		{RoleMember, PermissionTaskRead, false},
		{RoleMember, PermissionRunOperate, false},
		{RoleOperator, PermissionTaskRead, true},
		{RoleOperator, PermissionRunOperate, true},
		{RoleOperator, PermissionProjectManage, false},
		{RoleAdmin, PermissionProjectManage, true},
		{RoleAdmin, PermissionAuditRead, true},
		{Role("owner"), PermissionProjectRead, false},
	}
	for _, test := range tests {
		if got := test.role.Allows(test.permission); got != test.want {
			t.Errorf("Role(%q).Allows(%q) = %t, want %t", test.role, test.permission, got, test.want)
		}
	}
}
