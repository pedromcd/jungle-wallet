package bootstrap

import (
	"testing"

	"go.uber.org/fx"
)

// TestGraphIsValid checks that every constructor dependency is satisfiable
// without starting anything (no database, broker or Keycloak needed).
func TestGraphIsValid(t *testing.T) {
	if err := fx.ValidateApp(Modules); err != nil {
		t.Fatalf("invalid dependency graph: %v", err)
	}
}
