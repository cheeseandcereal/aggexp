package component

import (
	"strings"
	"testing"
)

func TestNewOptionsDefaults(t *testing.T) {
	t.Parallel()
	o := NewOptions()
	if o.BackendAddr == "" {
		t.Error("BackendAddr default missing")
	}
	if o.BackendTimeout <= 0 {
		t.Error("BackendTimeout default missing")
	}
	if !o.UseTypedWrapper {
		t.Error("UseTypedWrapper should default true (substrate assumes SSA)")
	}
	if o.ServerName == "" {
		t.Error("ServerName default missing")
	}
}

func TestValidateRejectsEmptyBackendAddr(t *testing.T) {
	t.Parallel()
	o := NewOptions()
	o.BackendAddr = "   "
	err := o.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "--backend-addr") {
		t.Errorf("error message should mention --backend-addr: %v", err)
	}
}
