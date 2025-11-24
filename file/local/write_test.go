package local

import (
	"os"
	"strings"
	"testing"
)

func TestOpenF_errPerms(t *testing.T) {
	// dir bad perms
	_, _, err := openF("/private/bad/perms/dir/file.txt", false)
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		// Expected error
		return
	}
	t.Error("Expected permission denied error")
}

func TestOpenF_errDir(t *testing.T) {
	// dir bad perms
	_, _, err := openF("/dir/path/", false)
	if err == nil {
		t.Error("Expected error for directory path")
		return
	}
	if !strings.Contains(err.Error(), "references a directory") {
		t.Errorf("Expected directory error, got: %v", err)
	}
}

func TestCloseF_err(t *testing.T) {
	// showing: closeF no err on nil f
	var nilF *os.File
	err := closeF("path.txt", nilF)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
}
