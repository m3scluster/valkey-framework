package utils

import "testing"

func TestGetenvReturnsValueAndFallback(t *testing.T) {
	t.Setenv("UTILS_GETENV_VALUE", "configured")
	if got := Getenv("UTILS_GETENV_VALUE", "fallback"); got != "configured" {
		t.Fatalf("Getenv configured value = %q, want configured", got)
	}
	if got := Getenv("UTILS_GETENV_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("Getenv missing value = %q, want fallback", got)
	}
}

func TestLookupEnvPreservesExplicitEmptyValue(t *testing.T) {
	t.Setenv("UTILS_GETENV_EMPTY", "")
	if got, ok := LookupEnv("UTILS_GETENV_EMPTY"); !ok || got != "" {
		t.Fatalf("LookupEnv explicit empty value = (%q, %v), want (empty, true)", got, ok)
	}
	if got := Getenv("UTILS_GETENV_EMPTY", "fallback"); got != "fallback" {
		t.Fatalf("Getenv explicit empty value = %q, want fallback", got)
	}
}
