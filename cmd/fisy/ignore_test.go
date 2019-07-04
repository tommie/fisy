package main

import (
	"testing"
)

func TestParseIgnoreFilter(t *testing.T) {
	fun, err := parseIgnoreFilter("/a/\n!/a/b/")
	if err != nil {
		t.Fatalf("parseIgnoreFilter failed: %v", err)
	}

	if want := true; fun("/a/c") != want {
		t.Errorf("parseIgnoreFilter /a/c: got %v, want %v", !want, want)
	}
	if want := false; fun("/a/b/d") != want {
		t.Errorf("parseIgnoreFilter /a/b/d: got %v, want %v", !want, want)
	}
}
