package main

import (
	"testing"
)

func TestMakeIDMapping(t *testing.T) {
	t.Run("id", func(t *testing.T) {
		fun, err := makeIDMapping("id")
		if err != nil {
			t.Fatalf("makeIDMapping failed: %v", err)
		}
		got := fun(42)
		if want := 42; got != want {
			t.Errorf("makeIDMapping: got %v, want %v", got, want)
		}
	})

	t.Run("current", func(t *testing.T) {
		fun, err := makeIDMapping("current")
		if err != nil {
			t.Fatalf("makeIDMapping failed: %v", err)
		}
		got := fun(42)
		if want := -1; got != want {
			t.Errorf("makeIDMapping: got %v, want %v", got, want)
		}
	})
}

func TestParseIDMappingSpec(t *testing.T) {
	t.Run("id", func(t *testing.T) {
		got, err := parseIDMappingSpec("id")
		if err != nil {
			t.Fatalf("parseIDMappingSpec failed: %v", err)
		}
		if want := identityIDMapping; got != want {
			t.Errorf("parseIDMappingSpec: got %v, want %v", got, want)
		}
	})

	t.Run("current", func(t *testing.T) {
		got, err := parseIDMappingSpec("current")
		if err != nil {
			t.Fatalf("parseIDMappingSpec failed: %v", err)
		}
		if want := currentIDMapping; got != want {
			t.Errorf("parseIDMappingSpec: got %v, want %v", got, want)
		}
	})
}
