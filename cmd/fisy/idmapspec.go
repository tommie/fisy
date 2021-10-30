package main

import (
	"fmt"
)

// makeIDMapping returns a mapping function for a specification of a
// UID/GID mapping.
func makeIDMapping(s string) (func(int) int, error) {
	kind, err := parseIDMappingSpec(s)
	if err != nil {
		return nil, err
	}

	switch kind {
	case identityIDMapping:
		return func(src int) int { return src }, nil
	case currentIDMapping:
		return func(src int) int { return -1 }, nil
	default:
		panic(fmt.Errorf("unhandled ID mapping kind: %s", kind))
	}
}

// parseIDMappingSpec parses a specification for a UID/GID mapping.
func parseIDMappingSpec(s string) (idMappingKind, error) {
	ss := idMappingKind(s)
	switch ss {
	case identityIDMapping, currentIDMapping:
		return ss, nil
	default:
		return "", fmt.Errorf("unknown ID mapping: %s", s)
	}
}

type idMappingKind = string

const (
	identityIDMapping idMappingKind = "id"
	currentIDMapping  idMappingKind = "current"
)
