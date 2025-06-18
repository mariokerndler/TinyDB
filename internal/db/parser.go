package db

import (
	"errors"
	"strings"
)

type Statement struct {
	Type     string
	Key      string
	Value    string
	StartKey string
	EndKey   string
}

func ParseSQL(input string) (*Statement, error) {
	parts := strings.Fields(input)

	if len(parts) == 0 {
		return nil, errors.New("empty input")
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) != 3 {
			return nil, errors.New("invalid SET")
		}
		return &Statement{Type: "SET", Key: parts[1], Value: parts[2]}, nil

	case "GET":
		if len(parts) != 2 {
			return nil, errors.New("invalid GET")
		}
		return &Statement{Type: "GET", Key: parts[1]}, nil

	case "DELETE":
		if len(parts) != 2 {
			return nil, errors.New("invalid DELETE")
		}
		return &Statement{Type: "DELETE", Key: parts[1]}, nil

	case "SCAN":
		if len(parts) != 3 {
			return nil, errors.New("invalid SCAN")
		}
		return &Statement{Type: "SCAN", StartKey: parts[1], EndKey: parts[2]}, nil

	default:
		return nil, errors.New("unknown command")
	}
}
