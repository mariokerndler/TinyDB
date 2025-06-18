package db

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var pairRegex = regexp.MustCompile(`\(\s*([^)]+?)\s*,\s*([^)]+?)\s*\)`)

func Parse(input string) (Statement, error) {
	tokens := tokenize(input)

	if len(tokens) == 0 {
		return nil, errors.New("empty input")
	}

	switch strings.ToUpper(tokens[0]) {
	case "INSERT":
		return parseInsert(tokens)
	case "SELECT":
		return parseSelect(tokens)
	case "DELETE":
		return parseDelete(tokens)
	case "DROP":
		return parseDrop(tokens)
	default:
		return nil, fmt.Errorf("unsupported statement: %s", tokens[0])
	}
}

func tokenize(input string) []string {
	input = strings.ReplaceAll(input, "(", " ( ")
	input = strings.ReplaceAll(input, ")", " ) ")
	input = strings.ReplaceAll(input, ",", " , ")
	return strings.Fields(input)
}

func parseInsert(tokens []string) (Statement, error) {
	if len(tokens) < 5 {
		return nil, errors.New("invalid INSERT syntax")
	}
	if strings.ToUpper(tokens[1]) != "INTO" {
		return nil, errors.New("expected INTO after INSERT")
	}
	if strings.ToUpper(tokens[3]) != "VALUES" {
		return nil, errors.New("expected VALUES keyword")
	}

	// Join remaining tokens into one string
	raw := strings.Join(tokens[4:], " ")
	// Example: "(key1, value1), (key2, value2)"

	matches := pairRegex.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil, errors.New("no valid (key, value) pairs found")
	}

	var values []KeyValue
	for _, match := range matches {
		if len(match) != 3 {
			return nil, errors.New("invalid match format")
		}
		key := strings.TrimSpace(match[1])
		value := strings.TrimSpace(match[2])
		values = append(values, KeyValue{Key: key, Value: value})
	}

	return &InsertStatement{
		Table:  tokens[2],
		Values: values,
	}, nil
}

func parseSelect(tokens []string) (Statement, error) {
	// SELECT * FROM table  -> tokens length 4
	// SELECT * FROM table WHERE key = value  -> tokens length 8

	if len(tokens) == 4 {
		if tokens[1] != "*" || strings.ToUpper(tokens[2]) != "FROM" {
			return nil, errors.New("expected SELECT * FROM")
		}
		return &SelectStatement{
			Table: tokens[3],
			Where: nil, // no WHERE clause
		}, nil
	}

	if len(tokens) == 8 {
		if tokens[1] != "*" || strings.ToUpper(tokens[2]) != "FROM" {
			return nil, errors.New("expected SELECT * FROM")
		}
		if strings.ToUpper(tokens[4]) != "WHERE" || tokens[6] != "=" {
			return nil, errors.New("expected WHERE key = value")
		}
		return &SelectStatement{
			Table: tokens[3],
			Where: &WhereClause{
				Key:   tokens[5],
				Value: tokens[7],
			},
		}, nil
	}

	return nil, errors.New("invalid SELECT syntax")
}

func parseDelete(tokens []string) (Statement, error) {
	if len(tokens) != 7 {
		return nil, errors.New("invalid DELETE syntax")
	}
	if strings.ToUpper(tokens[1]) != "FROM" {
		return nil, errors.New("expected FROM after DELETE")
	}
	if strings.ToUpper(tokens[3]) != "WHERE" || tokens[5] != "=" {
		return nil, errors.New("expected WHERE key = value")
	}

	return &DeleteStatement{
		Table: tokens[2],
		Key:   tokens[4],
		Value: tokens[6],
	}, nil
}

func parseDrop(tokens []string) (Statement, error) {
	if len(tokens) != 3 || strings.ToUpper(tokens[1]) != "TABLE" {
		return nil, errors.New("expected DROP TABLE table_name")
	}
	return &DropStatement{Table: tokens[2]}, nil
}
