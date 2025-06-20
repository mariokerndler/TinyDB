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
	fromIndex := -1
	for i := 0; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "FROM" {
			fromIndex = i
			break
		}
	}

	if fromIndex == -1 {
		return nil, errors.New("expected FROM keyword")
	}
	// "SELECT" "keys_or_star" "FROM" "table" is the minimum valid structure.
	// This means "FROM" must be at least at index 2 (e.g., SELECT * FROM).
	if fromIndex < 2 {
		return nil, errors.New("invalid SELECT syntax: missing columns or FROM keyword")
	}

	// Check if there's a token after "FROM" for the table name
	if fromIndex+1 >= len(tokens) {
		return nil, errors.New("expected table name after FROM")
	}
	table := tokens[fromIndex+1]
	// No need for `if table == ""` check here because `strings.Fields` ensures non-empty tokens.

	// Check if there are any unexpected tokens after the table name
	if fromIndex+2 < len(tokens) {
		return nil, errors.New("unexpected token after table name. SELECT statement does not support WHERE clause anymore.")
	}

	var keys []string
	// The tokens between "SELECT" (tokens[0]) and "FROM" (tokens[fromIndex]) are the selected columns
	columnTokens := tokens[1:fromIndex]

	if len(columnTokens) == 1 && columnTokens[0] == "*" {
		// SELECT * FROM ...
		// keys will remain empty, which signifies "all keys" in engine.go
	} else {
		// SELECT key1, key2 FROM ...
		// Join the column tokens and then split by "," to handle ["key1", ",", "key2"] correctly
		joinedKeys := strings.Join(columnTokens, "")
		parsedKeys := strings.Split(joinedKeys, ",")
		for _, k := range parsedKeys {
			trimmedKey := strings.TrimSpace(k)
			if trimmedKey != "" {
				keys = append(keys, trimmedKey)
			}
		}
		if len(keys) == 0 { // This might happen if input was just "SELECT FROM test" or similar malformed query
			return nil, errors.New("invalid SELECT syntax: no keys specified")
		}
	}

	return &SelectStatement{
		Table: table,
		Keys:  keys,
	}, nil
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
