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
	// Expected format: INSERT (key1, value1), (key2, value2) INTO tablename
	// Minimum tokens: INSERT (k, v) INTO t (8 tokens)
	if len(tokens) < 8 {
		return nil, errors.New("invalid INSERT syntax: too few arguments")
	}
	if strings.ToUpper(tokens[0]) != "INSERT" {
		return nil, errors.New("expected INSERT keyword")
	}

	intoIndex := -1
	for i := 0; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "INTO" {
			intoIndex = i
			break
		}
	}

	if intoIndex == -1 {
		return nil, errors.New("invalid INSERT syntax: expected INTO keyword")
	}
	// The "INTO" keyword must appear after the key-value pairs,
	// at least after "(", key, ",", value, ")" (5 tokens after INSERT).
	if intoIndex < 6 { // INSERT (a,b) INTO c -> tokens[0] to tokens[7], INTO is at index 6
		return nil, errors.New("invalid INSERT syntax: INTO keyword in wrong position")
	}

	// The table name is the token immediately after "INTO"
	if intoIndex+1 >= len(tokens) {
		return nil, errors.New("invalid INSERT syntax: expected table name after INTO")
	}
	table := tokens[intoIndex+1]

	// Check for any unexpected tokens after the table name
	if intoIndex+2 < len(tokens) {
		return nil, errors.New("invalid INSERT syntax: unexpected tokens after table name")
	}

	// The key-value pairs are the tokens between "INSERT" (index 0) and "INTO" (intoIndex)
	valuesTokens := tokens[1:intoIndex]
	rawValues := strings.Join(valuesTokens, "") // This will join `(key1,value1),(key2,value2)`

	matches := pairRegex.FindAllStringSubmatch(rawValues, -1)
	if len(matches) == 0 {
		return nil, errors.New("invalid INSERT syntax: no valid (key, value) pairs found")
	}

	var values []KeyValue
	for _, match := range matches {
		if len(match) != 3 { // Full match, capture group 1 (key), capture group 2 (value)
			return nil, errors.New("invalid match format for key-value pairs")
		}
		key := strings.TrimSpace(match[1])
		value := strings.TrimSpace(match[2])
		values = append(values, KeyValue{Key: key, Value: value})
	}

	return &InsertStatement{
		Table:  table,
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
		return nil, errors.New("unexpected token after table name. SELECT statement does not support WHERE clause anymore")
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
	// Expected format: DELETE key1, key2 FROM tableName
	if len(tokens) < 4 { // Minimum: DELETE key FROM table
		return nil, errors.New("invalid DELETE syntax: expected DELETE <keys> FROM <table_name>")
	}
	if strings.ToUpper(tokens[0]) != "DELETE" {
		return nil, errors.New("expected DELETE keyword")
	}

	fromIndex := -1
	for i := 0; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "FROM" {
			fromIndex = i
			break
		}
	}

	if fromIndex == -1 {
		return nil, errors.New("invalid DELETE syntax: expected FROM keyword")
	}
	if fromIndex < 2 { // DELETE <key> FROM ...
		return nil, errors.New("invalid DELETE syntax: missing keys or FROM keyword")
	}

	// The table name is the token immediately after "FROM"
	if fromIndex+1 >= len(tokens) {
		return nil, errors.New("invalid DELETE syntax: expected table name after FROM")
	}
	table := tokens[fromIndex+1]

	// Check for any unexpected tokens after the table name
	if fromIndex+2 < len(tokens) {
		return nil, errors.New("invalid DELETE syntax: unexpected tokens after table name")
	}

	var keys []string
	// The tokens between "DELETE" (tokens[0]) and "FROM" (tokens[fromIndex]) are the keys to delete
	keyTokens := tokens[1:fromIndex]

	// Join the key tokens and then split by "," to handle ["key1", ",", "key2"] correctly
	joinedKeys := strings.Join(keyTokens, "")
	parsedKeys := strings.Split(joinedKeys, ",")
	for _, k := range parsedKeys {
		trimmedKey := strings.TrimSpace(k)
		if trimmedKey != "" {
			keys = append(keys, trimmedKey)
		}
	}

	if len(keys) == 0 {
		return nil, errors.New("invalid DELETE syntax: no keys specified for deletion")
	}

	return &DeleteStatement{
		Table: table,
		Keys:  keys,
	}, nil
}

func parseDrop(tokens []string) (Statement, error) {
	if len(tokens) != 2 || strings.ToUpper(tokens[0]) != "DROP" {
		return nil, errors.New("expected DROP table_name")
	}
	return &DropStatement{Table: tokens[1]}, nil
}
