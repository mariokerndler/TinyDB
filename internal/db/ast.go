package db

type Statement interface {
	StmtType() string
}

// --- INSERT STATEMENT ---
type KeyValue struct {
	Key   string
	Value string
}

type InsertStatement struct {
	Table  string
	Values []KeyValue
}

func (s *InsertStatement) StmtType() string {
	return "INSERT"
}

// --- SELECT STATEMENT ---
type SelectStatement struct {
	Table string
	Keys  []string
}

func (s *SelectStatement) StmtType() string {
	return "SELECT"
}

// --- DELETE STATEMENT ---
type DeleteStatement struct {
	Table string
	Keys  []string
}

func (s *DeleteStatement) StmtType() string {
	return "DELETE"
}

// --- DROP STATEMENT ---
type DropStatement struct {
	Table string
}

func (s *DropStatement) StmtType() string {
	return "DROP"
}

// --- UPDATE STATEMENT ---
type UpdateStatement struct {
	Table  string
	Values []KeyValue
}

func (s *UpdateStatement) StmtType() string {
	return "UPDATE"
}

// --- BEGIN STATEMENT ---
type BeginStatement struct{}

func (s *BeginStatement) StmtType() string { return "BEGIN" }

// --- COMMIT STATEMENT ---
type CommitStatement struct{}

func (s *CommitStatement) StmtType() string { return "COMMIT" }

// --- ROLLBACK STATEMENT ---
type RollbackStatement struct{}

func (s *RollbackStatement) StmtType() string { return "ROLLBACK" }
