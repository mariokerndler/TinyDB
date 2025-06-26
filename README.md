# TinyDB
TinyDB is a minimalistic database engine written in Go, demonstrating fundamental database concepts like parsing SQL-like commands, managing data in a B+ tree, and persistent storage via a Write-Ahead Log (WAL).

## Supported Commands
This section outlines the SQL-like commands currently supported by TinyDB.

###  1. INSERT Statement
Used to insert key-value pairs into a specified table.

**Syntax:**
```
INSERT (<key1>, <value1>), (<key2>, <value2>), ... INTO <table_name>
```

**Examples:**
```
INSERT (id1, Alice), (id2, Bob) INTO users 
INSERT (prod_a, Laptop), (prod_b, Mouse) INTO products 
```

### 2. SELECT Statement
Used to retrieve data from a specified table. It supports selecting all key-value pairs or specific keys. The WHERE clause is currently not supported for SELECT statements.

**Syntax:**

To select all key-value pairs:
```
SELECT * FROM <table_name>
```
To select one or more specific keys:
```
SELECT <key1>[, <key2>, ...] FROM <table_name>
```
**Examples:**
```
SELECT * FROM users
SELECT id1 FROM users
SELECT prod_a, prod_b FROM products
```

### 3. DELETE Statement
Used to delete a specific key-value pair from a table based on a WHERE clause.

**Syntax:**
```
DELETE <key1>[, <key2>, ...] FROM <table_name>
```
**Examples:**
```
DELETE id FROM users
DELETE name FROM products
```

### 4. DROP Statement
Used to "drop" (clear) all data from a specified table. In this simple implementation, it effectively clears all entries in the underlying B+ tree and WAL for the conceptual "table".

**Syntax:**
```
DROP <table_name>
```

**Examples:**
```
DROP users
DROP products
```

### 5. UPDATE Statement
Used to modify the value associated with an existing key in a specified table.

**Syntax:**
```
UPDATE <table_name> SET (<key1>, <new_value1>)[, (<key2>, <new_value2>)...]
```

**Examples:**
```
UPDATE users SET (id1, Alicia)
UPDATE products SET (prod_a, GamingLaptop), (prod_b, WirelessMouse)
```
### 6. SHOW TABLES Statement
Used to list all currently existing tables in the database. If the database is within a transaction, tables created or modified within that transaction will be prefixed with the transaction ID.

**Syntax:**
```
SHOW TABLES
```

**Examples:**
```
SHOW TABLES
```

**Output Example (outside transaction):**
```
Tables:
- users
- products
```

**Output Example (inside transaction, where `new_users` was created in the transaction `tx_12345`):**
```
Tables:
- products
- users
- [tx_12345] new_users
```

## Transaction Management
TinyDB supports basic transaction management, allowing a series of operations to be grouped and either committed or rolled back. This provides atomicity for operations.

### BEGIN Statement
Initiates a new transaction. If a transaction is already active, it will return an error.

**Syntax:**
```
BEGIN
```

**Example:**
```
BEGIN
```

### COMMIT Statement
Applies all changes buffered within the current transaction to the main database. Once committed, the changes are permanent and written to the WAL.

**Syntax:**
```
COMMIT
```

**Example:**
```
COMMIT
```

### ROLLBACK Statement
Discards all changes buffered within the current transaction, effectively undoing any operations performed since the `BEGIN` statement. The database reverts to its state before the transaction began.

**Syntax:**
```
ROLLBACK
```

**Example:**
```
ROLLBACK
```

