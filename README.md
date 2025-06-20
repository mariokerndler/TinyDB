# TinySQL
TinySQL is a minimalistic database engine written in Go, demonstrating fundamental database concepts like parsing SQL-like commands, managing data in a B+ tree, and persistent storage via a Write-Ahead Log (WAL).

## Supported Commands
This section outlines the SQL-like commands currently supported by TinySQL.

###  1. INSERT Statement
Used to insert key-value pairs into a specified table.

**Syntax:**
```
INSERT INTO <table_name> VALUES (<key1>, <value1>), (<key2>, <value2>), ...
```

**Examples:**
```
INSERT INTO users VALUES (id1, Alice), (id2, Bob)
INSERT INTO products VALUES (prod_a, Laptop), (prod_b, Mouse)
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
DELETE FROM <table_name> WHERE <key> = <value>
```
**Examples:**
```
DELETE FROM users WHERE id = id1
DELETE FROM products WHERE name = Laptop
```

### 4. DROP Statement
Used to "drop" (clear) all data from a specified table. In this simple implementation, it effectively clears all entries in the underlying B+ tree and WAL for the conceptual "table".

**Syntax:**
```
DROP TABLE <table_name>
```

**Examples:**
```
DROP TABLE users
DROP TABLE products
```