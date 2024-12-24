package mig

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

// TODO: add tests for sqlite
func TestMigrate(t *testing.T) {
	t.Run("fresh migrate runs successfully", func(t *testing.T) {
		testDbPath := "./test/test2.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		migrations := []Migration{
			{
				Id:       1,
				FileName: "01_test1.sql",
				Up:       "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);",
				Down:     "DROP TABLE test1;",
			},
			{
				Id:       2,
				FileName: "02_test2.sql",
				Up:       "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT);",
				Down:     "DROP TABLE test2;",
			},
		}

		m, err := New(Config{
			Db:         db,
			Migrations: migrations,
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustNotExist(t, db, "test3")

		os.Remove(testDbPath)
	})

	t.Run("migrating twice works", func(t *testing.T) {
		testDbPath := "./test/test3.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		migrations := []Migration{
			{
				Id:       1,
				FileName: "01_test1.sql",
				Up:       "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);",
				Down:     "DROP TABLE test1;",
			},
			{
				Id:       2,
				FileName: "02_test2.sql",
				Up:       "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT);",
				Down:     "DROP TABLE test2;",
			},
		}

		m, err := New(Config{
			Db:         db,
			Migrations: migrations,
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")

		os.Remove(testDbPath)
	})

	t.Run("adding migrations works", func(t *testing.T) {
		testDbPath := "./test/test4.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		migrations := []Migration{
			{
				Id:   1,
				Up:   "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test1;",
			},
			{
				Id:   2,
				Up:   "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test2;",
			},
		}

		m, err := New(Config{
			Db:         db,
			Migrations: migrations,
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustNotExist(t, db, "test3")

		m.config.Migrations = append(m.config.Migrations, Migration{
			Id:   3,
			Up:   "CREATE TABLE test3 (id INTEGER PRIMARY KEY, name TEXT);",
			Down: "DROP TABLE test3;",
		})

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustExist(t, db, "test3")

		os.Remove(testDbPath)
	})

	t.Run("removing migrations works", func(t *testing.T) {
		testDbPath := "./test/test5.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		migrations := []Migration{
			{
				Id:   1,
				Up:   "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test1;",
			},
			{
				Id:   2,
				Up:   "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test2;",
			},
			{
				Id:   3,
				Up:   "CREATE TABLE test3 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test3;",
			},
		}

		m, err := New(Config{
			Db:         db,
			Migrations: migrations,
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustExist(t, db, "test3")

		m.config.Migrations = m.config.Migrations[:2]

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustNotExist(t, db, "test3")

		os.Remove(testDbPath)
	})

	t.Run("modifying migrations works", func(t *testing.T) {
		testDbPath := "./test/test6.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		migrations := []Migration{
			{
				Id:   1,
				Up:   "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test1;",
			},
			{
				Id:   2,
				Up:   "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test2;",
			},
			{
				Id:   3,
				Up:   "CREATE TABLE test3 (id INTEGER PRIMARY KEY, name TEXT);",
				Down: "DROP TABLE test3;",
			},
		}

		m, err := New(Config{
			Db:         db,
			Migrations: migrations,
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustExist(t, db, "test2")
		tableMustExist(t, db, "test3")
		tableMustNotExist(t, db, "test4")

		m.config.Migrations[1].Up = "CREATE TABLE test4 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);"
		m.config.Migrations[1].Down = "DROP TABLE test4;"

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExist(t, db, "migrations")
		tableMustExist(t, db, "test1")
		tableMustNotExist(t, db, "test2")
		tableMustExist(t, db, "test3")
		tableMustExist(t, db, "test4")

		os.Remove(testDbPath)
	})
}

func tableMustExist(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", tableName)
	assert.Nil(t, err)
	defer rows.Close()
	assert.True(t, rows.Next(), "table %s must exist", tableName)
}

func tableMustNotExist(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", tableName)
	assert.Nil(t, err)
	defer rows.Close()
	assert.False(t, rows.Next(), "table %s must not exist", tableName)
}
