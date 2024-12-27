package mig

import (
	"database/sql"
	"embed"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

//go:embed test/migrations1
var migrationsFS embed.FS

func TestMigrate(t *testing.T) {
	t.Run("fresh migrate runs successfully", func(t *testing.T) {
		testDbPath := "./test/test2.db"
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

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustNotExistSqlite(t, db, "test3")

		os.Remove(testDbPath)
	})

	t.Run("migrating twice works", func(t *testing.T) {
		testDbPath := "./test/test3.db"
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

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")

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

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustNotExistSqlite(t, db, "test3")

		m.config.Migrations = append(m.config.Migrations, Migration{
			Id:   3,
			Up:   "CREATE TABLE test3 (id INTEGER PRIMARY KEY, name TEXT);",
			Down: "DROP TABLE test3;",
		})

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustExistSqlite(t, db, "test3")

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

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustExistSqlite(t, db, "test3")

		m.config.Migrations = m.config.Migrations[:2]

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustNotExistSqlite(t, db, "test3")

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

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustExistSqlite(t, db, "test2")
		tableMustExistSqlite(t, db, "test3")
		tableMustNotExistSqlite(t, db, "test4")

		m.config.Migrations[1].Up = "CREATE TABLE test4 (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);"
		m.config.Migrations[1].Down = "DROP TABLE test4;"

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test1")
		tableMustNotExistSqlite(t, db, "test2")
		tableMustExistSqlite(t, db, "test3")
		tableMustExistSqlite(t, db, "test4")

		m.config.Migrations[0].Up = "CREATE TABLE test5 (id INTEGER PRIMARY KEY, name TEXT);"
		m.config.Migrations[0].Down = "DROP TABLE test5;"

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustNotExistSqlite(t, db, "test1")
		tableMustNotExistSqlite(t, db, "test2")
		tableMustExistSqlite(t, db, "test3")
		tableMustExistSqlite(t, db, "test4")
		tableMustExistSqlite(t, db, "test5")

		os.Remove(testDbPath)
	})

	t.Run("migrations from FS work", func(t *testing.T) {
		testDbPath := "./test/test7.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		m, err := New(Config{
			Db: db,
			Fs: os.DirFS("./test/migrations1"),
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test_table_1")
		tableMustExistSqlite(t, db, "test_table_2")
		tableMustExistSqlite(t, db, "test_table_3")
		tableMustNotExistSqlite(t, db, "test_table_4")

		m.config.Migrations[2].Up = "CREATE TABLE test_table_5 (id INTEGER PRIMARY KEY, name TEXT);"
		m.config.Migrations[2].Down = "DROP TABLE test_table_5;"
		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test_table_1")
		tableMustExistSqlite(t, db, "test_table_2")
		tableMustNotExistSqlite(t, db, "test_table_3")
		tableMustNotExistSqlite(t, db, "test_table_4")
		tableMustExistSqlite(t, db, "test_table_5")

		os.Remove(testDbPath)
	})

	t.Run("migrating from embedded FS works", func(t *testing.T) {
		testDbPath := "./test/test8.db"
		db, err := sql.Open("sqlite3", testDbPath)
		assert.Nil(t, err)
		defer db.Close()

		m, err := New(Config{
			Db: db,

			Fs:              migrationsFS,
			OverrideDirName: "test/migrations1",
		})
		assert.Nil(t, err)

		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test_table_1")
		tableMustExistSqlite(t, db, "test_table_2")
		tableMustExistSqlite(t, db, "test_table_3")
		tableMustNotExistSqlite(t, db, "test_table_4")

		m.config.Migrations[2].Up = "CREATE TABLE test_table_5 (id INTEGER PRIMARY KEY, name TEXT);"
		m.config.Migrations[2].Down = "DROP TABLE test_table_5;"
		err = m.Migrate()
		assert.Nil(t, err)

		tableMustExistSqlite(t, db, "migrations")
		tableMustExistSqlite(t, db, "test_table_1")
		tableMustExistSqlite(t, db, "test_table_2")
		tableMustNotExistSqlite(t, db, "test_table_3")
		tableMustNotExistSqlite(t, db, "test_table_4")
		tableMustExistSqlite(t, db, "test_table_5")

		os.Remove(testDbPath)
	})
}

func tableMustExistSqlite(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=$1;", tableName)
	assert.Nil(t, err)
	defer rows.Close()
	assert.True(t, rows.Next(), "table %s must exist", tableName)
}

func tableMustNotExistSqlite(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=$1;", tableName)
	assert.Nil(t, err)
	defer rows.Close()
	assert.False(t, rows.Next(), "table %s must not exist", tableName)
}
