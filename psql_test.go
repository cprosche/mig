package mig

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestPostgres(t *testing.T) {
	t.Run("FS migrate runs successfully", func(t *testing.T) {
		err := startPostgresContainer()
		assert.NoError(t, err)
		defer stopPostgresContainer()

		db, err := getPostgresConnection()
		assert.NoError(t, err)
		defer db.Close()

		m, err := New(Config{
			Db: db,
			Fs: os.DirFS("./test/migrations1"),
		})
		if err != nil {
			t.Fatalf("failed to create mig: %v", err)
		}

		err = m.Migrate()
		assert.NoError(t, err)

		tableMustExistPostgres(t, db, "migrations")
		tableMustExistPostgres(t, db, "test_table_1")
		tableMustExistPostgres(t, db, "test_table_2")
		tableMustExistPostgres(t, db, "test_table_3")
		tableMustNotExistPostgres(t, db, "test_table_4")
	})
}

func startPostgresContainer() error {
	cmd := exec.Command("docker", "run", "-d", "--name", "pg", "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=secret", "postgres")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to start postgres container: %v, output: %s", err, string(output))
	}
	return nil
}

func stopPostgresContainer() {
	cmd := exec.Command("docker", "stop", "pg")
	if err := cmd.Run(); err != nil {
		log.Printf("failed to stop postgres container: %v", err)
	}
	cmd = exec.Command("docker", "rm", "-f", "pg")
	if err := cmd.Run(); err != nil {
		log.Printf("failed to remove postgres container: %v", err)
	}
}

func getPostgresConnection() (*sql.DB, error) {
	connStr := "user=postgres password=secret dbname=postgres sslmode=disable host=localhost port=5432"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %v", err)
	}

	for i := 0; i < 10; i++ {
		err = db.Ping()
		if err == nil {
			return db, nil
		}
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to postgres: %v", err)
}

func tableMustExistPostgres(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_name = $1;", tableName)
	assert.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next(), "table %s must exist", tableName)
}

func tableMustNotExistPostgres(t *testing.T, db *sql.DB, tableName string) {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_name = $1;", tableName)
	assert.NoError(t, err)
	defer rows.Close()
	assert.False(t, rows.Next(), "table %s must not exist", tableName)
}
