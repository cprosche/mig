package mig

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"io/fs"
	"math"
	"sort"
	"strconv"
	"unicode"
)

// TODO: sqlite
// TODO: postgres
// TODO: mysql
// TODO: mssql

// Config is the configuration for the mig package
type Config struct {
	// Db is the database connection used for migrations
	Db *sql.DB

	// Fs is the filesystem where the migrations are stored
	Fs fs.FS

	// If Fs is nil, then this slice of migrations will be used
	Migrations []Migration
}

// Mig is the main struct for the mig package
type Mig struct {
	config Config
}

var migrationTableSchema = `
		CREATE TABLE IF NOT EXISTS migrations (
			id INTEGER PRIMARY KEY,
			filename TEXT,
			raw TEXT,
			hash TEXT,
			up TEXT,
			down TEXT
		)
`

type Migration struct {
	Id       int
	FileName string

	Raw  string
	Hash string

	Up   string
	Down string
}

func New(c Config) *Mig {
	return &Mig{
		config: c,
	}
}

func (m *Mig) Migrate() error {
	if m.config.Db == nil {
		return fmt.Errorf("db is nil")
	}

	m.config.Db.Exec(migrationTableSchema)

	var providedMigrations []Migration
	var err error
	if m.config.Fs != nil {
		providedMigrations, err = getMigrationsFromFS(m.config.Fs)
		if err != nil {
			return fmt.Errorf("mig: error getting migrations from fs: %w", err)
		}
	} else {
		providedMigrations = m.config.Migrations
	}
	if len(providedMigrations) == 0 {
		return fmt.Errorf("mig: no migrations provided")
	}

	dbMigrations, err := getMigrationsFromDB(m.config.Db)
	if err != nil {
		return fmt.Errorf("mig: error getting migrations from db: %w", err)
	}

	upStart := 1
	downEnd := math.MaxInt
	for _, pm := range providedMigrations {
		// get matching db migration
		dbm := Migration{}
		for _, m := range dbMigrations {
			if pm.Id == m.Id {
				dbm = m
				break
			}
		}
		upStart = pm.Id

		// if no matching db migration, then migrate starting from here
		if dbm.Id == 0 {
			break
		}

		// if hash doesn't match, then migrate down to here
		if dbm.Hash != pm.Hash {
			downEnd = dbm.Id
			fmt.Printf("mig: found hash mismatch at id %d, running down migrations", dbm.Id)
			break
		}
	}

	if downEnd < math.MaxInt {
		err = runDownMigrations(m.config.Db, dbMigrations, downEnd)
		if err != nil {
			return fmt.Errorf("mig: error running down migrations: %w", err)
		}

		err = runUpMigrations(m.config.Db, providedMigrations, upStart)
		if err != nil {
			return fmt.Errorf("mig: error running up migrations: %w", err)
		}
	}

	fmt.Println(providedMigrations)
	return nil
}

func runUpMigrations(db *sql.DB, migrations []Migration, startId int) error {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Id < migrations[j].Id
	})
	for _, m := range migrations {
		if m.Id < startId {
			continue
		}

		_, err := db.Exec(m.Up)
		if err != nil {
			return fmt.Errorf("mig: error running up migration %d: %w", m.Id, err)
		}

		_, err = db.Exec(
			`
		INSERT INTO 
			migrations (id, filename, raw, hash, up, down) 
		VALUES 
			(?, ?, ?, ?, ?, ?)
		`,
			m.Id,
			m.FileName,
			m.Raw,
			m.Hash,
			m.Up,
			m.Down,
		)

		if err != nil {
			return fmt.Errorf("mig: error inserting migration %d into db: %w", m.Id, err)
		}

		fmt.Printf("mig: ran up migration %d\n", m.Id)
	}

	return nil
}

func runDownMigrations(db *sql.DB, migrations []Migration, endId int) error {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Id < migrations[j].Id
	})
	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]

		// if we reach the end id, then we're done, but make sure we run end id migration
		if m.Id < endId {
			break
		}

		_, err := db.Exec(m.Down)
		if err != nil {
			return fmt.Errorf("mig: error running down migration %d: %w", m.Id, err)
		}

		_, err = db.Exec(`DELETE FROM migrations WHERE id = ?`, m.Id)
		if err != nil {
			return fmt.Errorf("mig: error deleting migration %d from db: %w", m.Id, err)
		}

		fmt.Printf("mig: ran down migration %d\n", m.Id)

	}

	return nil
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))

	result := fmt.Sprint(h.Sum32())

	// pad result with 0s to 10 characters
	for len(result) < 10 {
		result = "0" + result
	}

	return result
}

func getMigrationsFromFS(fsys fs.FS) ([]Migration, error) {
	result := []Migration{}

	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		m := Migration{}
		m.FileName = entry.Name()
		m.Id, err = getIntFromFileName(m.FileName)
		if err != nil {
			return nil, err
		}

		contents, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, err
		}
		m.Raw = string(contents)
		m.Hash = hash(m.Raw)

		result = append(result, m)
	}

	// sort by id
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	return result, nil
}

func getMigrationsFromDB(db *sql.DB) ([]Migration, error) {
	rows, err := db.Query("SELECT * FROM migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []Migration{}
	for rows.Next() {
		m := Migration{}
		err = rows.Scan(&m.Id, &m.FileName, &m.Raw, &m.Hash, &m.Up, &m.Down)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}

	// sort by id
	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	return result, nil
}

// Expected filename format: 0001_create_users_table.sql.
// This filename would return 1.
// Starting number can be any length.
func getIntFromFileName(fileName string) (int, error) {
	numStr := ""

	for _, r := range fileName {
		if !unicode.IsDigit(r) {
			break
		}

		numStr += string(r)
	}

	if numStr == "" {
		return 0, fmt.Errorf("mig: no number found in filename")
	}

	result, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("mig: error converting number in filename to int: %w", err)
	}
	if result < 1 {
		return 0, fmt.Errorf("mig: number in filename must be greater than 0")
	}

	return result, nil
}
