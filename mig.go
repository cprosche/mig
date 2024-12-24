package mig

import (
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"sort"
)

const (
	DEFAULT_UP_DELIMITER   = "-- up"
	DEFAULT_DOWN_DELIMITER = "-- down"
)

// Config is the configuration for Mig
type Config struct {
	// Db is the database connection used for migrations
	Db *sql.DB

	// Fs is the filesystem where the migrations are stored
	Fs fs.FS

	// If Fs is nil, then this slice of migrations will be used
	Migrations []Migration

	// Delimiters for splitting up and down migrations in a single file
	UpDelimiter   string
	DownDelimiter string
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

	raw  string
	hash string

	Up   string
	Down string
}

func New(c Config) *Mig {
	if c.UpDelimiter == "" {
		c.UpDelimiter = DEFAULT_UP_DELIMITER
	}
	if c.DownDelimiter == "" {
		c.DownDelimiter = DEFAULT_DOWN_DELIMITER
	}

	m := &Mig{
		config: c,
	}

	return m
}

func (m *Mig) Migrate() error {
	if m.config.Db == nil {
		return fmt.Errorf("db is nil")
	}

	m.config.Db.Exec(migrationTableSchema)

	var providedMigrations []Migration
	var err error
	if m.config.Fs != nil {
		providedMigrations, err = m.getMigrationsFromFS()
		if err != nil {
			return fmt.Errorf("mig: error getting migrations from fs: %w", err)
		}
	} else {
		providedMigrations = m.config.Migrations
		m.assignRaw(providedMigrations)
		assignHashes(providedMigrations)
	}
	if len(providedMigrations) == 0 {
		return fmt.Errorf("mig: no migrations provided")
	}

	dbMigrations, err := m.getMigrationsFromDB()
	if err != nil {
		return fmt.Errorf("mig: error getting migrations from db: %w", err)
	}

	upStart := 1
	downEnd := math.MaxInt
	for _, pm := range providedMigrations {
		// TODO: what about missing migrations?
		// thoughts: if a migration is missing, then we should stop and return an error

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
		if dbm.hash != pm.hash {
			downEnd = dbm.Id
			break
		}
	}

	if downEnd < math.MaxInt {
		err = runDownMigrations(m.config.Db, dbMigrations, downEnd)
		if err != nil {
			return fmt.Errorf("mig: error running down migrations: %w", err)
		}

		err = runUpMigrations(m.config.Db, providedMigrations, downEnd)
		if err != nil {
			return fmt.Errorf("mig: error running up migrations: %w", err)
		}
	} else {
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
			m.raw,
			m.hash,
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

func assignHashes(migrations []Migration) {
	for i := range migrations {
		migrations[i].hash = hashRaw(migrations[i].raw)
	}
}

func (mig *Mig) assignRaw(migrations []Migration) {
	for i := range migrations {
		migrations[i].raw = getRaw(migrations[i].Up, migrations[i].Down, mig.config.UpDelimiter, mig.config.DownDelimiter)
	}
}

func (mig *Mig) getMigrationsFromFS() ([]Migration, error) {
	result := []Migration{}

	entries, err := fs.ReadDir(mig.config.Fs, ".")
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

		contents, err := fs.ReadFile(mig.config.Fs, entry.Name())
		if err != nil {
			return nil, err
		}
		m.raw = string(contents)
		m.hash = hashRaw(m.raw)

		m.Up, m.Down, err = splitRaw(m.raw, mig.config.UpDelimiter, mig.config.DownDelimiter)
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

func (mig *Mig) getMigrationsFromDB() ([]Migration, error) {
	rows, err := mig.config.Db.Query("SELECT * FROM migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []Migration{}
	for rows.Next() {
		m := Migration{}
		err = rows.Scan(&m.Id, &m.FileName, &m.raw, &m.hash, &m.Up, &m.Down)
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
