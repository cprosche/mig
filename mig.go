package mig

import (
	"database/sql"
	"fmt"
	"io/fs"
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
	Fs              fs.FS
	OverrideDirName string

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
			id SERIAL PRIMARY KEY,
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

func New(c Config) (*Mig, error) {
	if c.UpDelimiter == "" {
		c.UpDelimiter = DEFAULT_UP_DELIMITER
	}
	if c.DownDelimiter == "" {
		c.DownDelimiter = DEFAULT_DOWN_DELIMITER
	}

	m := &Mig{
		config: c,
	}

	if m.config.Db == nil {
		return &Mig{}, fmt.Errorf("db is nil")
	}

	// Create migrations table if it doesn't exist
	_, err := m.config.Db.Exec(migrationTableSchema)
	if err != nil {
		return &Mig{}, fmt.Errorf("mig: error creating migrations table: %w", err)
	}

	// Get migrations from filesystem or from the provided slice
	if m.config.Fs != nil {
		m.config.Migrations, err = m.getMigrationsFromFS()
		if err != nil {
			return &Mig{}, fmt.Errorf("mig: error getting migrations from fs: %w", err)
		}
	}
	sort.Slice(m.config.Migrations, func(i, j int) bool {
		return m.config.Migrations[i].Id < m.config.Migrations[j].Id
	})

	m.assignRawAndHashes()
	return m, nil
}

func (mig *Mig) Migrate() error {
	mig.assignRawAndHashes()
	err := mig.runDown()
	if err != nil {
		return err
	}

	err = mig.runUp()
	if err != nil {
		return err
	}

	return nil
}

func (mig *Mig) assignRawAndHashes() {
	for i := range mig.config.Migrations {
		mig.config.Migrations[i].raw = getRaw(
			mig.config.Migrations[i].Up,
			mig.config.Migrations[i].Down,
			mig.config.UpDelimiter,
			mig.config.DownDelimiter,
		)
		mig.config.Migrations[i].hash = hashRaw(mig.config.Migrations[i].raw)
	}
}

func (mig *Mig) runUp() error {
	dbMigrations, err := mig.getMigrationsFromDB()
	if err != nil {
		return err
	}
	lastId := 0
	if len(dbMigrations) > 0 {
		lastId = dbMigrations[len(dbMigrations)-1].Id
	}

	for _, m := range mig.config.Migrations {
		if m.Id <= lastId {
			continue
		}

		_, err := mig.config.Db.Exec(m.Up)
		if err != nil {
			return err
		}

		_, err = mig.config.Db.Exec(
			`
		INSERT INTO 
			migrations (id, filename, raw, hash, up, down) 
		VALUES 
			($1, $2, $3, $4, $5, $6)`,
			m.Id,
			m.FileName,
			m.raw,
			m.hash,
			m.Up,
			m.Down,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// runDown finds if there are down migrations that need to be run and runs all migrations down to them
func (mig *Mig) runDown() error {
	dbMigrations, err := mig.getMigrationsFromDB()
	if err != nil {
		return err
	}

	// find any hash mismatches, and run down to the first one
	for i, dbMig := range dbMigrations {
		if i >= len(mig.config.Migrations) {
			continue
		}
		if dbMig.Id != mig.config.Migrations[i].Id {
			return fmt.Errorf(
				"mismatched migration id: dbMig.Id=%d, mig.config.Migrations[i].Id=%d",
				dbMig.Id,
				mig.config.Migrations[i].Id,
			)
		}
		if dbMig.hash != mig.config.Migrations[i].hash {
			return mig.runDownTo(dbMig.Id)
		}
	}

	// if there are more migrations in the db than in the slice, run down to the end of the slice
	if len(dbMigrations) > len(mig.config.Migrations) {
		lastId := mig.config.Migrations[len(mig.config.Migrations)-1].Id + 1
		return mig.runDownTo(lastId)
	}

	return nil
}

func (mig *Mig) runDownTo(endId int) error {
	dbMigrations, err := mig.getMigrationsFromDB()
	if err != nil {
		return fmt.Errorf("error getting migrations from db: %w", err)
	}

	for i := len(dbMigrations) - 1; i >= 0; i-- {
		if dbMigrations[i].Id < endId {
			break
		}

		// run down migration
		_, err := mig.config.Db.Exec(dbMigrations[i].Down)
		if err != nil {
			return fmt.Errorf("error running down migration: %w", err)
		}

		// remove migration from migrations table
		_, err = mig.config.Db.Exec(
			"DELETE FROM migrations WHERE id = $1",
			dbMigrations[i].Id,
		)
		if err != nil {
			return fmt.Errorf("error deleting migration from migrations table: %w", err)
		}
	}

	return nil
}

func (mig *Mig) getMigrationsFromFS() ([]Migration, error) {
	var (
		result  []Migration
		entries []fs.DirEntry
		err     error
	)

	if mig.config.OverrideDirName == "" {
		entries, err = fs.ReadDir(
			mig.config.Fs,
			".",
		)
		if err != nil {
			return nil, err
		}
	} else {
		entries, err = fs.ReadDir(
			mig.config.Fs,
			mig.config.OverrideDirName,
		)
		if err != nil {
			return nil, err
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		var (
			m        = Migration{}
			contents []byte
			err      error
		)

		m.FileName = entry.Name()
		m.Id, err = getIntFromFileName(m.FileName)
		if err != nil {
			return nil, err
		}

		if mig.config.OverrideDirName != "" {
			contents, err = fs.ReadFile(
				mig.config.Fs,
				fmt.Sprintf("%s/%s", mig.config.OverrideDirName, m.FileName),
			)
		} else {
			contents, err = fs.ReadFile(mig.config.Fs, m.FileName)
		}

		if err != nil {
			return nil, err
		}
		m.raw = string(contents)
		m.hash = hashRaw(m.raw)

		m.Up, m.Down, err = splitRaw(
			m.raw,
			mig.config.UpDelimiter,
			mig.config.DownDelimiter,
		)
		if err != nil {
			return nil, err
		}

		result = append(result, m)
	}

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
		err = rows.Scan(
			&m.Id,
			&m.FileName,
			&m.raw,
			&m.hash,
			&m.Up,
			&m.Down,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Id < result[j].Id
	})
	return result, nil
}
