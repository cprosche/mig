package mig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUninitialized(t *testing.T) {
	_, err := New(Config{})
	assert.NotNil(t, err)
}

func TestHash(t *testing.T) {
	strs := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)",
		"ljasldjflaskdjflaksjdfsdlfkjasdf",
		"hello world",
	}

	hashes := make(map[string]struct{})

	for _, str := range strs {
		h := hashRaw(str)
		assert.Equal(t, len(h), 10)
		_, ok := hashes[h]
		assert.False(t, ok)
	}

	hello1 := hashRaw("hello")
	hello2 := hashRaw("hello")

	assert.Equal(t, hello1, hello2)
}

// TODO: test getIntFromFilename
func TestGetIntFromFileName(t *testing.T) {
	t.Run("correctly gets int from filename", func(t *testing.T) {
		f1 := "0001_create_table_users.sql"
		got, err := getIntFromFileName(f1)
		assert.Nil(t, err)
		assert.Equal(t, got, 1)

		f2 := "12345_create_table_users.sql"
		got, err = getIntFromFileName(f2)
		assert.Nil(t, err)
		assert.Equal(t, got, 12345)

		f4 := "2_create_table_users.sql"
		got, err = getIntFromFileName(f4)
		assert.Nil(t, err)
		assert.Equal(t, got, 2)

		f5 := "0002: create_table_users.sql"
		got, err = getIntFromFileName(f5)
		assert.Nil(t, err)
		assert.Equal(t, got, 2)

		f7 := "03 - create_table_users.sql"
		got, err = getIntFromFileName(f7)
		assert.Nil(t, err)
		assert.Equal(t, got, 3)
	})

	t.Run("fails on invalid filename", func(t *testing.T) {
		f6 := "hello_create_table_users.sql"
		got, err := getIntFromFileName(f6)
		assert.NotNil(t, err)
		assert.Equal(t, got, 0)

		f3 := "0000_create_table_users.sql"
		got, err = getIntFromFileName(f3)
		assert.NotNil(t, err)
		assert.Equal(t, got, 0)
	})

}

func TestGetDelimiterIndex(t *testing.T) {
	t.Run("correctly finds delimiter index", func(t *testing.T) {
		raw := `
		-- up
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);

		-- down
		DROP TABLE users;
		`

		// for i, r := range raw {
		// 	t.Logf("%d: %s", i, strconv.Quote(string(r)))
		// }

		expected := 3
		got, err := findDelimiterIndex(raw, DEFAULT_UP_DELIMITER)
		assert.Nil(t, err)
		assert.Equal(t, expected, got)

		expected = 70
		got, err = findDelimiterIndex(raw, DEFAULT_DOWN_DELIMITER)
		assert.Nil(t, err)
		assert.Equal(t, expected, got)
	})

	t.Run("fails if delimiter not found", func(t *testing.T) {
		raw := `
		-- up
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		`

		_, err := findDelimiterIndex(raw, DEFAULT_DOWN_DELIMITER)
		assert.NotNil(t, err)
	})
}

func TestSplitRaw(t *testing.T) {
	expectedUp := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
	expectedDown := "DROP TABLE users;"
	raw := `-- up
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
-- down
DROP TABLE users;`

	up, down, err := splitRaw(raw, DEFAULT_UP_DELIMITER, DEFAULT_DOWN_DELIMITER)
	assert.Nil(t, err)
	assert.Equal(t, expectedUp, up)
	assert.Equal(t, expectedDown, down)

	raw = `-- down
DROP TABLE users;
-- up
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

	up, down, err = splitRaw(raw, DEFAULT_UP_DELIMITER, DEFAULT_DOWN_DELIMITER)
	assert.Nil(t, err)
	assert.Equal(t, expectedUp, up)
	assert.Equal(t, expectedDown, down)
}

func TestGetRaw(t *testing.T) {
	up := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
	down := "DROP TABLE users;"
	expected := `-- up
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
-- down
DROP TABLE users;`

	got := getRaw(up, down, DEFAULT_UP_DELIMITER, DEFAULT_DOWN_DELIMITER)
	assert.Equal(t, expected, got)
}
