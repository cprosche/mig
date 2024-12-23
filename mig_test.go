package mig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUninitialized(t *testing.T) {
	mig := New(Config{})
	err := mig.Migrate()
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
}
