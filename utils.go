package mig

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"unicode"
)

func findDelimiterIndex(raw, delimiter string) (int, error) {
	l := len(delimiter)
	for i := range raw {
		if i+l > len(raw) {
			break
		}

		if raw[i:i+l] == delimiter {
			return i, nil
		}
	}

	return 0, fmt.Errorf("mig: delimiter not found")
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

func splitRaw(raw, upDelimiter, downDelimiter string) (up string, down string, err error) {
	upStartIndex, err := findDelimiterIndex(raw, upDelimiter)
	if err != nil {
		return "", "", err
	}

	downStartIndex, err := findDelimiterIndex(raw, downDelimiter)
	if err != nil {
		return "", "", err
	}

	if upStartIndex < downStartIndex {
		up = strings.TrimSpace(raw[len(upDelimiter):downStartIndex])
		down = strings.TrimSpace(raw[downStartIndex+len(downDelimiter):])
	} else {
		up = strings.TrimSpace(raw[upStartIndex+len(upDelimiter):])
		down = strings.TrimSpace(raw[len(downDelimiter):upStartIndex])
	}

	return up, down, nil
}

func getRaw(up, down, upDelimiter, downDelimiter string) string {
	return upDelimiter + "\n" + up + "\n" + downDelimiter + "\n" + down
}

func hashRaw(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))

	result := fmt.Sprint(h.Sum32())

	// pad result with 0s to 10 characters
	for len(result) < 10 {
		result = "0" + result
	}

	return result
}
