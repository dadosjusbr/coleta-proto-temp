package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// logError prints to Stderr
func logError(format string, args ...interface{}) {
	time := fmt.Sprintf("%s: ", time.Now().Format(time.RFC3339))
	fmt.Fprintf(os.Stderr, time+format+"\n", args...)
}

func retrieveString(emp []string, key string, fileType string) string {
	return emp[headersMap[fileType][key]]
}

func retrieveFloat64(v interface{}, emp []string, key string, fileType string) error {
	var err error
	var value float64
	valueStr := retrieveString(emp, key, fileType)
	if valueStr == "" {
		value = 0.0
	} else {
		value, err = parseFloat(valueStr)
		if err != nil {
			return fmt.Errorf("error retrieving float %s from %v: %q", key, emp, err)
		}
	}

	if v, ok := v.(**float64); ok {
		*v = &value
		return nil
	}
	if v, ok := v.(*float64); ok {
		*v = value
		return nil
	}
	return fmt.Errorf("error retrieving float %s: v must be *float64 or **float64", key)
}

// parseFloat makes the string with format "xx.xx,xx" able to be parsed by the strconv.ParseFloat and return it parsed.
func parseFloat(s string) (float64, error) {
	s = strings.Trim(s, " ")
	s = strings.Replace(s, ",", ".", 1)
	if n := strings.Count(s, "."); n > 1 {
		s = strings.Replace(s, ".", "", n-1)
	}
	return strconv.ParseFloat(s, 64)
}

// cleanStrings makes all strings to uppercase and removes N/D fields
func cleanStrings(raw [][]string) [][]string {
	for row := range raw {
		for col := range raw[row] {
			raw[row][col] = strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(raw[row][col], "N/D", ""), "\n", " "))
			raw[row][col] = strings.ReplaceAll(raw[row][col], "  ", " ")
		}
	}
	return raw
}
