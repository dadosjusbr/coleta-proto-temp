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

// parseFloat makes the string with format "xx.xx,xx" able to be parsed by the strconv.ParseFloat and return it parsed.
func parseFloat(emp []string, key, fileType string) (float64, error) {
	valueStr := emp[headersMap[fileType][key]]
	if valueStr == "" {
		return 0.0, nil
	} else {
		valueStr = strings.Trim(valueStr, " ")
		valueStr = strings.Replace(valueStr, ",", ".", 1)
		if n := strings.Count(valueStr, "."); n > 1 {
			valueStr = strings.Replace(valueStr, ".", "", n-1)
		}
	}
	return strconv.ParseFloat(valueStr, 64)
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
