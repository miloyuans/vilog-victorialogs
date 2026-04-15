package util

import "time"

func ParseTimeOrDefault(value string, fallback time.Time) (time.Time, error) {
	if value == "" {
		return fallback, nil
	}
	return time.Parse(time.RFC3339, value)
}
