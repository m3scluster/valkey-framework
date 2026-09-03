package utils

import "os"

// Getenv gets an environment variable value or returns a fallback default.
func Getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

// LookupEnv reports an environment variable's value and whether it is set.
// It is useful when an explicitly empty value has meaning to the caller.
func LookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}
