package config

import (
	"log"
	"os"
)

var (
	ENV_SCOPE        string
	LOG_FILE_POS     string
	SIGNALFX_API_KEY string
)

func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		log.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}

// Initialize populates config variables.  Needed to prevent log.Fatal's when running unit tests.
func Initialize() {
	ENV_SCOPE = getEnv("ENV_SCOPE")
	LOG_FILE_POS = getEnv("LOG_FILE_POS")
	SIGNALFX_API_KEY = getEnv("SIGNALFX_API_KEY")
}
