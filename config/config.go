package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type DSNConnection struct {
	Host string
	Port string
	User string
	Pass string
	Name string
}

type Cfg struct {
	Username    string
	Password    string
	Sasl        bool
	WorkerCount int
	BatchSize   int
	Topics      []string
	GroupID     string
	Conn        DSNConnection
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}
}

func LoadConfigFromEnv() Cfg {
	return Cfg{
		Username:    os.Getenv("KAFKA_USERNAME"),
		Password:    os.Getenv("KAFKA_PASSWORD"),
		Sasl:        os.Getenv("KAFKA_SASL") == "true",
		WorkerCount: getEnvAsInt("WORKER_COUNT", 1),
		BatchSize:   getEnvAsInt("BATCH_SIZE", 100),
		Topics:      getEnvAsSlice("KAFKA_TOPICS", ","),
		GroupID:     os.Getenv("KAFKA_GROUP_ID"),
		Conn: DSNConnection{
			Host: os.Getenv("DB_HOST"),
			Port: os.Getenv("DB_PORT"),
			User: os.Getenv("DB_USER"),
			Pass: os.Getenv("DB_PASS"),
			Name: os.Getenv("DB_NAME"),
		},
	}
}

func getEnvAsInt(name string, defaultVal int) int {
	if value, exists := os.LookupEnv(name); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultVal
}

func getEnvAsSlice(name string, sep string) []string {
	if value, exists := os.LookupEnv(name); exists {
		return strings.Split(value, sep)
	}
	return []string{}
}
