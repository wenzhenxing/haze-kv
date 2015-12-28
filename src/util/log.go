package util

import (
	"log"
	"os"
)

var Logger *log.Logger

func InitLog() {
	file, err := os.Create("log.txt")
	if err != nil {
		log.Fatalf("%s\n", "log file error")
	}
	Logger = log.New(file, "", 0)
}
