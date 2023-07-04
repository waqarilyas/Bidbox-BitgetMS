package main

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/joho/godotenv"
	"github.com/kryptomind/bidboxapi/bitgetms/controllers"
	crons_service "github.com/kryptomind/bidboxapi/bitgetms/crons"
	bitget_websockets "github.com/kryptomind/bidboxapi/bitgetms/websockets/bitget"
	"github.com/robfig/cron/v3"
)

var server = controllers.Server{}
var bitget_WS = bitget_websockets.Server{}

func Run() {
	err := godotenv.Load()
	log := logrus.New()

	log.SetFormatter(&nested.Formatter{
		HideKeys:    true,
		FieldsOrder: []string{"file", "function"},
	})
	if err != nil {
		log.WithFields(logrus.Fields{
			"file":     "main.go",
			"function": "Run",
		}).Fatal("Error getting env")
	} else {
		log.WithFields(logrus.Fields{
			"file":     "main.go",
			"function": "Run",
		}).Info("Getting Values")
	}

	server.Initialize(os.Getenv("DB_DRIVER"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_PORT"), os.Getenv("DB_HOST"), os.Getenv("DB_NAME"))

	c := cron.New()

	tradesCron := crons_service.TradesCron{}
	tradesCron.DB = server.DB

	c.AddFunc("@every 10m", tradesCron.Run) // Run Cron After Every 10 Minutes
	// ... add additional crons here

	c.Start()

	// Bitget Websocket Connection Logic
	bitget_WS.DB = server.DB
	// bitget_WS.WebsocketTest()
	server.Run(":8080")
}

func main() {
	Run()

	for {
		time.Sleep(time.Second)
	}
}
