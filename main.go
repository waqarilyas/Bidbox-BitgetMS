package main

import (
	"os"
	"time"

	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/joho/godotenv"
	"github.com/kryptomind/bidboxapi/bitgetms/controllers"
	crons_service "github.com/kryptomind/bidboxapi/bitgetms/crons"
	bitget_websockets "github.com/kryptomind/bidboxapi/bitgetms/websockets/bitget"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var server = controllers.Server{}
var bitget_WS = bitget_websockets.Server{}

func Init() {
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

}

func Run() {
	Init()

	c := cron.New()

	tradesCron := crons_service.TradesCron{}
	tradesCron.DB = server.DB

	c.AddFunc("@every 10m", tradesCron.Run)
	c.AddFunc("@every 10m", tradesCron.RunPositionsCron)
	c.AddFunc("@every 10s", tradesCron.RunProfitCron)
	c.AddFunc("@every 1m", tradesCron.RunOrdersCron)

	c.Start()

	bitget_WS.DB = server.DB

	server.Run(":8080")
}

func main() {
	Run()

	for {
		time.Sleep(time.Second)
	}

}
