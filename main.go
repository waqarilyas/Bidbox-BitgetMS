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

	c.AddFunc("@every 10m", tradesCron.Run) // Run Cron After Every 10 Minutes
	// ... add additional crons here

	c.Start()

	// Bitget Websocket Connection Logic
	bitget_WS.DB = server.DB
	bitget_WS.WebsocketTest()

	// statement := models.Statements{
	// 	UserEmail:   "kmtester@yopmail.com",
	// 	Exchange:    "bitget",
	// 	Symbol:      "BTCUSDT",
	// 	Side:        "long",
	// 	ClosedPnl:   3.14,
	// 	Size:        12,
	// 	PositionId:  12,
	// 	QuoteAmount: 3000.12,
	// 	ProfitUSD:   12,
	// }

	// decrypted, _ := helpers.DecryptStrings("9QEUQG18aBpZCHMaFpIqDtZv0LsMjhOyLmJhz8c801WlE35c8neqDYzCtpCBF8Niq4LUcEBFHGiCfm58y7MU")
	// fmt.Println("🚀 ~ file: main.go:77 ~ funcRun ~ decrypted:", decrypted)

	// statement.CreateNewStatement(server.DB)

	// bitget_websockets.HandleMarketUpdate(server.DB, bitget_websockets.Snapshot{
	// 	Action: "snapshot",
	// 	Arg: bitget_websockets.Subscription{
	// 		InstType: "mc",
	// 		Channel:  "ticker",
	// 		InstID:   "EOSUSDT",
	// 	},
	// 	Data: []bitget_websockets.SnapshotData{
	// 		{
	// 			MarkPrice: "0.6",
	// 		},
	// 	},
	// })

	// decryptRes,error:=utils.DecryptKeys()

	server.Run(":8080")
}

func main() {
	Run()

	for {
		time.Sleep(time.Second)
	}

}
