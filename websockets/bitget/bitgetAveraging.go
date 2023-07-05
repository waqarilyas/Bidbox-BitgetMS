package bitget_websockets

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/jinzhu/gorm"

	"github.com/kryptomind/bidboxapi/bitgetms/models"
	"github.com/kryptomind/bidboxapi/bitgetms/utils"
)

type Ticker struct {
	// Define the structure of your ticker data here
	// Add fields as per your requirement
}

const (
	PERCENTAGE_PROFIT = 3.0
)

func HandleWebSocketMessages(conn *websocket.Conn, db *gorm.DB) {
	defer conn.Close()

	// tickerQueue := make(chan Snapshot)
	var wg sync.WaitGroup
	numWorkers := 10          //  number of worker goroutines
	numTickersPerWorker := 30 // number of tickers to process per worker
	tickerBuffer := make(chan Snapshot, numTickersPerWorker*numWorkers)

	// Start worker goroutines to process the tickers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processTicker(tickerBuffer, &wg, db)
	}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("WebSocket message receiving error:", err)
			return
		}

		var eventData Snapshot

		err = json.Unmarshal(message, &eventData)
		if err != nil {
			log.Println("WebSocket message parsing error:", err)
			continue
		}

		// Push the ticker to the buffer for processing
		tickerBuffer <- eventData
		if len(tickerBuffer) == numTickersPerWorker*numWorkers {
			// Buffer is full, wait for the current batch of tickers to be processed before accepting more
			wg.Wait()
		}
	}

	// Close the ticker buffer and wait for all goroutines to finish processing
	close(tickerBuffer)
	wg.Wait()
}

func processTicker(buffer <-chan Snapshot, wg *sync.WaitGroup, db *gorm.DB) {
	defer wg.Done()

	for ticker := range buffer {
		// Perform your operation on the ticker here
		// Example: Call the HandleMarketUpdate function
		HandleMarketUpdate(db, ticker)

	}
}

func HandleMarketUpdate(db *gorm.DB, ticker Snapshot) {

	tickerData := ticker.Data

	if len(tickerData) == 0 {
		return
	}

	splitted := strings.Split(ticker.Arg.InstID, "USDT")
	formattedCoinsymbol := "S" + splitted[0] + "SUSDT_SUMCBL"

	floatMarkPrice, err := strconv.ParseFloat(tickerData[0].MarkPrice, 64)
	if err != nil {
		fmt.Println("-- error converting ticker mark to float")
		return
	}

	if len(tickerData) > 0 {
		position := models.Positions{}
		positions, err := position.GetGroupedOpenPositionsByExchangeAndCoinSymbol(db, "bitget", formattedCoinsymbol)
		if err != nil {
			fmt.Println("---- unable to handle coin price event -----", err)
		}

		if len(positions) > 0 {

			for _, position := range positions {
				HandlePositionsOnTicker(floatMarkPrice, position)
			}
		}
	}

}

func HandlePositionsOnTicker(markPrice float64, positions []models.Positions) {
	if len(positions) != 2 {
		return
	}

	var longPos models.Positions
	var shortPos models.Positions

	for _, pos := range positions {

		if pos.Side == "long" {
			longPos = pos
		} else if pos.Side == "short" {
			shortPos = pos
		}
	}

	fmt.Println("🚀 ~ file: bitgetAveraging.go:119 ~ funcHandlePositionsOnTicker ~ longPos:", longPos)
	fmt.Println("🚀 ~ file: bitgetAveraging.go:121 ~ funcHandlePositionsOnTicker ~ shortPos:", shortPos)

	isLongInProfit, pnl, error := GetProfitPosition(longPos, shortPos, markPrice)

	if error != nil {
		fmt.Println("--- unable to get position profit ---", error)
		return
	}

	if isLongInProfit {
		// handle case in which long pos is in profit
	} else {

	}

	fmt.Println("🚀 ~ file: bitgetAveraging.go:136 ~ funcHandlePositionsOnTicker ~ pnl:", pnl)

}

func GetProfitPosition(longPos models.Positions, shortPos models.Positions, markPrice float64) (bool, float64, error) {
	shortPnl, shortError := GetPosPnl(shortPos, markPrice)
	if shortError != nil {
		return false, 0.0, shortError
	}

	longPnl, longError := GetPosPnl(longPos, markPrice)
	if longError != nil {
		return false, 0.0, longError
	}

	if longPnl > shortPnl {
		return true, longPnl, nil
	} else {
		return false, shortPnl, nil
	}

}

func GetPosPnl(position models.Positions, markPrice float64) (float64, error) {
	floatPosEntryPrice, error := strconv.ParseFloat(position.OpenPrice, 64)
	if error != nil {
		fmt.Println("-- error converting position entry price to float")
		return 0.0, error
	}

	floatPosSize, error := strconv.ParseFloat(position.OpenPrice, 64)
	if error != nil {
		fmt.Println("-- error converting position entry price to float")
		return 0.0, error

	}

	var positionPnl float64

	if position.Side == "long" {
		positionPnl = utils.CalculateLongPosFloatingPnL(floatPosEntryPrice, markPrice, floatPosSize)
	} else {
		positionPnl = utils.CalculateShortPosFloatingPnL(floatPosEntryPrice, markPrice, floatPosSize)
	}

	return positionPnl, nil

}

// func HandlePositionOnRateUpdate(ticker SnapshotData, position models.Positions, markProce float64) {

// 	floatPosEntryPrice, error := strconv.ParseFloat(position.OpenPrice, 64)
// 	if error != nil {
// 		fmt.Println("-- error converting position entry price to float")
// 	}

// 	floatPosSize, error := strconv.ParseFloat(position.OpenPrice, 64)
// 	if error != nil {
// 		fmt.Println("-- error converting position entry price to float")
// 	}

// 	// if position.Side == "long" {
// 	// 	handleLongPosition(position, positionPnl)
// 	// } else if position.Side == "short" {
// 	// 	positionPnl := utils.CalculateShortPosFloatingPnL(floatPosEntryPrice, floatMarkPrice, floatPosSize)
// 	// 	handleShortPosition(position, positionPnl)
// 	// }

// }

// func handleLongPosition(position models.Positions, pnl float64) {
// 	fmt.Println("🚀 ~ file: bitgetAveraging.go:130 ~ funchandleLongPosition ~ pnl:", pnl)

// 	if pnl > PERCENTAGE_PROFIT {
// 		//handle trade closing logic here
// 	}

// }

// func handleShortPosition(position models.Positions, pnl float64) {
// 	fmt.Println("🚀 ~ file: bitgetAveraging.go:135 ~ funchandleShortPosition ~ pnl:", pnl)

// 	if pnl > PERCENTAGE_PROFIT {
// 		//handle trade closing logic here

// 	}
// }
