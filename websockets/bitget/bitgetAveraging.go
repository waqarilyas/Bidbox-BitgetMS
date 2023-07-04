package bitget_websockets

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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

func HandleWebSocketMessages(conn *websocket.Conn, cache *Cache, db *gorm.DB) {
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

	if len(tickerData) > 0 {
		position := models.Positions{}
		positions, err := position.GetOpenPositionsByExchange(db, "bitget")
		if err != nil {
			fmt.Println("---- unable to handle coin price event -----", err)
		}

		for _, position := range *positions {
			HandlePositionOnRateUpdate(tickerData[0], position)
		}
	}

}

func HandlePositionOnRateUpdate(ticker SnapshotData, position models.Positions) {

	// compare position entry price with current market price
	floatMarkPrice, err := strconv.ParseFloat(ticker.MarkPrice, 64)
	if err != nil {
		fmt.Println("-- error converting ticker mark to float")
		return
	}

	floatPosEntryPrice, error := strconv.ParseFloat(position.OpenPrice, 64)
	if error != nil {
		fmt.Println("-- error converting position entry price to float")
	}

	floatPosSize, error := strconv.ParseFloat(position.OpenPrice, 64)
	if error != nil {
		fmt.Println("-- error converting position entry price to float")
	}

	if position.Side == "long" {
		positionPnl := utils.CalculateLongPosFloatingPnL(floatPosEntryPrice, floatMarkPrice, floatPosSize)
		handleLongPosition(position, positionPnl)
	} else if position.Side == "short" {
		positionPnl := utils.CalculateShortPosFloatingPnL(floatPosEntryPrice, floatMarkPrice, floatPosSize)
		handleShortPosition(position, positionPnl)
	}

}

func handleLongPosition(position models.Positions, pnl float64) {
	fmt.Println("🚀 ~ file: bitgetAveraging.go:130 ~ funchandleLongPosition ~ pnl:", pnl)

	if pnl > PERCENTAGE_PROFIT {
		//handle trade closing logic here
	}

}

func handleShortPosition(position models.Positions, pnl float64) {
	fmt.Println("🚀 ~ file: bitgetAveraging.go:135 ~ funchandleShortPosition ~ pnl:", pnl)

	if pnl > PERCENTAGE_PROFIT {
		//handle trade closing logic here

	}
}
