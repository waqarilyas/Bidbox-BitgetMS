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
	PERCENTAGE_PROFIT = 0.5
	ALLOWED_LAYERS    = 2
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

			for userEmail, position := range positions {
				fmt.Println("---- user email ---", userEmail)

				if userEmail != "kmtester@yopmail.com" {
					fmt.Println("---- user is not km tester ----")
					return
				}

				HandlePositionsOnTicker(floatMarkPrice, position, db)
			}
		}
	}

}

func HandlePositionsOnTicker(markPrice float64, positions []models.Positions, db *gorm.DB) {
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

	isLongInProfit, pnl, error := GetProfitPosition(longPos, shortPos, markPrice)
	if error != nil {
		fmt.Println("--- unable to get position profit ---", error)
		return
	}

	if pnl < PERCENTAGE_PROFIT {
		fmt.Println("---- position profit is less than the percentage profit set by admin ----", pnl)
		return
	}

	userKeys, error := models.FindKeysByEmailandService(db, longPos.UserEmail, longPos.Exchange)
	if error != nil {
		fmt.Println("--- user keys not found ---", longPos.UserEmail)
		return
	}

	apiKey, secretKey, passphrase, decryptError := utils.DecryptKeys(userKeys.ApiKey, userKeys.SecretKey, userKeys.Passphrase, "bitget")
	if decryptError != nil {
		fmt.Println("--- error decrypting user keys ---", longPos.UserEmail)
		return
	}

	if isLongInProfit {

		if longPos.Layer >= ALLOWED_LAYERS {
			fmt.Println("--- num layers reached ----")
			return
		}

		fmt.Println("--- long position is in profit ---", pnl)

		_, closePosError := CloseUserPosition(db, longPos, apiKey, secretKey, passphrase)
		if closePosError != nil {
			return
		}

		// handle case in which long pos is in profit
	} else {

		if shortPos.Layer >= ALLOWED_LAYERS {
			fmt.Println("--- num layers reached ----")
			return
		}

		_, closePosError := CloseUserPosition(db, shortPos, apiKey, secretKey, passphrase)
		if closePosError != nil {
			return
		}

		fmt.Println("--- short position is in profit ---", longPos)

		// handle the case in which short is in profit
	}

	// fmt.Println("🚀 ~ file: bitgetAveraging.go:136 ~ funcHandlePositionsOnTicker ~ pnl:", pnl)

}

func CloseUserPosition(db *gorm.DB, position models.Positions, apiKey string, secretKey string, passphrase string) (string, error) {

	orderSide := "close_long"
	if position.Side == "short" {
		orderSide = "close_short"
	}

	closeOrderPayload := utils.NormalOrderRequest{
		MarginCoin: "SUSDT",
		Symbol:     position.Symbol,
		Size:       position.Size,
		Side:       orderSide,
		OrderType:  "market",
	}

	closePosResponse, closePosError := utils.PlaceBitgetOrder(apiKey, secretKey, passphrase, closeOrderPayload)
	if closePosError != nil {
		fmt.Println(" --- unable to close position ---")
		fmt.Println(" --- user email ---", position.UserEmail)
		fmt.Println(" --- coinSymbol ---", position.Symbol)
		return "", closePosError
	}

	fmt.Sprintln("--- user position closed successfully ---", closePosResponse)

	return "successfully closed position", nil
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
		positionPnl = utils.CalculateLongPosFloatingPnLPercentage(floatPosEntryPrice, markPrice, floatPosSize)
	} else {
		positionPnl = utils.CalculateShortPosFloatingPnLPercentage(floatPosEntryPrice, markPrice, floatPosSize)
	}

	return positionPnl, nil
}
