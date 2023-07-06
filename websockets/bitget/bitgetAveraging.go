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
	PERCENTAGE_PROFIT = 1
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

			for _, position := range positions {

				// if userEmail != "kmtester@yopmail.com" {
				// 	fmt.Println("---- user is not km tester ----")
				// 	return
				// }

				HandlePositionsOnTicker(floatMarkPrice, position, db, formattedCoinsymbol)
			}
		}
	}

}

func HandlePositionsOnTicker(markPrice float64, positions []models.Positions, db *gorm.DB, formattedCoinsymbol string) {
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

	positionProfitUSD := 0.0

	if isLongInProfit {

		if longPos.Layer >= ALLOWED_LAYERS {
			fmt.Println("--- num layers reached ----")
			return
		}

		openPriceFloat, convErr := strconv.ParseFloat(longPos.OpenPrice, 64)
		if convErr != nil {
			fmt.Println("--- unable to convert open price to float ----", convErr)
			return
		}

		positionProfitUSD = markPrice - openPriceFloat

		// close Position in Profit
		_, closePosError := CloseUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		_, openPosError := OpenUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		_, avgPosError := AverageUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
		if avgPosError != nil {
			return
		}

		shortPos.Layer += 1

	} else {

		if shortPos.Layer >= ALLOWED_LAYERS {
			fmt.Println("--- num layers reached ----")
			return
		}

		openPriceFloat, convErr := strconv.ParseFloat(shortPos.OpenPrice, 64)
		if convErr != nil {
			fmt.Println("--- unable to convert open price to float ----", convErr)
			return

		}

		positionProfitUSD = openPriceFloat - markPrice

		_, closePosError := CloseUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		_, openPosError := OpenUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		_, avgPosError := AverageUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
		if avgPosError != nil {
			return
		}

		longPos.Layer += 1

	}

	UpdateUserPositionsInDatabase(db, longPos, shortPos, apiKey, secretKey, passphrase, formattedCoinsymbol, positionProfitUSD)

	// fmt.Println("🚀 ~ file: bitgetAveraging.go:136 ~ funcHandlePositionsOnTicker ~ pnl:", pnl)

}

func CloseUserPosition(db *gorm.DB, position models.Positions, apiKey string, secretKey string, passphrase string, markPrice float64) (string, error) {
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

	floatSize, convErr := strconv.ParseFloat(closeOrderPayload.Size, 64)
	if convErr != nil {
		fmt.Println("--unable to convert size to float--")
	}

	quoteAmount := floatSize * markPrice

	dbOrder := models.Order{
		Email:       position.UserEmail,
		Symbol:      position.Symbol,
		MarginCoin:  closeOrderPayload.MarginCoin,
		Size:        position.Size,
		Side:        orderSide,
		OrderType:   closeOrderPayload.OrderType,
		Service:     position.Exchange,
		QuoteAmount: quoteAmount,
		Profit:      0.0,
		PositionId:  position.Id,
		OrderPrice:  fmt.Sprintf("%f", markPrice),
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---")
	}

	return "successfully closed position", nil
}

func OpenUserPosition(db *gorm.DB, position models.Positions, apiKey string, secretKey string, passphrase string, markPrice float64) (string, error) {
	orderSide := "open_long"
	if position.Side == "short" {
		orderSide = "open_short"
	}

	openOrderPayload := utils.NormalOrderRequest{
		MarginCoin: "SUSDT",
		Symbol:     position.Symbol,
		Size:       position.Size,
		Side:       orderSide,
		OrderType:  "market",
	}

	closePosResponse, closePosError := utils.PlaceBitgetOrder(apiKey, secretKey, passphrase, openOrderPayload)
	if closePosError != nil {
		fmt.Println(" --- unable to close position ---")
		fmt.Println(" --- user email ---", position.UserEmail)
		fmt.Println(" --- coinSymbol ---", position.Symbol)
		return "", closePosError
	}

	fmt.Sprintln("--- user position closed successfully ---", closePosResponse)

	floatSize, convErr := strconv.ParseFloat(openOrderPayload.Size, 64)
	if convErr != nil {
		fmt.Println("--unable to convert size to float--")
	}

	quoteAmount := floatSize * markPrice

	dbOrder := models.Order{
		Email:       position.UserEmail,
		Symbol:      position.Symbol,
		MarginCoin:  openOrderPayload.MarginCoin,
		Size:        position.Size,
		Side:        orderSide,
		OrderType:   openOrderPayload.OrderType,
		Service:     position.Exchange,
		QuoteAmount: quoteAmount,
		Profit:      0.0,
		PositionId:  position.Id,
		OrderPrice:  fmt.Sprintf("%f", markPrice),
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---")
	}

	return "successfully closed position", nil
}

func AverageUserPosition(db *gorm.DB, position models.Positions, apiKey string, secretKey string, passphrase string, markPrice float64) (string, error) {
	orderSide := "open_long"
	if position.Side == "short" {
		orderSide = "open_short"
	}

	closeOrderPayload := utils.NormalOrderRequest{
		MarginCoin: "SUSDT",
		Symbol:     position.Symbol,
		Size:       position.FirstBuyAmount,
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

	floatSize, convErr := strconv.ParseFloat(closeOrderPayload.Size, 64)
	if convErr != nil {
		fmt.Println("--unable to convert size to float--")
	}

	quoteAmount := floatSize * markPrice

	dbOrder := models.Order{
		Email:       position.UserEmail,
		Symbol:      position.Symbol,
		MarginCoin:  closeOrderPayload.MarginCoin,
		Size:        position.Size,
		Side:        orderSide,
		OrderType:   closeOrderPayload.OrderType,
		Service:     position.Exchange,
		QuoteAmount: quoteAmount,
		Profit:      0.0,
		PositionId:  position.Id,
		OrderPrice:  fmt.Sprintf("%f", markPrice),
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---")
	}

	return "successfully closed position", nil
}

func GetProfitPosition(db *gorm.DB, longPos models.Positions, shortPos models.Positions, markPrice float64) (bool, float64, error) {

	shortPnl := 0.0
	var shortError error

	longPnl := 0.0
	var longError error

	if shortPos.Layer > 0 {

		// handle greater layer pos here
	} else {
		shortPnl, shortError = GetPosPnl(shortPos, markPrice)
		if shortError != nil {
			return false, 0.0, shortError
		}
	}

	if longPos.Layer > 0 {
		positionOrders, orderError := models.GetOrdersByPositionIdAndSide(db, longPos.Id, "open_long")
		if orderError != nil {
			fmt.Println("--- unable to get position orders for pnl ---")
		}

		//handle greater layer POS here
	} else {
		longPnl, longError = GetPosPnl(longPos, markPrice)
		if longError != nil {
			return false, 0.0, longError
		}
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

func UpdateUserPositionsInDatabase(db *gorm.DB, longPos models.Positions, shortPos models.Positions, apiKey string, secretKey string, passphrase string, coinSymbol string, profit float64) {
	reqPositions, _, posError := utils.PerformBitgetPositionQuery(apiKey, secretKey, passphrase, coinSymbol)
	if posError != nil {
		fmt.Println("---- unable to get user updated positions from exchange ----")
	}

	for _, position := range reqPositions {
		currDbPos := longPos
		if position.HoldSide == "short" {
			currDbPos = shortPos
		}

		profits := currDbPos.TotalProfit + profit

		updatedPosition := models.Positions{
			Symbol:       position.Symbol,
			Leverage:     fmt.Sprintf("%d", position.Leverage),
			OpenPrice:    position.AverageOpenPrice,
			LiqPrice:     position.LiquidationPrice,
			UnrealizedPl: position.UnrealizedPL,
			MarkPrice:    position.MarketPrice,
			Size:         position.Available,
			Margin:       position.Margin,
			TotalProfit:  profits,
			Layer:        currDbPos.Layer,
		}

		updateErr := models.UpdatePositionByID(db, currDbPos.Id, updatedPosition)
		if updateErr != nil {
			fmt.Println("---- unable to update position in databaSe ----", updateErr)
		}

	}

}
