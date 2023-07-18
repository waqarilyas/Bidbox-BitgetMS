package bitget_websockets

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"math"

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
	PERCENTAGE_PROFIT = 1.0
	ALLOWED_LAYERS    = 10
)

func HandleWebSocketMessages(conn *websocket.Conn, db *gorm.DB) {
	defer conn.Close()

	var wg sync.WaitGroup
	numWorkers := 100         //  number of worker goroutines
	numTickersPerWorker := 10 // number of tickers to process per worker
	tickerBuffer := make(chan Snapshot, numTickersPerWorker*numWorkers)

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
			return
		}

		if len(positions) > 0 {

			for _, position := range positions {

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

	isLongInProfit, pnl, error := GetProfitPosition(db, longPos, shortPos, markPrice)
	if error != nil {
		fmt.Println("--- unable to get position profit ---", error)
		return
	}

	if pnl < PERCENTAGE_PROFIT {
		fmt.Println(pnl, "--- profit is less than defined by admin ---")
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

		if longPos.Layer > 0 {

			floatSize, err := strconv.ParseFloat(longPos.Size, 64)
			if err != nil {
				fmt.Println(" -- unable to parse size to float ---")
				return
			}

			positionProfitUSD = pnl * floatSize * markPrice
			CloseSymbolBothPositions(db, apiKey, secretKey, passphrase, longPos, shortPos, markPrice, isLongInProfit, positionProfitUSD)
			return
		}

		// close Position in Profit
		_, profits, closePosError := CloseUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		positionProfitUSD = profits

		_, openPosError := OpenUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		if shortPos.Layer < ALLOWED_LAYERS {
			_, avgPosError := AverageUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
			if avgPosError != nil {
				return
			}
		}

		shortPos.Layer += 1

	} else {

		if shortPos.Layer > 0 {
			// close whole position because the counter position is in profit
			floatSize, err := strconv.ParseFloat(shortPos.Size, 64)
			if err != nil {
				fmt.Println(" -- unable to parse size to float ---")
				return
			}

			positionProfitUSD = pnl * floatSize * markPrice
			CloseSymbolBothPositions(db, apiKey, secretKey, passphrase, longPos, shortPos, markPrice, isLongInProfit, positionProfitUSD)
			return
		}

		_, profits, closePosError := CloseUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		positionProfitUSD = profits

		_, openPosError := OpenUserPosition(db, shortPos, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		if longPos.Layer < ALLOWED_LAYERS {
			_, avgPosError := AverageUserPosition(db, longPos, apiKey, secretKey, passphrase, markPrice)
			if avgPosError != nil {
				return
			}
		}

		longPos.Layer += 1

	}

	UpdateUserPositionsInDatabase(db, longPos, shortPos, apiKey, secretKey, passphrase, formattedCoinsymbol, positionProfitUSD)

	// fmt.Println("🚀 ~ file: bitgetAveraging.go:136 ~ funcHandlePositionsOnTicker ~ pnl:", pnl)

}

func CloseUserPosition(db *gorm.DB, position models.Positions, apiKey string, secretKey string, passphrase string, markPrice float64) (string, float64, error) {
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
		return "", 0.0, closePosError
	}

	orderDetails, detailsError := utils.BitgetOrderDetails(apiKey, secretKey, passphrase, position.Symbol, closePosResponse.Data.OrderID)
	if detailsError != nil {
		fmt.Println("----= unable to get oprder details after closing position ----", detailsError)
	}

	dbOrder := models.Order{
		Email:       position.UserEmail,
		Symbol:      position.Symbol,
		MarginCoin:  closeOrderPayload.MarginCoin,
		Size:        position.Size,
		Side:        orderSide,
		OrderType:   closeOrderPayload.OrderType,
		Service:     position.Exchange,
		Profit:      0.0,
		PositionId:  position.Id,
		QuoteAmount: orderDetails.Data.FilledAmount,
		OrderPrice:  fmt.Sprintf("%f", orderDetails.Data.PriceAvg),
		Fee:         orderDetails.Data.Fee,
		OrderId:     orderDetails.Data.OrderID,
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---", saveErr)
	}

	// statements saving logic here

	CreateStatement(db, position, orderDetails)

	//statements logic ends here

	posOpenPrice, openErr := strconv.ParseFloat(position.OpenPrice, 64)
	if openErr != nil {
		fmt.Println("---- err converting open price to float ---")
	}

	positionSize, sizeErr := strconv.ParseFloat(position.Size, 64)
	if sizeErr != nil {
		fmt.Println("---- err converting open price to float ---")
	}

	totalProfits := 0.0

	if position.Side == "long" {
		totalProfits = (orderDetails.Data.PriceAvg - posOpenPrice) * float64(positionSize)
	} else if position.Side == "short" {
		totalProfits = (posOpenPrice - orderDetails.Data.PriceAvg) * float64(positionSize)
	}

	return "successfully closed position", totalProfits, nil
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

	orderDetails, detailsError := utils.BitgetOrderDetails(apiKey, secretKey, passphrase, position.Symbol, closePosResponse.Data.OrderID)
	if detailsError != nil {
		fmt.Println("----= unable to get oprder details after closing position ----", detailsError)
	}

	fmt.Sprintln("--- user position closed successfully ---", closePosResponse)

	orderPrice := orderDetails.Data.PriceAvg
	if orderPrice == 0 {
		orderPrice = markPrice
	}

	dbOrder := models.Order{
		Email:      position.UserEmail,
		Symbol:     position.Symbol,
		MarginCoin: openOrderPayload.MarginCoin,
		Size:       position.Size,
		Side:       orderSide,
		OrderType:  openOrderPayload.OrderType,
		Service:    position.Exchange,

		Profit:     0.0,
		PositionId: position.Id,

		QuoteAmount: orderDetails.Data.FilledAmount,
		OrderPrice:  fmt.Sprintf("%f", orderPrice),
		Fee:         orderDetails.Data.Fee,
		OrderId:     orderDetails.Data.OrderID,
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---", saveErr)
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

	orderDetails, detailsError := utils.BitgetOrderDetails(apiKey, secretKey, passphrase, position.Symbol, closePosResponse.Data.OrderID)
	if detailsError != nil {
		fmt.Println("----= unable to get oprder details after closing position ----", detailsError)
	}

	orderPrice := orderDetails.Data.PriceAvg
	if orderPrice == 0 {
		orderPrice = markPrice
	}

	fmt.Sprintln("--- user position closed successfully ---", closePosResponse)

	dbOrder := models.Order{
		Email:       position.UserEmail,
		Symbol:      position.Symbol,
		MarginCoin:  closeOrderPayload.MarginCoin,
		Size:        position.Size,
		Side:        orderSide,
		OrderType:   closeOrderPayload.OrderType,
		Service:     position.Exchange,
		Profit:      0.0,
		PositionId:  position.Id,
		QuoteAmount: orderDetails.Data.FilledAmount,
		OrderPrice:  fmt.Sprintf("%f", orderPrice),
		Fee:         orderDetails.Data.Fee,
		OrderId:     orderDetails.Data.OrderID,
	}

	_, saveErr := dbOrder.SaveOrder(db)
	if saveErr != nil {
		fmt.Println("---- unabel to save order in database ---", saveErr)
	}

	return "successfully closed position", nil
}

func GetProfitPosition(db *gorm.DB, longPos models.Positions, shortPos models.Positions, markPrice float64) (bool, float64, error) {

	shortPnl := 0.0
	var shortError error

	longPnl := 0.0
	var longError error

	if shortPos.Layer > 0 {
		positionOrders, orderError := models.GetOrdersByPositionIdAndSide(db, shortPos.Id, "open_short")
		if orderError != nil {
			fmt.Println("--- unable to get position orders for pnl ---")
		}

		var orders []models.Order
		for _, order := range positionOrders {
			orders = append(orders, *order)
		}

		shortPnl = utils.CalculateShortLayeredPnl(orders, markPrice)
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

		var orders []models.Order
		for _, order := range positionOrders {
			orders = append(orders, *order)
		}

		longPnl = utils.CalculateLongLayeredPnl(orders, markPrice)

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

	floatPosSize, error := strconv.ParseFloat(position.Size, 64)
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
		}
		incr := 0
		if currDbPos.Layer > 0 {
			incr = 1
		}
		updateErr := models.UpdateAveragingPosition(db, currDbPos.Id, updatedPosition, incr)
		if updateErr != nil {
			fmt.Println("---- unable to update position in databaSe ----", updateErr)
		}

	}

}

func CloseSymbolBothPositions(
	db *gorm.DB,
	apiKey string,
	secretKey string,
	passphrase string,
	longPos models.Positions,
	shortPos models.Positions,
	markPrice float64,
	isLongInProfit bool,
	profits float64,
) {

	longOrder := utils.OrderRequest{
		Size:      longPos.Size,
		Side:      "close_long",
		OrderType: "Market",
	}

	shortOrder := utils.OrderRequest{
		Size:      shortPos.Size,
		Side:      "close_short",
		OrderType: "Market",
	}

	batchOrderRequest := utils.BitgetBatchOrderRequest{
		Symbol:        longPos.Symbol,
		MarginCoin:    "SUSDT",
		OrderDataList: []utils.OrderRequest{longOrder, shortOrder},
	}

	batchOrdersResponse, batchError := utils.PlaceBitgetBatchOrder(apiKey, secretKey, passphrase, &batchOrderRequest)
	if len(batchOrdersResponse.Data.Failure) > 0 {
		fmt.Println("--- error closing positions ---", batchError)
		return
	}

	orderIds := batchOrdersResponse.Data.OrderInfo

	var longOrderDetails utils.OrderDetailsResponse
	var firstOrderError error
	var shortOrderDetails utils.OrderDetailsResponse
	var secondOrderError error

	longOrderDetails, firstOrderError = utils.BitgetOrderDetails(apiKey, secretKey, passphrase, shortPos.Symbol, orderIds[0].OrderID)
	if firstOrderError != nil {
		fmt.Println("----- first order details api has failed -----")
	}

	shortOrderDetails, secondOrderError = utils.BitgetOrderDetails(apiKey, secretKey, passphrase, shortPos.Symbol, orderIds[1].OrderID)
	if secondOrderError != nil {
		fmt.Println("--- second order details api has failed ----")
	}

	longTotalProfit := longOrderDetails.Data.TotalProfits - math.Abs(longOrderDetails.Data.Fee)
	shortTotalProfits := shortOrderDetails.Data.TotalProfits - math.Abs(shortOrderDetails.Data.Fee)

	if isLongInProfit {
		longTotalProfit = profits
	} else {
		shortTotalProfits = profits
	}

	longOrderPrice := longOrderDetails.Data.PriceAvg
	if longOrderPrice == 0 {
		longOrderPrice = markPrice
	}

	shortOrderPrice := shortOrderDetails.Data.PriceAvg
	if shortOrderPrice == 0 {
		shortOrderPrice = markPrice
	}

	ordersPayload := []*models.Order{
		{
			Email:       longPos.UserEmail,
			Symbol:      longPos.Symbol,
			MarginCoin:  "SUSDT",
			Size:        longOrder.Size,
			Side:        longOrder.Side,
			OrderType:   longOrder.OrderType,
			Service:     "bitget",
			QuoteAmount: longOrderDetails.Data.FilledAmount,
			Profit:      0.0,
			PositionId:  longPos.Id,
			OrderPrice:  fmt.Sprintf("%f", longOrderPrice),
			Fee:         longOrderDetails.Data.Fee,
			OrderId:     longOrderDetails.Data.OrderID,
		},
		{
			Email:       shortPos.UserEmail,
			Symbol:      shortPos.Symbol,
			MarginCoin:  "SUSDT",
			Size:        longOrder.Size,
			Side:        shortOrder.Side,
			OrderType:   shortOrder.OrderType,
			Service:     "bitget",
			QuoteAmount: shortOrderDetails.Data.FilledAmount,
			Profit:      0.0,
			PositionId:  shortPos.Id,
			OrderPrice:  fmt.Sprintf("%f", shortOrderPrice),
			Fee:         shortOrderDetails.Data.Fee,
			OrderId:     shortOrderDetails.Data.OrderID,
		},
	}

	//statements saving logic starts here
	CreateStatement(db, longPos, longOrderDetails)
	CreateStatement(db, shortPos, shortOrderDetails)

	//statements logic end here

	_, err := models.SaveMultipleOrders(db, ordersPayload)
	if err != nil {
		fmt.Println("--- Unable to save close orders in database ---", err)
		return
	}

	longPosOrders, orderError := models.GetOrdersByPositionIdAndSide(db, longPos.Id, "open_long")
	if orderError != nil {
		fmt.Println("--- unable to get position orders for pnl ---")
	}

	shortPosOrders, orderError := models.GetOrdersByPositionIdAndSide(db, shortPos.Id, "open_long")
	if orderError != nil {
		fmt.Println("--- unable to get position orders for pnl ---")
	}

	totalLongSize := 0.0
	totalLongMargin := 0.0

	totalShortSize := 0.0
	totalShortMargin := 0.0

	for _, order := range longPosOrders {
		totalLongMargin += order.QuoteAmount
		floatSize, err := strconv.ParseFloat(order.Size, 64)
		if err != nil {
			fmt.Println("--- unable to convert order size to float ---")
		}
		totalLongSize += floatSize
	}

	for _, order := range shortPosOrders {
		totalShortMargin += order.QuoteAmount
		floatSize, err := strconv.ParseFloat(order.Size, 64)
		if err != nil {
			fmt.Println("--- unable to convert order size to float ---")
		}
		totalShortSize += floatSize
	}

	updatedPositions := []models.Positions{
		{
			Id:           longPos.Id,
			Symbol:       longPos.Symbol,
			UnrealizedPl: fmt.Sprintf("%f", longOrderDetails.Data.TotalProfits),
			MarkPrice:    fmt.Sprintf("%f", markPrice),
			Size:         fmt.Sprintf("%f", totalLongSize),
			Margin:       fmt.Sprintf("%f", totalLongMargin),
			Status:       "closed",
			TotalProfit:  longTotalProfit,
			Fee:          longOrderDetails.Data.Fee,
		},
		{
			Id:           shortPos.Id,
			Symbol:       shortPos.Symbol,
			UnrealizedPl: fmt.Sprintf("%f", shortOrderDetails.Data.TotalProfits),
			MarkPrice:    fmt.Sprintf("%f", markPrice),
			Size:         fmt.Sprintf("%f", totalShortSize),
			Margin:       fmt.Sprintf("%f", totalShortMargin),
			Status:       "closed",
			TotalProfit:  shortTotalProfits,
			Fee:          shortOrderDetails.Data.Fee,
		},
	}

	for _, position := range updatedPositions {
		updateErr := models.UpdatePositionByID(db, position.Id, position)
		if updateErr != nil {
			fmt.Println("---- unable to update position in databaSe ----", updateErr)
			continue
		}

		fmt.Println("--- position updated in database succesfully ---")
	}

}

func CreateStatement(db *gorm.DB, position models.Positions, orderDetails utils.OrderDetailsResponse) *models.Statements {
	floatSize, _ := strconv.ParseFloat(position.Size, 64)
	dbStatement := models.Statements{
		UserEmail:   position.UserEmail,
		Exchange:    position.Exchange,
		Symbol:      position.Symbol,
		Side:        position.Side,
		ClosedPnl:   orderDetails.Data.TotalProfits,
		Size:        floatSize,
		PositionId:  position.Id,
		QuoteAmount: floatSize * orderDetails.Data.PriceAvg,
		ProfitUSD:   position.TotalProfit - position.Fee,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
	}

	var statementRes *models.Statements

	if dbStatement.ProfitUSD > 0 {
		var statementErr error
		statementRes, statementErr = dbStatement.CreateNewStatement(db)
		if statementErr != nil {
			fmt.Println("---- error saving user statement in database ----", statementErr)
			return nil
		}
		fmt.Println("🚀 ~ file: bitgetAveraging.go:731 ~ funcCreateStatement ~ statementRes:", statementRes)
	} else {
		fmt.Println("---- statement not saved because total profit is  ----", orderDetails.Data.TotalProfits, " ---- and fee is ---", orderDetails.Data.Fee)
	}

	return statementRes

}
