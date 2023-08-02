package crons_service

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/helpers"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
	"github.com/kryptomind/bidboxapi/bitgetms/utils"
	bitget_websockets "github.com/kryptomind/bidboxapi/bitgetms/websockets/bitget"
)

const (
	TAKE_PROFIT    = 2
	ALLOWED_LAYERS = 10.0
)

type GroupedData struct {
	Symbol            string
	ExchangePositions []utils.MarginData
	DatabasePositions []models.Positions
}

type emailPositions struct {
	email     string
	positions []models.Positions
}

func (server *TradesCron) RunProfitCron() {
	fmt.Println("---- profit cron running ----")

	position := models.Positions{}
	positions, err := position.GetOpenPositionsByExchange(server.DB, "bitget")
	if err != nil {
		// Proper error handling or logging here
		return
	}

	// Create a channel to receive emailPositions struct
	ch := make(chan emailPositions)

	// Fetch positions from the database concurrently
	go func() {
		for email, positions := range positionsByUserEmail(*positions) {
			ch <- emailPositions{email, positions}
		}
		close(ch)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ { // Choose an appropriate number of goroutines to run concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()
			for emailPos := range ch {
				// if emailPos.email != "kmtester@yopmail.com" {
				// 	continue
				// }
				handleUserPositions(server.DB, emailPos.email, emailPos.positions)
			}
		}()
	}

	wg.Wait()
}

func positionsByUserEmail(positions []models.Positions) map[string][]models.Positions {
	positionsByUserEmail := make(map[string][]models.Positions)
	for _, position := range positions {
		userEmail := position.UserEmail
		positionsByUserEmail[userEmail] = append(positionsByUserEmail[userEmail], position)
	}
	return positionsByUserEmail
}

func handleUserPositions(db *gorm.DB, email string, positions []models.Positions) {
	userKeys, keysErr := models.FindKeysByEmailandService(db, email, "bitget")
	if keysErr != nil {
		// Proper error handling or logging here
		return
	}

	apiKey, secretKey, passphrase, err := helpers.DecryptAllKeys(userKeys.ApiKey, userKeys.SecretKey, userKeys.Passphrase, "bitget")
	if err != nil {
		// Proper error handling or logging here
		return
	}

	_, userAllOpenPositions, openPosError := utils.PerformBitgetPositionQuery(apiKey, secretKey, passphrase, "")
	if openPosError != nil {
		// Proper error handling or logging here
		return
	}

	groupedDataMap := make(map[string]GroupedData)

	for _, position := range positions {
		coinSymbol := position.Symbol
		groupedData, found := groupedDataMap[coinSymbol]
		if !found {
			groupedData = GroupedData{Symbol: coinSymbol}
		}
		groupedData.DatabasePositions = append(groupedData.DatabasePositions, position)
		groupedDataMap[coinSymbol] = groupedData
	}

	for _, marginData := range userAllOpenPositions {
		coinSymbol := marginData.Symbol
		groupedData, found := groupedDataMap[coinSymbol]
		if !found {
			continue
		}
		groupedData.ExchangePositions = append(groupedData.ExchangePositions, marginData)
		groupedDataMap[coinSymbol] = groupedData
	}

	for _, groupedPos := range groupedDataMap {
		handleGroupedPos(db, groupedPos, apiKey, secretKey, passphrase)
	}
}

func handleGroupedPos(db *gorm.DB, groupedPos GroupedData, apiKey string, secretKey string, passphrase string) {
	exchangePositions := groupedPos.ExchangePositions
	databasePositions := groupedPos.DatabasePositions

	if len(exchangePositions) < 2 || len(databasePositions) < 2 {
		// Proper error handling or logging here
		return
	}

	exchangeLong := exchangePositions[1]
	exchangeShort := exchangePositions[0]

	if exchangePositions[0].HoldSide == "long" {
		exchangeLong = exchangePositions[0]
		exchangeShort = exchangePositions[1]
	}

	databaseLong := databasePositions[1]
	databaseShort := databasePositions[0]

	// general_websockets.SendEventOnEmail(exchangeLong, databaseLong.UserEmail)

	if databasePositions[0].Side == "long" {
		databaseLong = databasePositions[0]
		databaseShort = databasePositions[1]
	}

	isValid, isLongInProfit, pnl := isValidGroupPos(groupedPos, TAKE_PROFIT, exchangeLong, exchangeShort)
	if !isValid {
		return
	}

	fmt.Println("🚀 ~ file: takeProfit.cron.go:126 ~ funchandleGroupedPos ~ isBothProfitable, roe:", isLongInProfit, pnl)

	markPrice, _ := strconv.ParseFloat(exchangeLong.MarketPrice, 64)

	positionProfitUSD := 0.0
	if isLongInProfit {
		if databaseLong.Layer > 0 {
			bitget_websockets.CloseSymbolBothPositions(db, apiKey, secretKey, passphrase, databaseLong, databaseShort, markPrice, isLongInProfit, 0)
			return
		}

		// close Position in Profit
		_, profits, closePosError := bitget_websockets.CloseUserPosition(db, databaseLong, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		positionProfitUSD = profits

		_, openPosError := bitget_websockets.OpenUserPosition(db, databaseLong, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		if databaseShort.Layer < ALLOWED_LAYERS {
			_, avgPosError := bitget_websockets.AverageUserPosition(db, databaseShort, apiKey, secretKey, passphrase, markPrice)
			if avgPosError != nil {
				return
			}
			databaseShort.Layer += 1
		}

	} else {

		if databaseShort.Layer > 0 {
			bitget_websockets.CloseSymbolBothPositions(db, apiKey, secretKey, passphrase, databaseLong, databaseShort, markPrice, isLongInProfit, 0)
			return
		}

		_, profits, closePosError := bitget_websockets.CloseUserPosition(db, databaseShort, apiKey, secretKey, passphrase, markPrice)
		if closePosError != nil {
			return
		}

		positionProfitUSD = profits

		_, openPosError := bitget_websockets.OpenUserPosition(db, databaseShort, apiKey, secretKey, passphrase, markPrice)
		if openPosError != nil {
			return
		}

		if databaseLong.Layer < ALLOWED_LAYERS {
			_, avgPosError := bitget_websockets.AverageUserPosition(db, databaseLong, apiKey, secretKey, passphrase, markPrice)
			if avgPosError != nil {
				return
			}
		}

		databaseLong.Layer += 1

	}

	bitget_websockets.UpdateUserPositionsInDatabase(db, databaseLong, databaseShort, apiKey, secretKey, passphrase, databaseLong.Symbol, positionProfitUSD)

}

func isValidGroupPos(groupedPos GroupedData,
	takeProfit float64,
	longPos utils.MarginData,
	shortPos utils.MarginData,
) (isValid, isBothProfitable bool, profitROE float64) {
	exchangePositions := groupedPos.ExchangePositions

	if len(exchangePositions) < 2 {
		return false, false, 0
	}

	getROE := func(position utils.MarginData) float64 {
		margin, _ := strconv.ParseFloat(position.Margin, 64)
		percentageProfit := margin * float64(position.Leverage) * (takeProfit / 100.0)
		return percentageProfit
	}

	longROE := getROE(longPos)
	shortROE := getROE(shortPos)

	longPnl, _ := strconv.ParseFloat(longPos.UnrealizedPL, 64)
	shortPnl, _ := strconv.ParseFloat(shortPos.UnrealizedPL, 64)

	if longPnl < longROE && shortPnl < shortROE {
		return false, false, 0
	}

	if longPnl > shortPnl {
		return true, true, longPnl
	}

	return true, false, shortPnl
}
