package crons_service

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

func NewHistoricalPosSync() *TradesCron {
	return &TradesCron{}
}

func (server *TradesCron) RunHistoricalPosCron() {
	fmt.Println("--- positions cron started ---")

	var wg sync.WaitGroup

	// positions := models.Positions{}

	var positions models.Positions

	dbPositions, dbErr := positions.GetClosedPositionsByExchange(server.DB, "bitget")
	if dbErr != nil {
		fmt.Println("---- unable to get user positions from database ---", dbErr)
		return
	}

	for _, v := range *dbPositions {
		// wg.Add(1)

		handleClosedPosition(server.DB, v)

		// go func(v models.Positions) {
		// 	defer wg.Done()

		// 	handleClosedPosition(server.DB, v)
		// }(v)
	}

	wg.Wait()
}

// func handleClosedPosition(db *gorm.DB, position models.Positions) {

// 	positionOrders, orderError := models.GetOrdersByPositionId(db, position.Id)
// 	if orderError != nil {
// 		fmt.Println("--- unable to get position orders from database")
// 		return
// 	}

// 	totalProfit := 0.0
// 	totalSize := 0.0
// 	totalMargin := 0.0

// 	totalFee := 0.0

// 	closingOrder := "close_long"
// 	if position.Side == "short" {
// 		closingOrder = "close_short"
// 	}

// 	closePrice := 0.0

// 	if position.Layer > 0 {
// 		var biggestOrder models.Order
// 		biggestSize := 0.0

// 		for _, order := range positionOrders {
// 			floatSize, _ := strconv.ParseFloat(order.Size, 64)
// 			if floatSize > biggestSize {
// 				biggestSize = floatSize
// 				biggestOrder = *order
// 			}
// 		}

// 		floatPriceAvg, _ := strconv.ParseFloat(biggestOrder.OrderPrice, 64)
// 		closePrice = floatPriceAvg

// 	} else {
// 		for _, order := range positionOrders {
// 			if order.Side == closingOrder {
// 				floatPriceAvg, _ := strconv.ParseFloat(order.OrderPrice, 64)
// 				closePrice = floatPriceAvg
// 			}
// 		}

// 	}

// 	for _, ord := range positionOrders {
// 		totalProfit += ord.Profit
// 		floatSize, _ := strconv.ParseFloat(ord.Size, 64)
// 		totalSize += floatSize
// 		totalMargin += ord.QuoteAmount
// 		totalFee += math.Abs(ord.Fee)
// 	}

// 	totalProfit = totalProfit - totalFee
// 	updatePayload := models.Positions{
// 		TotalProfit:   totalProfit,
// 		TotalMargin:   totalMargin,
// 		TotalSize:     totalSize,
// 		AvgClosePrice: closePrice,
// 	}

// 	posUpdateErr := models.UpdatePositionByID(db, position.Id, updatePayload)
// 	if posUpdateErr != nil {
// 		fmt.Println("---- unable to update position in database ----", posUpdateErr)
// 		return
// 	}

// }

func handleClosedPosition(db *gorm.DB, position models.Positions) {
	positionOrders, orderError := models.GetOrdersByPositionId(db, position.Id)
	if orderError != nil {
		fmt.Println("--- unable to get position orders from database")
		return
	}

	totalProfit := 0.0
	totalSize := 0.0
	totalMargin := 0.0
	totalFee := 0.0
	closePrice := 0.0
	closingOrder := "close_long"

	if position.Side == "short" {
		closingOrder = "close_short"
	}

	if position.Layer > 0 {
		// var biggestOrder models.Order
		biggestSize := 0.0

		for _, order := range positionOrders {
			floatSize, _ := strconv.ParseFloat(order.Size, 64)
			if floatSize > biggestSize {
				biggestSize = floatSize
				closePrice, _ = strconv.ParseFloat(order.OrderPrice, 64)
			}
		}
	} else {
		var latestCreatedAt time.Time
		var latestClosedOrder *models.Order

		for _, order := range positionOrders {
			if order.Side == closingOrder {
				if latestClosedOrder == nil || order.CreatedAt.After(latestCreatedAt) {
					latestClosedOrder = order
					latestCreatedAt = order.CreatedAt
					closePrice, _ = strconv.ParseFloat(order.OrderPrice, 64)
				}
			}
		}
	}

	for _, ord := range positionOrders {
		totalProfit += ord.Profit
		floatSize, _ := strconv.ParseFloat(ord.Size, 64)
		totalSize += floatSize
		totalMargin += ord.QuoteAmount
		totalFee += math.Abs(ord.Fee)
	}

	totalProfit -= totalFee
	updatePayload := models.Positions{
		TotalProfit:   totalProfit,
		TotalMargin:   totalMargin,
		TotalSize:     totalSize,
		AvgClosePrice: closePrice,
	}

	fmt.Println("🚀 ~ file: HistoricalPosSync.cron.go:166 ~ funchandleClosedPosition ~ updatePayload:", updatePayload)
	posUpdateErr := models.UpdatePositionByID(db, position.Id, updatePayload)
	if posUpdateErr != nil {
		fmt.Println("---- unable to update position in database ----", posUpdateErr)
		return
	}
}
