package crons_service

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/helpers"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
	"github.com/kryptomind/bidboxapi/bitgetms/utils"
)

func NewOrdersSyncCron() *TradesCron {
	return &TradesCron{}
}

func (server *TradesCron) RunOrdersCron() {
	fmt.Println("--- Orders cron started ---")

	var wg sync.WaitGroup

	key := models.Key{}
	keys, err := key.FindKeysByService(server.DB, "bitget")
	if err != nil {
		log.Fatal("error getting keys")
		return
	}

	orders, ordError := models.GetUnhandledOrdersByExchange(server.DB, "bitget")

	if ordError != nil {
		fmt.Println("--- unable to handle orders at the moment ---")
		return
	}

	for _, order := range orders {

		if order.Email == "kmtester@yopmail.com" {
			continue
		}

		handleUnhandledOrders(server.DB, *order, *keys)
	}

	wg.Wait()
}

func handleUnhandledOrders(db *gorm.DB, order models.Order, keys []models.Key) {

	var key models.Key

	for _, user := range keys {
		if user.UserEmail == order.Email {
			key = user
		}
	}

	apiKey, secretKey, passphrase, err := helpers.DecryptAllKeys(key.ApiKey, key.SecretKey, key.Passphrase, "bitget")
	if err != nil {
		fmt.Println(order.Email, "--- error in decryptkeys: ", err)
		return
	}

	response, orderError := utils.BitgetOrderDetails(apiKey, secretKey, passphrase, order.Symbol, order.OrderId)
	if orderError != nil {
		fmt.Println("----- error getting order details from exchange ----", orderError)
		return
	}

	orderDetails := response.Data

	if orderDetails.State == "init" || orderDetails.State == "new" || orderDetails.State == "partially_filled" {
		return
	}

	dbOrder := models.Order{
		Size:        fmt.Sprintf("%f", orderDetails.Size),
		Side:        orderDetails.Side,
		QuoteAmount: orderDetails.FilledAmount,
		Profit:      orderDetails.TotalProfits,
		OrderPrice:  fmt.Sprintf("%f", orderDetails.PriceAvg),
		Fee:         orderDetails.Fee,
		IsHandled:   true,
	}

	updateErr := models.UpdateOrderById(db, order.Id, dbOrder)
	if updateErr != nil {
		fmt.Println("---- error updating order in database ---", updateErr)
		return
	}

	if orderDetails.Side == "close_long" || orderDetails.Side == "close_short" {
		stateRes := CreateStatement(db, order, orderDetails)
		fmt.Println("🚀 ~ file: ordersSync.cron.go:100 ~ funchandleUnhandledOrders ~ stateRes:", stateRes)

		var dbPosition models.Positions
		databasePosition, posError := dbPosition.GetPositionById(db, order.PositionId)
		if posError != nil {
			return
		}

		if databasePosition.Status == "closed" {
			//handle profit calculation here
			orderSide := "close_short"
			if databasePosition.Side == "long" {
				orderSide = "close_long"
			}

			positionOrders, orderError := models.GetOrdersByPositionId(db, order.PositionId)
			if orderError != nil {
				fmt.Println("--- unable to get position orders from database")
				return
			}

			totalProfit := 0.0
			totalSize := 0.0
			totalMargin := 0.0
			totalOpenFee := 0.0
			totalCloseFee := 0.0

			for _, ord := range positionOrders {
				if ord.Side == orderSide {
					totalCloseFee += math.Abs(ord.Fee)
					totalProfit += ord.Profit
					floatSize, _ := strconv.ParseFloat(ord.Size, 64)
					totalSize += floatSize
					totalMargin += ord.QuoteAmount
				} else {
					totalOpenFee += math.Abs(ord.Fee)
				}

			}

			totalProfit = totalProfit - totalCloseFee - (totalOpenFee)
			updatePayload := models.Positions{
				TotalProfit: totalProfit,
				TotalMargin: totalMargin,
				TotalSize:   totalSize,
			}

			posUpdateErr := models.UpdatePositionByID(db, databasePosition.Id, updatePayload)
			if posUpdateErr != nil {
				fmt.Println("---- unable to update position in database ----", posUpdateErr)
				return
			}

		}
	}

}

func CreateStatement(db *gorm.DB, dbOrder models.Order, orderDetails utils.OrderDetails) *models.Statements {
	floatSize, _ := strconv.ParseFloat(dbOrder.Size, 64)

	acquiredProfit := orderDetails.TotalProfits - math.Abs(orderDetails.Fee)
	dbStatement := models.Statements{
		UserEmail:   dbOrder.Email,
		Exchange:    dbOrder.Service,
		Symbol:      dbOrder.Symbol,
		Side:        dbOrder.Side,
		ClosedPnl:   orderDetails.TotalProfits,
		Size:        floatSize,
		PositionId:  dbOrder.PositionId,
		QuoteAmount: floatSize * orderDetails.PriceAvg,
		ProfitUSD:   acquiredProfit,
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
		fmt.Println("---- statement not saved because total profit is  ----", orderDetails.TotalProfits, " ---- and fee is ---", orderDetails.Fee)
	}

	return statementRes
}
