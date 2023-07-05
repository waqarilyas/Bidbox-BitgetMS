package crons_service

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/helpers"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
	"github.com/kryptomind/bidboxapi/bitgetms/utils"
	bitget_websockets "github.com/kryptomind/bidboxapi/bitgetms/websockets/bitget"
)

type TradesCron struct {
	DB *gorm.DB
}

func NewUserTradesCron() *TradesCron {
	return &TradesCron{}
}

func (server *TradesCron) Run() {
	var wg sync.WaitGroup

	key := models.Key{}
	keys, err := key.FindKeysByService(server.DB, "bitget")
	if err != nil {
		log.Fatal("error getting keys")
		return
	}

	coinPair := models.CoinPair{}

	coinPairs, err := coinPair.GetAllActiveCoins(server.DB)
	if err != nil {
		fmt.Println("---- error fetching coins ----", err)
		return
	}

	for _, v := range *keys {
		wg.Add(1)
		go func(v models.Key) {
			defer wg.Done()

			if v.UserEmail != "kmtester@yopmail.com" {
				return
			}

			val := int(math.Floor(float64(v.TradeAmount)/100.0) * 100)
			cond := models.Conditions{}
			c, err := cond.FindCondition(server.DB, val)
			if err != nil {
				return
			}

			hedgeOrderAmount := float64(v.TradeAmount) * 0.08 / float64(c.Positions)
			go placeBitgetOrder(&v, hedgeOrderAmount, server.DB, coinPairs)
		}(v)
	}

	wg.Wait()
}

func GetTradeEligibleCoinSymbol(apiKey string, secretKey string, passphrase string, coinPairs *[]models.CoinPair) (string, error) {
	_, userAllOpenPositions, openPosError := utils.PerformBitgetPositionQuery(apiKey, secretKey, passphrase, "")
	if openPosError != nil {
		return "", openPosError
	}

	var eligibleCoinSymbols []string

	for _, pair := range *coinPairs {
		vCoinSymbol := strings.Split(pair.Coin, "/")

		pairSymbol := "S" + vCoinSymbol[0] + "S" + vCoinSymbol[1] + "_SUMCBL"
		matched := false
		for _, pos := range userAllOpenPositions {

			if pos.Symbol == pairSymbol && pos.Available != "0" {
				matched = true
				break
			}
		}

		if !matched {
			eligibleCoinSymbols = append(eligibleCoinSymbols, pairSymbol)
		}

	}

	if len(eligibleCoinSymbols) == 0 {
		return "", errors.New("no eligible trade symbol foiund")
	}

	tradePair := utils.SelectRandomElement(eligibleCoinSymbols)

	return tradePair, nil

}

func placeBitgetOrder(v *models.Key, amount float64, db *gorm.DB, coinPairs *[]models.CoinPair) {

	apiKey, secretKey, passphrase, err := helpers.DecryptAllKeys(v.ApiKey, v.SecretKey, v.Passphrase, "bitget")
	if err != nil {
		fmt.Println("error in decryptkeys: ", err)
		return
	}

	tradeSymbol, tradeSymbolError := GetTradeEligibleCoinSymbol(apiKey, secretKey, passphrase, coinPairs)
	if tradeSymbolError != nil {
		fmt.Println("---- no eligible trade symbol found ----", tradeSymbolError)
		return
	}

	orderSize, _, err := utils.GetSize(tradeSymbol, amount)
	if err != nil {
		log.Fatal(err)
		return
	}

	longOrder := bitget_websockets.OrderRequest{
		Size:      fmt.Sprintf("%f", orderSize),
		Side:      "open_long",
		OrderType: "Market",
	}

	shortOrder := bitget_websockets.OrderRequest{
		Size:      fmt.Sprintf("%f", orderSize),
		Side:      "open_short",
		OrderType: "Market",
	}

	batchOrderRequest := bitget_websockets.BitgetBatchOrderRequest{
		Symbol:        tradeSymbol,
		MarginCoin:    "SUSDT",
		OrderDataList: []bitget_websockets.OrderRequest{longOrder, shortOrder},
	}

	_, error := BitgetNewBatchOrder(apiKey, secretKey, passphrase, &batchOrderRequest)
	if error != nil {
		fmt.Println("--- error opening positions ---", error)
		return
	}

	dbPositions, posError := fetchAndUpdateBitgetPosition(tradeSymbol, *v, db, apiKey, secretKey, passphrase)
	if posError != nil {
		fmt.Println("--- unable to update positions in database ---")
	}

	_, orderErr := SaveOrdersInDatabase(db, v, tradeSymbol, amount, "SUSDT", shortOrder, longOrder, dbPositions)

	if orderErr != nil {
		fmt.Println("----  error saving orders ----", err)
	}

}

func BitgetNewBatchOrder(apiKey string, secretKey string, passphrase string, order *bitget_websockets.BitgetBatchOrderRequest) (string, error) {
	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/batch-orders"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(order)
	if err != nil {
		return "", err
	}

	serverTime := helpers.GetBitgetServerTimeStamp()
	signature := GenerateBitgetSignature(secretKey, apiKey, passphrase, "POST", path, serverTime, string(jsonVal))

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonVal))
	req.Header.Add("ACCESS-KEY", apiKey)
	req.Header.Add("ACCESS-SIGN", signature)
	req.Header.Add("ACCESS-TIMESTAMP", serverTime)
	req.Header.Add("ACCESS-PASSPHRASE", passphrase)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("local", "zh-CN")

	if err != nil {
		return "", err
	}

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if res.StatusCode != http.StatusOK {
		return "", errors.New("bitget batch order failed")
	}
	return string(body), nil
}

func GenerateBitgetSignature(apiSecret string, apiKey string, passphrase string, method string, uri string, timestamp string, requestBody string) string {
	message := ""
	if method == "GET" {
		message = fmt.Sprintf("%s%s%s", timestamp, method, uri)
	} else if method == "POST" {
		message = fmt.Sprintf("%s%s%s%s", timestamp, method, uri, requestBody)
	}

	hmac := hmac.New(sha256.New, []byte(apiSecret))
	hmac.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(hmac.Sum(nil))

	return signature
}

func SaveOrdersInDatabase(db *gorm.DB, v *models.Key, coinSymbol string, quoteAmount float64, marginCoin string, shortOrder bitget_websockets.OrderRequest, longOrder bitget_websockets.OrderRequest, positions []models.Positions) ([]*models.Order, error) {
	var longPos models.Positions
	var shortPos models.Positions

	for _, position := range positions {

		if position.Side == "long" {
			longPos = position
		} else if position.Side == "short" {
			shortPos = position
		}

	}

	ordersPayload := []*models.Order{
		{
			Email:       v.UserEmail,
			Symbol:      coinSymbol,
			MarginCoin:  marginCoin,
			Size:        shortOrder.Size,
			Side:        shortOrder.Side,
			OrderType:   shortOrder.OrderType,
			Service:     v.Service,
			QuoteAmount: quoteAmount,
			Profit:      0.0,
			PositionId:  shortPos.Id,
			OrderPrice:  shortPos.OpenPrice,
		},
		{
			Email:       v.UserEmail,
			Symbol:      coinSymbol,
			MarginCoin:  marginCoin,
			Size:        longOrder.Size,
			Side:        longOrder.Side,
			OrderType:   longOrder.OrderType,
			Service:     v.Service,
			QuoteAmount: quoteAmount,
			Profit:      0.0,
			PositionId:  longPos.Id,
			OrderPrice:  longPos.OpenPrice,
		},
	}

	response, err := models.SaveMultipleOrders(db, ordersPayload)
	if err != nil {
		fmt.Println("Error in saving orders:", err)
		return nil, err
	}

	fmt.Println("Orders saved successfully:", response)

	return ordersPayload, nil

}

func fetchAndUpdateBitgetPosition(coinsymbol string, v models.Key, db *gorm.DB, api_key string, secret_key string, passphrase string) ([]models.Positions, error) {
	positionsResponse, _, err := utils.PerformBitgetPositionQuery(api_key, secret_key, passphrase, coinsymbol)
	if err != nil {
		fmt.Println("---error getting position data ---", err)
	}

	var longPos utils.MarginData
	var shortPos utils.MarginData

	var dbPositions []models.Positions

	for _, position := range positionsResponse {
		if position.HoldSide == "long" {
			longPos = position
		} else if position.HoldSide == "short" {
			shortPos = position
		}
	}

	for _, pos := range positionsResponse {
		side := "long"
		currentOrderPos := longPos

		if pos.HoldSide == "short" {
			currentOrderPos = shortPos
			side = "short"
		}

		userPosition := models.Positions{
			Symbol:         pos.Symbol,
			Leverage:       fmt.Sprintf("%d", currentOrderPos.Leverage),
			OpenPrice:      currentOrderPos.AverageOpenPrice,
			LiqPrice:       currentOrderPos.LiquidationPrice,
			UnrealizedPl:   currentOrderPos.UnrealizedPL,
			MarkPrice:      currentOrderPos.MarketPrice,
			Side:           side,
			Size:           currentOrderPos.Available,
			Margin:         currentOrderPos.Margin,
			UserEmail:      v.UserEmail,
			Status:         "opened",
			Exchange:       "bitget",
			FirstBuyAmount: pos.Available,
		}

		posResponse, createErr := userPosition.UpdateOrCreatePosition(db)
		dbPositions = append(dbPositions, *posResponse)

		if createErr != nil {
			fmt.Println(userPosition, "---- error creating new position in database ----", createErr)
			return nil, createErr
		}
		fmt.Println("---- position saved successfully---", posResponse)

	}

	return dbPositions, nil

}
