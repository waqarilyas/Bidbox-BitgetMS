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

	for _, v := range *keys {
		wg.Add(1)
		go func(v models.Key) {
			defer wg.Done()
			val := int(math.Floor(float64(v.TradeAmount)/100.0) * 100)
			cond := models.Conditions{}
			c, err := cond.FindCondition(server.DB, val)
			if err != nil {
				return
			}

			hedgeOrderAmount := float64(v.TradeAmount) * 0.08 / float64(c.Positions)
			placeBitgetOrder(&v, hedgeOrderAmount, server.DB)
		}(v)
	}

	wg.Wait()
}

func placeBitgetOrder(v *models.Key, amount float64, db *gorm.DB) {
	if v.UserEmail != "kmtester@yopmail.com" {
		fmt.Println("---- user is not km tester ---")
		return
	}

	apiKey, secretKey, passphrase, err := helpers.DecryptAllKeys(v.ApiKey, v.SecretKey, v.Passphrase, "bitget")
	if err != nil {
		fmt.Println("error in decryptkeys: ", err)
		return
	}

	orderSize, _, err := utils.GetSize("SETHSUSDT_SUMCBL", amount)
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
		Symbol:        "SETHSUSDT_SUMCBL",
		MarginCoin:    "SUSDT",
		OrderDataList: []bitget_websockets.OrderRequest{longOrder, shortOrder},
	}

	_, error := BitgetNewBatchOrder(apiKey, secretKey, passphrase, &batchOrderRequest)
	if error != nil {
		fmt.Println("--- error opening positions ---", error)
		return
	}

	orders, err := SaveOrdersInDatabase(db, v, "SETHSUSDT_SUMCBL", amount, "SUSDT", shortOrder, longOrder)

	if err != nil {
		fmt.Println("----  error saving orders ----", err)
	}

	fetchAndUpdateBitgetPosition(orders, "SETHSUSDT_SUMCBL", *v, db, apiKey, secretKey, passphrase)
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

func SaveOrdersInDatabase(db *gorm.DB, v *models.Key, coinSymbol string, quoteAmount float64, marginCoin string, shortOrder bitget_websockets.OrderRequest, longOrder bitget_websockets.OrderRequest) ([]*models.Order, error) {

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

func PerformBitgetPositionQuery(apiKey, apiSecret, passphrase string, coin_pair string) (*MarginData, error) {
	expires := helpers.GetBitgetServerTimeStamp()
	uri := "/api/mix/v1/position/allPosition?productType=sumcbl"

	serverTime := helpers.GetBitgetServerTimeStamp()

	signature := GenerateBitgetSignature(apiSecret, apiKey, passphrase, "GET", uri, serverTime, "")

	url := fmt.Sprintf("https://api.bitget.com%s", uri)
	method := "GET"

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("ACCESS-KEY", apiKey)
	req.Header.Add("ACCESS-PASSPHRASE", passphrase)
	req.Header.Add("ACCESS-TIMESTAMP", expires)
	req.Header.Add("ACCESS-SIGN", signature)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		return nil, err
	}

	var accountData MarginDataResponse

	err = json.Unmarshal(body, &accountData)
	if err != nil {
		return nil, err
	}

	var requiredPosition MarginData
	for _, pos := range accountData.Data {

		if pos.Symbol == coin_pair {
			requiredPosition = pos
			break
		}
	}

	return &requiredPosition, nil
}

func fetchAndUpdateBitgetPosition(orders []*models.Order, coinsymbol string, v models.Key, db *gorm.DB, api_key string, secret_key string, passphrase string) {
	positionsResponse, err := utils.PerformBitgetPositionQuery(api_key, secret_key, passphrase, coinsymbol)
	if err != nil {
		fmt.Println("---error getting position data ---", err)
	}

	var longPos utils.MarginData
	var shortPos utils.MarginData

	for _, position := range positionsResponse {

		if position.HoldSide == "long" {
			longPos = position

		} else if position.HoldSide == "short" {
			shortPos = position

		}

	}

	for _, order := range orders {
		currentOrderPos := longPos

		if order.Side == "open_short" {
			currentOrderPos = shortPos
		}

		userPosition := models.Positions{
			Symbol:       order.Symbol,
			Leverage:     fmt.Sprintf("%d", currentOrderPos.Leverage),
			OpenPrice:    currentOrderPos.AverageOpenPrice,
			LiqPrice:     currentOrderPos.LiquidationPrice,
			UnrealizedPl: currentOrderPos.UnrealizedPL,
			MarkPrice:    currentOrderPos.MarketPrice,
			Side:         order.Side,
			Size:         order.Size,
			Margin:       currentOrderPos.Margin,
			UserEmail:    v.UserEmail,
			Status:       "opened",
			Exchange:     "bitget",
		}

		posResponse, createErr := userPosition.UpdateOrCreatePosition(db)

		if createErr != nil {
			fmt.Println(userPosition, "---- error creating new position in database ----", createErr)
			return
		}
		fmt.Println("---- position saved successfully---", posResponse)

	}

}
