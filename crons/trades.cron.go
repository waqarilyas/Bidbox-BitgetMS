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
	key := models.Key{}

	keys, err := key.FindKeysByService(server.DB, "bitget")
	if err != nil {
		log.Fatal("error getting keys")
		return
	}

	for _, v := range *keys {
		val := int(math.Floor(float64(v.TradeAmount)/100.0) * 100)
		cond := models.Conditions{}
		c, err := cond.FindCondition(server.DB, val)
		if err != nil {
			return
		}

		hedge_order_amount := float64(v.TradeAmount) * 0.08 / float64(c.Positions)
		go placeBitgetOrder(&v, hedge_order_amount)
	}

}

func placeBitgetOrder(v *models.Key, amount float64) {

	if v.UserEmail != "kmtester@yopmail.com" {
		fmt.Println("---- user is not km tester ---")
		return
	}

	api_key, secret_key, passphrase, err := helpers.DecryptAllKeys(v.ApiKey, v.SecretKey, v.Passphrase, "bitget")
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
	go BitgetNewBatchOrder(api_key, secret_key, passphrase, &batchOrderRequest)

}

func BitgetNewBatchOrder(api_key string, secret_key string, passphrase string, order *bitget_websockets.BitgetBatchOrderRequest) (string, error) {
	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/batch-orders"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(order)
	if err != nil {

		return "", err
	}

	server_time := helpers.GetBitgetServerTimeStamp()
	signatures := GenerateBitgetSignature(secret_key, api_key, passphrase, "POST", path, server_time, string(jsonVal))

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonVal))
	req.Header.Add("ACCESS-KEY", api_key)
	req.Header.Add("ACCESS-SIGN", signatures)
	req.Header.Add("ACCESS-TIMESTAMP", server_time)
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

func saveOrdersInDatabase() {

}
