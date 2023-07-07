package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/kryptomind/bidboxapi/bitgetms/helpers"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

type MarginData struct {
	MarginCoin        string `json:"marginCoin"`
	Symbol            string `json:"symbol"`
	HoldSide          string `json:"holdSide"`
	OpenDelegateCount string `json:"openDelegateCount"`
	Margin            string `json:"margin"`
	Available         string `json:"available"`
	Locked            string `json:"locked"`
	Total             string `json:"total"`
	Leverage          int    `json:"leverage"`
	AchievedProfits   string `json:"achievedProfits"`
	AverageOpenPrice  string `json:"averageOpenPrice"`
	MarginMode        string `json:"marginMode"`
	HoldMode          string `json:"holdMode"`
	UnrealizedPL      string `json:"unrealizedPL"`
	LiquidationPrice  string `json:"liquidationPrice"`
	KeepMarginRate    string `json:"keepMarginRate"`
	MarketPrice       string `json:"marketPrice"`
	CTime             string `json:"cTime"`
}

type MarginDataResponse struct {
	Code        string       `json:"code"`
	Msg         string       `json:"msg"`
	RequestTime int64        `json:"requestTime"`
	Data        []MarginData `json:"data"`
}

type NormalOrderRequest struct {
	Symbol     string `json:"symbol"`
	MarginCoin string `json:"marginCoin"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	OrderType  string `json:"orderType"`
}

type OrderRequest struct {
	// Symbol     string `json:"symbol"`
	// MarginCoin string `json:"marginCoin"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	OrderType string `json:"orderType"`
}

type BitgetBatchOrderRequest struct {
	Symbol        string         `json:"symbol"`
	MarginCoin    string         `json:"marginCoin"`
	OrderDataList []OrderRequest `json:"orderDataList"`
}

type BatchOrderResponse struct {
	Code        string    `json:"code"`
	Msg         string    `json:"msg"`
	RequestTime int64     `json:"requestTime"`
	Data        BatchData `json:"data"`
}

type BatchData struct {
	OrderInfo []BatchOrderInfo `json:"orderInfo"`
	Failure   []interface{}    `json:"failure"`
	Result    bool             `json:"result"`
}

type PlaceOrderResponse struct {
	Code        string         `json:"code"`
	Msg         string         `json:"msg"`
	RequestTime int64          `json:"requestTime"`
	Data        BatchOrderInfo `json:"data"`
}

type BatchOrderInfo struct {
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid"`
}

type OrderDetailsResponse struct {
	Code        string       `json:"code"`
	Msg         string       `json:"msg"`
	RequestTime int64        `json:"requestTime"`
	Data        OrderDetails `json:"data"`
}

type OrderDetails struct {
	Symbol           string   `json:"symbol"`
	Size             float64  `json:"size"`
	OrderID          string   `json:"orderId"`
	ClientOid        string   `json:"clientOid"`
	FilledQty        float64  `json:"filledQty"`
	Fee              float64  `json:"fee"`
	Price            *float64 `json:"price"`
	PriceAvg         float64  `json:"priceAvg"`
	State            string   `json:"state"`
	Side             string   `json:"side"`
	TimeInForce      string   `json:"timeInForce"`
	TotalProfits     float64  `json:"totalProfits"`
	PosSide          string   `json:"posSide"`
	MarginCoin       string   `json:"marginCoin"`
	FilledAmount     float64  `json:"filledAmount"`
	OrderType        string   `json:"orderType"`
	Leverage         string   `json:"leverage"`
	MarginMode       string   `json:"marginMode"`
	ReduceOnly       bool     `json:"reduceOnly"`
	EnterPointSource string   `json:"enterPointSource"`
	TradeSide        string   `json:"tradeSide"`
	HoldMode         string   `json:"holdMode"`
	OrderSource      string   `json:"orderSource"`
	CTime            string   `json:"cTime"`
	UTime            string   `json:"uTime"`
}

func PerformBitgetPositionQuery(apiKey, apiSecret, passphrase string, coin_pair string) ([]MarginData, []MarginData, error) {
	expires := helpers.GetBitgetServerTimeStamp()
	uri := "/api/mix/v1/position/allPosition?productType=sumcbl"
	signature := GenerateBitgetSignature(apiSecret, apiKey, passphrase, "GET", uri, expires, "")

	url := fmt.Sprintf("https://api.bitget.com%s", uri)
	method := "GET"

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Add("ACCESS-KEY", apiKey)
	req.Header.Add("ACCESS-PASSPHRASE", passphrase)
	req.Header.Add("ACCESS-TIMESTAMP", expires)
	req.Header.Add("ACCESS-SIGN", signature)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		return nil, nil, err
	}

	var accountData MarginDataResponse

	err = json.Unmarshal(body, &accountData)
	if err != nil {
		return nil, nil, err
	}

	var requiredPosition []MarginData
	for _, pos := range accountData.Data {

		if pos.Symbol == coin_pair {
			requiredPosition = append(requiredPosition, pos)
		}
	}

	return requiredPosition, accountData.Data, nil
}

func PlaceClosePositionOrder(apiKey string, secretKey string, passphrase string, position models.Positions, marginCoin string) (string, error) {
	orderSide := "close_long"
	if position.Side == "short" {
		orderSide = "close_short"
	}
	payload := NormalOrderRequest{
		MarginCoin: marginCoin,
		Symbol:     position.Symbol,
		Size:       position.Size,
		Side:       orderSide,
		OrderType:  "market",
	}

	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/placeOrder"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(payload)
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
		return "", errors.New("unable to close position at the moment")
	}
	return string(body), nil
}

func PlaceOpenPositionOrder(apiKey string, secretKey string, passphrase string, position NormalOrderRequest) (string, error) {

	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/placeOrder"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(position)
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
		return "", errors.New("unable to close position at the moment")
	}
	return string(body), nil
}

func PlaceBitgetOrder(apiKey string, secretKey string, passphrase string, payload NormalOrderRequest) (PlaceOrderResponse, error) {
	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/placeOrder"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(payload)
	if err != nil {
		return PlaceOrderResponse{}, err
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
		return PlaceOrderResponse{}, err
	}

	res, err := client.Do(req)
	if err != nil {
		return PlaceOrderResponse{}, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return PlaceOrderResponse{}, err
	}

	if res.StatusCode != http.StatusOK {
		return PlaceOrderResponse{}, errors.New("unable to close position at the moment")
	}

	var orderResponse PlaceOrderResponse

	err = json.Unmarshal(body, &orderResponse)
	if err != nil {
		return PlaceOrderResponse{}, err
	}

	return orderResponse, nil
}

func PlaceBitgetBatchOrder(apiKey string, secretKey string, passphrase string, order *BitgetBatchOrderRequest) (BatchOrderResponse, error) {
	host := "https://api.bitget.com"
	path := "/api/mix/v1/order/batch-orders"
	url := host + path

	method := "POST"
	client := &http.Client{}

	jsonVal, err := json.Marshal(order)
	if err != nil {
		return BatchOrderResponse{}, err
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
		return BatchOrderResponse{}, err
	}

	res, err := client.Do(req)
	if err != nil {
		return BatchOrderResponse{}, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return BatchOrderResponse{}, err
	}

	if res.StatusCode != http.StatusOK {
		return BatchOrderResponse{}, errors.New("bitget batch order failed")
	}

	var batchResponse BatchOrderResponse

	err = json.Unmarshal(body, &batchResponse)
	if err != nil {
		return BatchOrderResponse{}, err
	}

	return batchResponse, nil
}

func BitgetOrderDetails(apiKey string, secretKey string, passphrase string, symbol string, order_id string) (OrderDetailsResponse, error) {

	uri := "/api/mix/v1/order/detail?symbol=" + symbol + "&" + "orderId=" + order_id
	expires := helpers.GetBitgetServerTimeStamp()
	signature := GenerateBitgetSignature(secretKey, apiKey, passphrase, "GET", uri, expires, "")

	url := fmt.Sprintf("https://api.bitget.com%s", uri)
	method := "GET"

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return OrderDetailsResponse{}, err
	}

	req.Header.Add("ACCESS-KEY", apiKey)
	req.Header.Add("ACCESS-PASSPHRASE", passphrase)
	req.Header.Add("ACCESS-TIMESTAMP", expires)
	req.Header.Add("ACCESS-SIGN", signature)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return OrderDetailsResponse{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		return OrderDetailsResponse{}, err
	}

	var orderDetails OrderDetailsResponse

	err = json.Unmarshal(body, &orderDetails)
	if err != nil {
		return OrderDetailsResponse{}, err
	}

	return orderDetails, nil

}
