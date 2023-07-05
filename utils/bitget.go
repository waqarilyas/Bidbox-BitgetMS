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

func PlaceBitgetOrder(apiKey string, secretKey string, passphrase string, payload NormalOrderRequest) (string, error) {
	// orderSide := "close_long"
	// if position.Side == "short" {
	// 	orderSide = "close_short"
	// }
	// payload := NormalOrderRequest{
	// 	MarginCoin: marginCoin,
	// 	Symbol:     position.Symbol,
	// 	Size:       position.Size,
	// 	Side:       orderSide,
	// 	OrderType:  "market",
	// }

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
