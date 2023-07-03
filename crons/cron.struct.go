package crons_service

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
