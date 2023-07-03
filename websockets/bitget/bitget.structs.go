package bitget_websockets

import (
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

type Server struct {
	DB     *gorm.DB
	Router *mux.Router
}

type Cache struct {
	Positions []models.Positions
}

type Payload struct {
	Op   string         `json:"op"`
	Args []Subscription `json:"args"`
}

type Subscription struct {
	InstType string `json:"instType"`
	Channel  string `json:"channel"`
	InstID   string `json:"instId"`
}

type SnapshotData struct {
	InstID      string `json:"instId"`
	Last        string `json:"last"`
	Open24h     string `json:"open24h"`
	High24h     string `json:"high24h"`
	Low24h      string `json:"low24h"`
	BestBid     string `json:"bestBid"`
	BestAsk     string `json:"bestAsk"`
	BaseVolume  string `json:"baseVolume"`
	QuoteVolume string `json:"quoteVolume"`
	Timestamp   int64  `json:"ts"`
	LabelID     int    `json:"labelId"`
	OpenUtc     string `json:"openUtc"`
	ChangeUTC   string `json:"chgUTC"`
	BidSize     string `json:"bidSz"`
	AskSize     string `json:"askSz"`
}

type Snapshot struct {
	Action string         `json:"action"`
	Arg    Subscription   `json:"arg"`
	Data   []SnapshotData `json:"data"`
}

type Exchange string

const (
	Bitget  Exchange = "bitget"
	Binance Exchange = "binance"
	Bybit   Exchange = "bybit"
)

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
