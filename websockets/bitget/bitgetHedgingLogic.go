package bitget_websockets

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

func HandleWebSocketMessages(conn *websocket.Conn) {
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("WebSocket message receiving error:", err)
			return
		}

		var eventData Snapshot

		err = json.Unmarshal(message, &eventData)
		if err != nil {
			log.Println("WebSocket message parsing error:", err)
			continue
		}

		fmt.Println("---- event data ---", eventData)

		// go handleMarketUpdate(db, cache, eventData.Symbol, eventData.MarketPrice)
	}
}

func HandleMarketUpdate(db *gorm.DB, cache *Cache, coinPair string, marketPrice string) {

	// positions := cache.Positions

	// var symbolPositions []models.Positions

	// for _, pos := range positions {
	// 	if pos.Symbol == coinPair {
	// 		symbolPositions = append(symbolPositions, pos)
	// 	}
	// }

	// for _, filteredPos := range symbolPositions {
	// 	go handlePositionOnRateUpdate(filteredPos, marketPrice)
	// }

}

func HandlePositionOnRateUpdate(position models.Positions, marketPrice string) {
	// lastUpdatePrice := position.OpenPrice

	// if position.LastUpdatePrice != "" {
	// 	lastUpdatePrice = position.LastUpdatePrice
	// }

	// flEntryPrice, err := strconv.ParseFloat(lastUpdatePrice, 64)
	// if err != nil {
	// 	fmt.Println("--- unable to convert entryprice to float ---", err)
	// }

	// flMarketPriceFloat, err := strconv.ParseFloat(marketPrice, 64)
	// if err != nil {
	// 	fmt.Println("--- unable to convert marketPrice to float ---", err)
	// }

	// percentage_change := (flMarketPriceFloat - flEntryPrice) / flEntryPrice * 100

	// switch position.Side {
	// case "long":
	// 	if percentage_change > 1 {
	// 		fmt.Println(position.Symbol, "--- long position", "---- percentage change ----", percentage_change)
	// 		take_profit := flEntryPrice * (1 - PERCENT_CHANGE/100)
	// 		fmt.Println("--- take profit for long position at ----", take_profit)

	// 	}

	// case "short":
	// 	if percentage_change < -1 {
	// 		// update TP/SL here
	// 		fmt.Println(position.Symbol, "--- short position", "---- percentage change ----", percentage_change)
	// 		take_profit := flEntryPrice * (1 - PERCENT_CHANGE/100)
	// 		fmt.Println("--- take profit for short position at ----", take_profit)

	// 	}

	// default:
	// 	fmt.Println("--- defaault case reached ----")
	// }

}
