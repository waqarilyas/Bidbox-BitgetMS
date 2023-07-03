package bitget_websockets

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

const (
	PERCENT_CHANGE = 5
)

func (s *Server) WebsocketTest() {
	var paramsList []Subscription
	coinPair := models.CoinPair{}

	coinPairs, err := coinPair.GetAllCoins(s.DB)
	if err != nil {
		fmt.Println("---- error fetching coins ----", err)
		return
	}

	for _, coinPair := range *coinPairs {
		result := strings.ReplaceAll(coinPair.Coin, "/", "")
		eventString := strings.ToUpper(result)

		paramsList = append(paramsList, Subscription{
			InstType: "SP",
			Channel:  "ticker",
			InstID:   eventString,
		})
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	cache := &Cache{}

	go func() {
		for {
			position := models.Positions{}
			positions, err := position.GetOpenPositionsByExchange(s.DB, "bitget")
			if err != nil {
				log.Println("Error fetching positions:", err)
			} else {
				cache.Positions = *positions
				log.Println("Positions cache updated successfully.")
			}
			time.Sleep(10 * time.Second)
		}
	}()

	for {
		conn, err := connectWebSocket()
		if err != nil {
			log.Println("WebSocket connection error:", err)
			time.Sleep(5 * time.Second)
			continue
		}

		err = subscribeToMarketEvents(conn, paramsList)
		if err != nil {
			log.Println("WebSocket subscribe error:", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		go handleWebSocketMessages(conn)

		select {
		case <-interrupt:
			log.Println("Received interrupt signal. Closing WebSocket connection...")
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("WebSocket close message sending error:", err)
			}
			time.Sleep(1 * time.Second)
			return
		}
	}
}

func connectWebSocket() (*websocket.Conn, error) {
	url := "wss://ws.bitget.com/spot/v1/stream"
	dialer := &websocket.Dialer{}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	fmt.Println("-- bitget socket connected successfully ---")

	return conn, nil
}

func subscribeToMarketEvents(conn *websocket.Conn, paramsList []Subscription) error {
	subscribeRequest := struct {
		Op   string         `json:"op"`
		Args []Subscription `json:"args"`
	}{
		Op:   "SUBSCRIBE",
		Args: paramsList,
	}

	fmt.Println("🚀 ~ file: bitget.go:142 ~ funcsubscribeToMarketEvents ~ subscribeRequest:", subscribeRequest)

	err := conn.WriteJSON(subscribeRequest)
	if err != nil {
		fmt.Println("🚀 ~ file: bitget.go:177 ~ funcsubscribeToMarketEvents ~ err:", err)
		return err
	}

	return nil
}

func handleWebSocketMessages(conn *websocket.Conn) {
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
