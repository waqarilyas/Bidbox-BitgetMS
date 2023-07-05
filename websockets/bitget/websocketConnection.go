package bitget_websockets

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
)

func (s *Server) WebsocketTest() {
	var paramsList []Subscription
	coinPair := models.CoinPair{}

	coinPairs, err := coinPair.GetAllCoins(s.DB)
	if err != nil {
		return
	}

	for _, coinPair := range *coinPairs {
		result := strings.ReplaceAll(coinPair.Coin, "/", "")
		eventString := strings.ToUpper(result)
		paramsList = append(paramsList, Subscription{
			InstType: "mc",
			Channel:  "ticker",
			InstID:   eventString,
		})
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	pingTicker := time.NewTicker(20 * time.Second)

	// go func() {
	// 	for {
	// 		position := models.Positions{}
	// 		positions, err := position.GetOpenPositionsByExchange(s.DB, "bitget")
	// 		if err != nil {
	// 			log.Println("Error fetching positions:", err)
	// 		} else {
	// 			cache.Positions = *positions
	// 			log.Println("Positions cache updated successfully.")
	// 		}
	// 		time.Sleep(10 * time.Second)
	// 	}
	// }()

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

		go HandleWebSocketMessages(conn, s.DB)

		select {
		case <-interrupt:
			log.Println("Received interrupt signal. Closing WebSocket connection...")
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("WebSocket close message sending error:", err)
			}
			time.Sleep(1 * time.Second)
			return
		case <-pingTicker.C:
			// Send ping message
			err := sendPingMessage(conn)
			if err != nil {
				log.Println("Ping message sending error:", err)
				conn.Close()
				time.Sleep(5 * time.Second)
				break
			}
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

	err := conn.WriteJSON(subscribeRequest)
	if err != nil {
		fmt.Println("🚀 ~ file: bitget.go:177 ~ funcsubscribeToMarketEvents ~ err:", err)
		return err
	}

	return nil
}

func sendPingMessage(conn *websocket.Conn) error {

	pingMessage := struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}{
		Op:   "ping",
		Args: []string{"ping"},
	}

	err := conn.WriteJSON(pingMessage)
	if err != nil {
		return err
	}

	return nil
}
