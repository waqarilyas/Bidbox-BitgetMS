package general_websockets

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[*websocket.Conn]string)
var broadcast = make(chan string)

func WsHandler(w http.ResponseWriter, r *http.Request) {
	email := r.URL.Query().Get("email") // Assuming the email is passed as a query parameter
	if email == "" {
		http.Error(w, "Email not provided", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading to WebSocket:", err)
		return
	}

	clients[conn] = email

	go handleWebSocketConnection(conn, email)
}

func handleWebSocketConnection(conn *websocket.Conn, email string) {
	defer func() {
		conn.Close()
		delete(clients, conn)
	}()

	for {
		select {
		case msg := <-broadcast:
			// Check if the event should be sent to this client
			if clients[conn] == email {
				err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
				if err != nil {
					log.Println("Error sending message:", err)
					return
				}
			}
		}
	}
}

func SendEventOnEmail(exchangeData interface{}, email string) {
	event, err := json.Marshal(exchangeData)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Find the WebSocket connection with the matching email and send the event
	for _, connEmail := range clients {
		if connEmail == email {
			broadcast <- string(event)
		}
	}
}
