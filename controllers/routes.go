package controllers

import (
	"github.com/kryptomind/bidboxapi/bitgetms/middleware"
	general_websockets "github.com/kryptomind/bidboxapi/bitgetms/websockets/general"
)

func (r *Server) initializeRoutes() {
	s := r.Router.PathPrefix("/trades").Subrouter()
	s.HandleFunc("/", middleware.MiddlewareJSON(r.Home)).Methods("GET")
	s.HandleFunc("/ws", general_websockets.WsHandler)
}
