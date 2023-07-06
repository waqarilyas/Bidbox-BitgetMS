package models

import (
	"errors"
	"fmt"

	"github.com/jinzhu/gorm"
)

type BybitOrderRequest struct {
	Category       string `json:"category"`
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	OrderType      string `json:"orderType"`
	Qty            string `json:"qty"`
	TimeInForce    string `json:"timeInForce"`
	ReduceOnly     bool   `json:"reduce_only"`
	CloseOnTrigger bool   `json:"closeOnTrigger"`
}
type BybitResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		OrderID     string `json:"orderId"`
		OrderLinkId string `json:"orderLinkId"`
	} `json:"result"`
	RetExtInfo map[string]interface{} `json:"retExtInfo"`
	Time       int64                  `json:"time"`
}

// type BybitResponse struct {
// 	RetCode    int                    `json:"retCode"`
// 	RetMsg     string                 `json:"retMsg"`
// 	Result     struct {
// 		OrderID     string `json:"order_id"`
// 		OrderLinkId string `json:"orderLinkId"`
// 	}
// 	RetExtInfo map[string]interface{} `json:"retExtInfo"`
// 	Time       int64                  `json:"time"`
// }

type OrderRequest struct {
	Symbol     string `json:"symbol"`
	MarginCoin string `json:"marginCoin"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	OrderType  string `json:"orderType"`
	StopLoss   string `json:"presetStopLossPrice"`
	TakeProfit string `json:"presetTakeProfitPrice"`
}

type OrderResponse struct {
	Code        string `json:"code"`
	Msg         string `json:"msg"`
	RequestTime int64  `json:"requestTime"`
	Data        struct {
		ClientOid string `json:"clientOid"`
		OrderID   string `json:"orderId"`
	} `json:"data"`
}

type Order struct {
	Email       string
	Symbol      string
	MarginCoin  string
	Size        string
	Side        string
	OrderType   string
	Service     string
	QuoteAmount float64
	Profit      float64
	PositionId  int    `json:"position_id"`
	OrderPrice  string `json:"order_price"`
}

func SaveMultipleOrders(db *gorm.DB, orders []*Order) ([]*Order, error) {
	var savedOrders []*Order
	for _, order := range orders {
		err := db.Create(order).Error
		if err != nil {
			fmt.Println("Error in saving function")
			return nil, err
		}
		savedOrders = append(savedOrders, order)
	}
	return savedOrders, nil
}

func (o *Order) Initialize(order OrderRequest, email string, client_id string, order_id string) {
	o.MarginCoin = order.MarginCoin
	o.Side = order.Side
	o.Symbol = order.Symbol
	o.Size = order.Size
	o.OrderType = order.OrderType
	o.Email = email
}

func (o *OrderRequest) Validate() error {
	if o.MarginCoin == "" {
		return errors.New("margin coin is required")
	}
	if o.OrderType == "" {
		return errors.New("ordertype is required")
	}
	if o.Side == "" {
		return errors.New("side is required")
	}
	if o.Size == "" {
		return errors.New("size is required")
	}
	if o.Symbol == "" {
		return errors.New("symbol is required")
	}
	return nil
}

func (o *Order) SaveOrder(db *gorm.DB) (*Order, error) {
	err := db.Create(&o).Error
	if err != nil {
		fmt.Println("error in saving func")
		return &Order{}, err
	}
	return o, nil
}

func GetOrdersByPositionIdAndSide(db *gorm.DB, positionId int, side string) ([]*Order, error) {
	var positions []*Order
	err := db.Where("position_id = ? AND side = ?", positionId, side).Find(&positions).Error
	if err != nil {
		return nil, err
	}
	return positions, nil
}

// func SaveMultipleOrders(db *gorm.DB, orders []*Order) ([]*Order, error) {
// 	err := db.Create(&orders).Error
// 	if err != nil {
// 		fmt.Println("Error in saving function")
// 		return nil, err
// 	}
// 	return orders, nil
// }
