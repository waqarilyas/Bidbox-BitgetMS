package models

import (
	"time"

	"github.com/jinzhu/gorm"
)

type Statements struct {
	UserEmail   string    `json:"user_email"`
	Exchange    string    `json:"exchange"`
	Symbol      string    `json:"symbol"`
	CreatedTime time.Time `json:"created_time"`
	UpdatedTime time.Time `json:"updated_time"`
	Side        string    `json:"side"`
	ClosedPnl   float64   `json:"closed_pnl"`
	Size        float64   `json:"size"`
	PositionId  int       `json:"position_id"`
	QuoteAmount float64   `json:"quote_amount"`
	ProfitUSD   float64   `json:"profit_usd"`
}

type LeaderboardUser struct {
	UserEmail string
	Name      string
	ClosedPnl float64
	Country   string
	Username  string
}

type LeaderboardAPI struct {
	db *gorm.DB
}

func NewLeaderboardAPI(db *gorm.DB) *LeaderboardAPI {
	return &LeaderboardAPI{db: db}
}

func (statement *Statements) CreateNewStatement(db *gorm.DB) (*Statements, error) {
	err := db.Create(&statement).Error
	if err != nil {
		return &Statements{}, err
	}
	return statement, nil
}
