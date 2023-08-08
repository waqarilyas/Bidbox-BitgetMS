package models

import "github.com/jinzhu/gorm"

type Settings struct {
	Layers           int     `json:"layers"`
	Leverage         int     `json:"leverage"`
	ProfitPercentage float64 `json:"profit_percentage"`
	TakeProfit       float64 `json:"take_profit"`
}

func (s *Settings) GetSettings(db *gorm.DB) (*Settings, error) {
	settings := Settings{}
	err := db.Model(&Settings{}).Take(&settings).Error
	if err != nil {
		return &Settings{}, err
	}
	return &settings, nil
}
