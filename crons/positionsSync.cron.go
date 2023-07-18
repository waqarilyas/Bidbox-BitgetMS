package crons_service

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/kryptomind/bidboxapi/bitgetms/helpers"
	"github.com/kryptomind/bidboxapi/bitgetms/models"
	"github.com/kryptomind/bidboxapi/bitgetms/utils"
)

func NewPositionSyncCron() *TradesCron {
	return &TradesCron{}
}

func (server *TradesCron) RunPositionsCron() {
	var wg sync.WaitGroup

	key := models.Key{}
	keys, err := key.FindKeysByService(server.DB, "bitget")
	if err != nil {
		log.Fatal("error getting keys")
		return
	}

	for _, v := range *keys {
		wg.Add(1)
		go func(v models.Key) {
			defer wg.Done()

			if !v.Start {
				return
			}

			handleOpenPositions(server.DB, v)
		}(v)
	}

	wg.Wait()
}

func handleOpenPositions(db *gorm.DB, key models.Key) {
	apiKey, secretKey, passphrase, err := helpers.DecryptAllKeys(key.ApiKey, key.SecretKey, key.Passphrase, "bitget")
	if err != nil {
		fmt.Println("error in decryptkeys: ", err)
		return
	}

	_, exchangePositions, openPosError := utils.PerformBitgetPositionQuery(apiKey, secretKey, passphrase, "")
	if openPosError != nil {

		return
	}

	var positions models.Positions

	dbPositions, _, dbErr := positions.FindAllUserPositions(db, key.UserEmail)
	if dbErr != nil {
		fmt.Println("---- unable to get user positions from database ---", dbErr)
		return
	}

	for _, pos := range *dbPositions {
		found := false

		for _, exchangePos := range exchangePositions {
			floatAvailable, _ := strconv.ParseFloat(exchangePos.Available, 64)
			if (exchangePos.Symbol == pos.Symbol) && (floatAvailable != 0) && (exchangePos.HoldSide == pos.Side) {
				found = true
			}
		}

		if !found {
			updatedPos := models.Positions{
				Status: "closed",
			}

			posUpdateErr := models.UpdatePositionByID(db, pos.Id, updatedPos)
			if posUpdateErr != nil {
				fmt.Println("---- unable to update user pos in database in position sync ----", posUpdateErr)
				continue
			}
		}

	}

}
