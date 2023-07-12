package tests

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres" //postgres database driver
	"github.com/joho/godotenv"

	crons_service "github.com/kryptomind/bidboxapi/bitgetms/crons"
)

var db *gorm.DB

func Initialize(Dbdriver, DbUser, DbPassword, DbPort, DbHost, DbName string) {

	var err error
	DBURL := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s", DbHost, DbPort, DbUser, DbName, DbPassword)
	db, err = gorm.Open(Dbdriver, DBURL)
	if err != nil {
		log.Println("Cannot connect to the database")
		log.Fatal("This is the error:", err)
	} else {
		log.Println("Connected to the database")
	}
}

func Init() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error getting env")
	} else {
		log.Println("Getting Values")
	}

	Initialize(os.Getenv("DB_DRIVER"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_PORT"), os.Getenv("DB_HOST"), os.Getenv("DB_NAME"))
}

func TestCron(t *testing.T) {
	Init()
	tradesCron := crons_service.TradesCron{}
	tradesCron.DB = db
	tradesCron.Run()
}
