package models

import (
	"errors"
	"math"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
)

type Key struct {
	Keyid       uuid.UUID `gorm:"primary_key;type:uuid;default:gen_random_uuid()" json:"key_id"`
	Uid         string    `gorm:"size:255" json:"uid"`
	Service     string    `gorm:"size:255;not null" json:"service"`
	ApiKey      string    `gorm:"not null;unique" json:"api_key"`
	SecretKey   string    `gorm:"not null;unique" json:"secret_key"`
	Passphrase  string    `gorm:"" json:"passphrase"`
	UserEmail   string    `json:"user_email"`
	OpenShort   int
	OpenLong    int
	TradeAmount int
	Prev        string
}

func (u *Key) FindAllKeys(db *gorm.DB) (*[]Key, error) {
	Keys := []Key{}
	err := db.Model(&Key{}).Limit(100).Find(&Keys).Error
	if err != nil {
		return &[]Key{}, err
	}
	return &Keys, nil
}

func (u *Key) FindKeysByService(db *gorm.DB, service string) (*[]Key, error) {
	Keys := []Key{}
	err := db.Model(&Key{}).Where("service = ? AND trade_amount > ?", service, 0).Limit(100).Find(&Keys).Error
	if err != nil {
		return &[]Key{}, err
	}
	return &Keys, nil
}

func (u *Key) FindKeyById(db *gorm.DB, kid uuid.UUID) (*Key, error) {
	err := db.Model(Key{}).Where("keyid = ?", kid).Take(&u).Error
	if err != nil {
		return &Key{}, err
	}
	if gorm.IsRecordNotFoundError(err) {
		return &Key{}, errors.New("Key not found")
	}
	return u, nil
}

func (u *Key) FindKeysByEmail(db *gorm.DB, email string) (*[]Key, error) {
	Keys := []Key{}
	err := db.Model(Key{}).Where("user_email = ?", email).Find(&Keys).Error
	if err != nil {
		return &[]Key{}, err
	}
	return &Keys, nil
}
func FindKeysByEmailandService(db *gorm.DB, email string, service string) (*Key, error) {
	Keys := Key{}
	err := db.Model(Key{}).Where("user_email = ? AND service = ? ", email, service).Find(&Keys).Error
	if err != nil {
		return &Key{}, err
	}
	return &Keys, nil
}

func (u *Key) FindKeysByUserId(db *gorm.DB, uid uuid.UUID) (*[]Key, error) {
	Keys := []Key{}
	err := db.Model(Key{}).Where("uid = ?", uid).Find(&Keys).Error
	if err != nil {
		return &[]Key{}, err
	}
	return &Keys, nil
}

func (u *Key) FindKeyByUserIdAndShort(db *gorm.DB, uid uuid.UUID, service string) (*Key, error) {
	Keys := Key{}
	err := db.Model(Key{}).Where("uid = ? AND service = ? ", uid, service).Find(&Keys).Error
	if err != nil {
		return &Key{}, err
	}

	if gorm.IsRecordNotFoundError(err) {
		return &Key{}, errors.New("no connected keys found")
	}
	return &Keys, nil
}

func (u *Key) ChangePositions(db *gorm.DB, pos int, pos_type string) (*Key, error) {
	if pos_type == "short" {
		db = db.Model(&Key{}).Where("user_email = ? AND service = ?", u.UserEmail, u.Service).Take(&Key{}).UpdateColumns(
			map[string]interface{}{
				"open_short": pos,
				"prev":       pos_type,
			},
		)
		u.OpenShort = pos
	} else {
		db = db.Model(&Key{}).Where("user_email = ? AND service = ?", u.UserEmail, u.Service).Take(&Key{}).UpdateColumns(
			map[string]interface{}{
				"open_long": pos,
				"prev":      pos_type,
			},
		)
		u.OpenLong = pos
	}

	if db.Error != nil {
		return &Key{}, db.Error
	}
	// This is the display the updated user
	err := db.Model(&Key{}).Where("user_email = ? AND service = ?", u.UserEmail, u.Service).Take(&u).Error
	if err != nil {
		return &Key{}, err
	}
	return u, nil
}

func (u *Key) ChangeTradeAmount(db *gorm.DB, trade_amount int) (*Key, error) {
	conds := Conditions{}
	cond, err := conds.FindCondition(db, int(math.Floor(float64(trade_amount/100))*100))

	if err != nil {
		return &Key{}, err
	}

	long := 0
	short := 0
	for i := 0; i < cond.Positions; i++ {
		if i%2 == 0 {
			long++
		} else {
			short++
		}
	}

	db = db.Model(&Key{}).Where("user_email = ? AND service = ?", u.UserEmail, u.Service).Take(&Key{}).UpdateColumns(
		map[string]interface{}{
			"trade_amount": trade_amount,
			"open_short":   short,
			"open_long":    long,
		},
	)
	if db.Error != nil {
		return &Key{}, db.Error
	}
	// This is the display the updated user
	err = db.Model(&Key{}).Where("user_email = ? AND service = ?", u.UserEmail, u.Service).Take(&u).Error
	if err != nil {
		return &Key{}, err
	}
	return u, nil
}
