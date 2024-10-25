package persistence

import (
	"bytes"

	"github.com/DamiaRalitsa/messaging-lib-go/config"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

var db *pg.DB

func ConnectDB(cfg config.Cfg) *pg.DB {
	var addr bytes.Buffer
	addr.WriteString(cfg.Conn.Host)
	addr.WriteString(":")
	addr.WriteString(cfg.Conn.Port)
	db = pg.Connect(&pg.Options{
		Addr:     addr.String(),
		User:     cfg.Conn.User,
		Password: cfg.Conn.Pass,
		Database: cfg.Conn.Name,
	})
	return db
}

func GetDB() *pg.DB {
	return db
}

func CloseDB() error {
	return db.Close()
}

func CreateSchema(models ...interface{}) error {
	for _, model := range models {
		err := db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
