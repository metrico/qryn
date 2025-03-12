package dbRegistry

import (
	"crypto/tls"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/config"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
	"github.com/metrico/qryn/reader/utils/dsn"
	"github.com/metrico/qryn/reader/utils/logger"
	"strconv"
	"time"
)

var Registry model.IDBRegistry
var DataDBSession []model.ISqlxDB
var DatabaseNodeMap []model.DataDatabasesMap

func Init() {
	p := plugins.GetDatabaseRegistryPlugin()
	if p != nil {
		Registry = (*p)()
	}
	Registry = InitStaticRegistry()
}

func InitStaticRegistry() model.IDBRegistry {
	initDataDBSession()
	if len(DataDBSession) == 0 {
		panic("We don't have any active DB session configured. Please check your config")
	}
	dbMap := map[string]*model.DataDatabasesMap{}
	for i, node := range DatabaseNodeMap {
		node.Session = DataDBSession[i]
		dbMap[node.Config.Node] = &node
	}
	return NewStaticDBRegistry(dbMap)
}

func initDataDBSession() {
	dbMap := []model.ISqlxDB{}
	dbNodeMap := []model.DataDatabasesMap{}

	for _, _dbObject := range config.Cloki.Setting.DATABASE_DATA {
		dbObject := _dbObject
		//logger.Info(fmt.Sprintf("Connecting to [%s, %s, %s, %s, %d, %d, %d]\n", dbObject.Host, dbObject.User, dbObject.Name,
		//	dbObject.Node, dbObject.Port, dbObject.ReadTimeout, dbObject.WriteTimeout))
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		defer jsoniter.ConfigFastest.ReturnStream(stream)
		stream.WriteRaw("Connecting to [")
		stream.WriteRaw(dbObject.Host)
		stream.WriteRaw(", ")
		stream.WriteRaw(dbObject.User)
		stream.WriteRaw(", ")
		stream.WriteRaw(dbObject.Name)
		stream.WriteRaw(", ")
		stream.WriteRaw(dbObject.Node)
		stream.WriteRaw(", ")
		stream.WriteInt64(int64(dbObject.Port))
		stream.WriteRaw(", ")
		stream.WriteInt64(int64(dbObject.ReadTimeout))
		stream.WriteRaw(", ")
		stream.WriteInt64(int64(dbObject.WriteTimeout))
		stream.WriteRaw("]\n")
		logger.Info(string(stream.Buffer()))
		addr := func() string {
			stream := jsoniter.ConfigFastest.BorrowStream(nil)
			defer jsoniter.ConfigFastest.ReturnStream(stream)
			stream.WriteRaw(dbObject.Host)
			stream.WriteRaw(":")
			// WriteUint32 is used if dbObject.Port is a uint32; otherwise adjust conversion as needed.
			stream.WriteUint32(dbObject.Port)
			return string(stream.Buffer())
		}()
		getDB := func() *sqlx.DB {
			opts := &clickhouse.Options{
				TLS: nil,
				//Addr: []string{fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port)},
				Addr: []string{addr},
				Auth: clickhouse.Auth{
					Database: dbObject.Name,
					Username: dbObject.User,
					Password: dbObject.Password,
				},
				DialContext: nil,
				Debug:       dbObject.Debug,
				Settings:    nil,
			}

			if dbObject.Secure {
				opts.TLS = &tls.Config{
					InsecureSkipVerify: true,
				}
			}
			conn := clickhouse.OpenDB(opts)
			conn.SetMaxOpenConns(dbObject.MaxOpenConn)
			conn.SetMaxIdleConns(dbObject.MaxIdleConn)
			conn.SetConnMaxLifetime(time.Minute * 10)
			db := sqlx.NewDb(conn, "clickhouse")
			db.SetMaxOpenConns(dbObject.MaxOpenConn)
			db.SetMaxIdleConns(dbObject.MaxIdleConn)
			db.SetConnMaxLifetime(time.Minute * 10)
			return db
		}

		dbMap = append(dbMap, &dsn.StableSqlxDBWrapper{
			DB:    getDB(),
			GetDB: getDB,
			Name:  _dbObject.Node,
		})

		chDsn := "n-clickhouse://"
		if dbObject.ClusterName != "" {
			chDsn = "c-clickhouse://"
		}
		chDsn += dbObject.User + ":" + dbObject.Password + "@" + dbObject.Host +
			strconv.FormatInt(int64(dbObject.Port), 10) + "/" + dbObject.Name
		if dbObject.Secure {
			chDsn += "?secure=true"
		}

		dbNodeMap = append(dbNodeMap, model.DataDatabasesMap{
			Config: &dbObject,
			DSN:    chDsn,
		})

		logger.Info("----------------------------------- ")
		logger.Info("*** Database Config Session created *** ")
		logger.Info("----------------------------------- ")
	}

	DataDBSession = dbMap
	DatabaseNodeMap = dbNodeMap
}
