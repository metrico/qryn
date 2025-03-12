package maintenance

import (
	"context"
	"crypto/tls"
	clickhouse_v2 "github.com/ClickHouse/clickhouse-go/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/ctrl/logger"
	"time"
)

func ConnectV2(dbObject *config.ClokiBaseDataBase, database bool) (clickhouse_v2.Conn, error) {
	databaseName := ""
	if database {
		databaseName = dbObject.Name
	}
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw(dbObject.Host)
	stream.WriteRaw(":")
	// Use WriteUint32 if dbObject.Port is uint32
	stream.WriteUint32(dbObject.Port)
	addr := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	opt := &clickhouse_v2.Options{
		//Addr: []string{fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port)},
		Addr: []string{addr},
		Auth: clickhouse_v2.Auth{
			Database: databaseName,
			Username: dbObject.User,
			Password: dbObject.Password,
		},
		Debug:           dbObject.Debug,
		DialTimeout:     time.Second * 30,
		ReadTimeout:     time.Second * 30,
		MaxOpenConns:    10,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Settings: map[string]interface{}{
			"allow_experimental_database_replicated": "1",
			"materialize_ttl_after_modify":           "0",
		},
	}
	if dbObject.Secure {
		opt.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	return clickhouse_v2.Open(opt)
}

func InitDBTry(conn clickhouse_v2.Conn, clusterName string, dbName string, cloud bool, logger logger.ILogger) error {
	engine := ""
	onCluster := ""
	//if clusterName != "" {
	//	onCluster = fmt.Sprintf("ON CLUSTER `%s`", clusterName)
	//}
	if clusterName != "" {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ON CLUSTER `")
		stream.WriteRaw(clusterName)
		stream.WriteRaw("`")
		onCluster = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
	}
	//query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` %s %s", dbName, onCluster, engine)

	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("CREATE DATABASE IF NOT EXISTS ")
	stream.WriteRaw("`")
	stream.WriteRaw(dbName)
	stream.WriteRaw("` ")
	stream.WriteRaw(onCluster)
	stream.WriteRaw(" ")
	stream.WriteRaw(engine)
	query := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	logger.Info("Creating database: ", query)
	err := conn.Exec(MakeTimeout(), query)
	if err == nil {
		return nil
	}
	return err
}

func MakeTimeout() context.Context {
	res, _ := context.WithTimeout(context.Background(), time.Second*30)
	return res
}
