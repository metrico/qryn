package maintenance

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/ctrl/logger"
	"github.com/metrico/qryn/ctrl/maintenance"
	"strings"
	"time"
)

func upgradeDB(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) error {
	conn, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return err
	}
	mode := CLUST_MODE_SINGLE
	if dbObject.Cloud {
		mode = CLUST_MODE_CLOUD
	}
	if dbObject.ClusterName != "" {
		mode |= CLUST_MODE_DISTRIBUTED
	}
	//if dbObject.TTLDays == 0 {
	//	return fmt.Errorf("ttl_days should be set for node#%s", dbObject.Node)
	//}

	if dbObject.TTLDays == 0 {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ttl_days should be set for node#")
		stream.WriteRaw(dbObject.Node)
		errMsg := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		return errors.New(errMsg)
	}
	return Update(conn, dbObject.Name, dbObject.ClusterName, mode, dbObject.TTLDays,
		dbObject.StoragePolicy, dbObject.SamplesOrdering, dbObject.SkipUnavailableShards, logger)
}

func InitDB(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) error {
	if dbObject.Name == "" || dbObject.Name == "default" {
		return nil
	}
	conn, err := maintenance.ConnectV2(dbObject, false)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = maintenance.InitDBTry(conn, dbObject.ClusterName, dbObject.Name, dbObject.Cloud, logger)
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("SHOW CREATE DATABASE `")
	stream.WriteRaw(dbObject.Name)
	stream.WriteRaw("`")
	queryStr := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	//rows, err := conn.Query(maintenance.MakeTimeout(), fmt.Sprintf("SHOW CREATE DATABASE `%s`", dbObject.Name))
	//if err != nil {
	//	return err
	//}
	rows, err := conn.Query(maintenance.MakeTimeout(), queryStr)
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	var create string
	err = rows.Scan(&create)
	if err != nil {
		return err
	}
	logger.Info(create)
	return nil
}

func TestDistributed(dbObject *config.ClokiBaseDataBase, logger logger.ILogger) (bool, error) {
	if dbObject.ClusterName == "" {
		return false, nil
	}
	conn, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	// Build onCluster: ON CLUSTER `<dbObject.ClusterName>`
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("ON CLUSTER `")
	stream.WriteRaw(dbObject.ClusterName)
	stream.WriteRaw("`")
	onCluster := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)

	logger.Info("TESTING Distributed table support")

	// Build query: CREATE TABLE IF NOT EXISTS dtest <onCluster> (a UInt64) Engine = Null
	stream = jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("CREATE TABLE IF NOT EXISTS dtest ")
	stream.WriteRaw(onCluster)
	stream.WriteRaw(" (a UInt64) Engine = Null")
	q := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	logger.Info(q)
	err = conn.Exec(maintenance.MakeTimeout(), q)
	if err != nil {
		return false, err
	}
	// Build DROP TABLE query for dtest
	stream = jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("DROP TABLE dtest ")
	stream.WriteRaw(onCluster)
	q = string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	defer conn.Exec(maintenance.MakeTimeout(), q)

	// Build query for dtest_dist:
	// CREATE TABLE IF NOT EXISTS dtest_dist <onCluster> (a UInt64) Engine = Distributed('<ClusterName>', '<Name>', 'dtest', a)
	stream = jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("CREATE TABLE IF NOT EXISTS dtest_dist ")
	stream.WriteRaw(onCluster)
	stream.WriteRaw(" (a UInt64) Engine = Distributed('")
	stream.WriteRaw(dbObject.ClusterName)
	stream.WriteRaw("', '")
	stream.WriteRaw(dbObject.Name)
	stream.WriteRaw("', 'dtest', a)")
	q = string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	logger.Info(q)
	err = conn.Exec(maintenance.MakeTimeout(), q)
	if err != nil {
		logger.Error("Distributed creation error: ", err.Error())
		if strings.Contains(err.Error(), "Only tables with a Replicated engine") {
			logger.Info("Probably CH Cloud DEV. No Dist support.")
			return false, nil
		}
		return false, err
	}
	// Build DROP TABLE query for dtest_dist
	stream = jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("DROP TABLE dtest_dist ")
	stream.WriteRaw(onCluster)
	q = string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	defer conn.Exec(maintenance.MakeTimeout(), q)
	logger.Info("Distributed support ok")
	return true, nil

	//if dbObject.ClusterName == "" {
	//	return false, nil
	//}
	//conn, err := maintenance.ConnectV2(dbObject, true)
	//if err != nil {
	//	return false, err
	//}
	//defer conn.Close()
	//onCluster := "ON CLUSTER `" + dbObject.ClusterName + "`"
	//logger.Info("TESTING Distributed table support")
	//q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS dtest %s (a UInt64) Engine = Null", onCluster)
	//logger.Info(q)
	//err = conn.Exec(maintenance.MakeTimeout(), q)
	//if err != nil {
	//	return false, err
	//}
	//defer conn.Exec(maintenance.MakeTimeout(), fmt.Sprintf("DROP TABLE dtest %s", onCluster))
	//q = fmt.Sprintf("CREATE TABLE IF NOT EXISTS dtest_dist %s (a UInt64) Engine = Distributed('%s', '%s', 'dtest', a)",
	//	onCluster, dbObject.ClusterName, dbObject.Name)
	//logger.Info(q)
	//err = conn.Exec(maintenance.MakeTimeout(), q)
	//if err != nil {
	//	logger.Error("Distributed creation error: ", err.Error())
	//	if strings.Contains(err.Error(), "Only tables with a Replicated engine or tables which do not store data on disk are allowed in a Replicated database") {
	//		logger.Info("Probably CH Cloud DEV. No Dist support.")
	//		return false, nil
	//	}
	//	return false, err
	//}
	//defer conn.Exec(maintenance.MakeTimeout(), fmt.Sprintf("DROP TABLE dtest_dist %s", onCluster))
	//logger.Info("Distributed support ok")
	//return true, nil
}

func rotateDB(dbObject *config.ClokiBaseDataBase) error {
	connDb, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return err
	}
	defer connDb.Close()
	ttlPolicy := make([]RotatePolicy, len(dbObject.TTLPolicy))
	for i, p := range dbObject.TTLPolicy {
		d, err := time.ParseDuration(p.Timeout)
		if err != nil {
			return err
		}
		ttlPolicy[i] = RotatePolicy{
			TTL:    d,
			MoveTo: p.MoveTo,
		}
	}
	return Rotate(connDb, dbObject.ClusterName, dbObject.ClusterName != "",
		ttlPolicy, dbObject.TTLDays, dbObject.StoragePolicy, logger.Logger)
}

func RecodecDB(dbObject *config.ClokiBaseDataBase) error {
	connDb, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return err
	}
	defer connDb.Close()
	return UpdateTextCodec(connDb, dbObject.ClusterName != "", dbObject.TextCodec)
}

func ReindexDB(dbObject *config.ClokiBaseDataBase) error {
	connDb, err := maintenance.ConnectV2(dbObject, true)
	if err != nil {
		return err
	}
	defer connDb.Close()
	return UpdateLogsIndex(connDb, dbObject.ClusterName != "", dbObject.LogsIndex, int(dbObject.LogsIndexGranularity))
}

func UpgradeAll(config []config.ClokiBaseDataBase, logger logger.ILogger) error {
	//for _, dbObject := range config {
	//	logger.Info(fmt.Sprintf("Upgrading %s:%d/%s", dbObject.Host, dbObject.Port, dbObject.Name))
	//	err := upgradeDB(&dbObject, logger)
	//	if err != nil {
	//		return err
	//	}
	//	logger.Info(fmt.Sprintf("Upgrading %s:%d/%s: OK", dbObject.Host, dbObject.Port, dbObject.Name))
	//}
	//return nil

	for _, dbObject := range config {
		// Build message: "Upgrading <Host>:<Port>/<Name>"
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("Upgrading ")
		stream.WriteRaw(dbObject.Host)
		stream.WriteRaw(":")
		stream.WriteUint32(dbObject.Port)
		stream.WriteRaw("/")
		stream.WriteRaw(dbObject.Name)
		msg := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		logger.Info(msg)
		err := upgradeDB(&dbObject, logger)
		if err != nil {
			return err
		}
		// Build OK message: "Upgrading <Host>:<Port>/<Name>: OK"
		stream = jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("Upgrading ")
		stream.WriteRaw(dbObject.Host)
		stream.WriteRaw(":")
		stream.WriteUint32(dbObject.Port)
		stream.WriteRaw("/")
		stream.WriteRaw(dbObject.Name)
		stream.WriteRaw(": OK")
		msg = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		logger.Info(msg)
	}
	return nil
}

func RotateAll(base []config.ClokiBaseDataBase, logger logger.ILogger) error {
	for _, dbObject := range base {
		//logger.Info(fmt.Sprintf("Rotating %s:%d/%s", dbObject.Host, dbObject.Port, dbObject.Name))
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("Rotating ")
		stream.WriteRaw(dbObject.Host)
		stream.WriteRaw(":")
		stream.WriteUint32(dbObject.Port)
		stream.WriteRaw("/")
		stream.WriteRaw(dbObject.Name)
		msg := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		logger.Info(msg)
		err := rotateDB(&dbObject)
		if err != nil {
			return err
		}
		//logger.Info(fmt.Sprintf("Rotating %s:%d/%s: OK", dbObject.Host, dbObject.Port, dbObject.Name))
		stream = jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("Rotating ")
		stream.WriteRaw(dbObject.Host)
		stream.WriteRaw(":")
		stream.WriteUint32(dbObject.Port)
		stream.WriteRaw("/")
		stream.WriteRaw(dbObject.Name)
		stream.WriteRaw(": OK")
		msg = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		logger.Info(msg)
	}

	/*for _, dbObject := range base {
		err := RecodecDB(&dbObject)
		if err != nil {
			return err
		}
		err = ReindexDB(&dbObject)
		if err != nil {
			return err
		}
	}*/
	return nil
}
