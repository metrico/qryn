package maintenance

import (
	"context"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/ctrl/logger"
	"github.com/metrico/qryn/ctrl/qryn/heputils"
	"strconv"
	"strings"
	"time"
)

func getSetting(db clickhouse.Conn, dist bool, tp string, name string) (string, error) {
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("{\"type\":")
	stream.WriteRaw(strconv.Quote(tp))
	stream.WriteRaw(", \"name\":")
	stream.WriteRaw(strconv.Quote(name))
	// (Note: original code did not close the JSON object)
	fpStr := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(fpStr))
	//fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(
	//	fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name)),
	//))
	settings := "settings"
	if dist {
		settings += "_dist"
	}
	//	rows, err := db.Query(context.Background(),
	//		fmt.Sprintf(`SELECT argMax(value, inserted_at) as _value FROM %s WHERE fingerprint = $1
	//GROUP BY fingerprint HAVING argMax(name, inserted_at) != ''`, settings), fp)
	//	if err != nil {
	//		return "", err
	//	}
	//	res := ""
	//	for rows.Next() {
	//		err = rows.Scan(&res)
	//		if err != nil {
	//			return "", err
	//		}
	//	}
	//	return res, nil
	stream = jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("SELECT argMax(value, inserted_at) as _value FROM ")
	stream.WriteRaw(settings)
	stream.WriteRaw(" WHERE fingerprint = $1 \nGROUP BY fingerprint HAVING argMax(name, inserted_at) != ''")
	queryStr := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)

	rows, err := db.Query(context.Background(), queryStr, fp)
	if err != nil {
		return "", err
	}
	res := ""
	for rows.Next() {
		err = rows.Scan(&res)
		if err != nil {
			return "", err
		}
	}
	return res, nil
}

func putSetting(db clickhouse.Conn, tp string, name string, value string) error {
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw("{\"type\":")
	stream.WriteRaw(strconv.Quote(tp))
	stream.WriteRaw(", \"name\":")
	stream.WriteRaw(strconv.Quote(name))
	_name := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)

	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	err := db.Exec(context.Background(), "INSERT INTO settings (fingerprint, type, name, value, inserted_at)\nVALUES ($1, $2, $3, $4, NOW())", fp, tp, name, value)
	return err
	//	_name := fmt.Sprintf(`{"type":%s, "name":%s`, strconv.Quote(tp), strconv.Quote(name))
	//	fp := heputils.FingerprintLabelsDJBHashPrometheus([]byte(_name))
	//	err := db.Exec(context.Background(), `INSERT INTO settings (fingerprint, type, name, value, inserted_at)
	//VALUES ($1, $2, $3, $4, NOW())`, fp, tp, name, value)
	//	return err
}

func rotateTables(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, minTTL time.Duration,
	//	insertTimeExpression string, dropTTLExpression, settingName string,
	//	logger logger.ILogger, tables ...string) error {
	//	var rotateTTLArr []string
	//	for _, rp := range days {
	//		intsevalSec := int32(rp.TTL.Seconds())
	//		if intsevalSec < int32(minTTL.Seconds()) {
	//			intsevalSec = int32(minTTL.Seconds())
	//		}
	//		rotateTTL := fmt.Sprintf("%s + toIntervalSecond(%d)",
	//			insertTimeExpression,
	//			intsevalSec)
	//		if rp.MoveTo != "" {
	//			rotateTTL += fmt.Sprintf(" TO DISK '" + rp.MoveTo + "'")
	//		}
	//		rotateTTLArr = append(rotateTTLArr, rotateTTL)
	//	}
	//	rotateTTLArr = append(rotateTTLArr, dropTTLExpression)
	//	rotateTTLStr := strings.Join(rotateTTLArr, ", ")
	//
	//	onCluster := ""
	//	if clusterName != "" {
	//		onCluster = fmt.Sprintf(" ON CLUSTER `%s` ", clusterName)
	//	}
	//
	//	val, err := getSetting(db, distributed, "rotate", settingName)
	//	if err != nil || val == rotateTTLStr {
	//		return err
	//	}
	//	for _, table := range tables {
	//		q := fmt.Sprintf(`ALTER TABLE %s %s
	//MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192`, table, onCluster)
	//		logger.Debug(q)
	//		err = db.Exec(context.Background(), q)
	//		if err != nil {
	//			return fmt.Errorf("query: %s\nerror: %v", q, err)
	//		}
	//		logger.Debug("Request OK")
	//		logger.Debug(q)
	//		q = fmt.Sprintf(`ALTER TABLE %s %s MODIFY TTL %s`, table, onCluster, rotateTTLStr)
	//		err = db.Exec(context.Background(), q)
	//		if err != nil {
	//			return fmt.Errorf("query: %s\nerror: %v", q, err)
	//		}
	//		logger.Debug("Request OK")
	//	}
	//	return putSetting(db, "rotate", settingName, rotateTTLStr)
	insertTimeExpression string, dropTTLExpression, settingName string,
	logger logger.ILogger, tables ...string) error {

	var rotateTTLArr []string
	for _, rp := range days {
		intsevalSec := int32(rp.TTL.Seconds())
		if intsevalSec < int32(minTTL.Seconds()) {
			intsevalSec = int32(minTTL.Seconds())
		}
		// Build: <insertTimeExpression> + toIntervalSecond(<intsevalSec>)
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw(insertTimeExpression)
		stream.WriteRaw(" + toIntervalSecond(")
		stream.WriteRaw(strconv.Itoa(int(intsevalSec)))
		stream.WriteRaw(")")
		rotateTTL := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)

		if rp.MoveTo != "" {
			stream = jsoniter.ConfigFastest.BorrowStream(nil)
			stream.WriteRaw(" TO DISK '")
			stream.WriteRaw(rp.MoveTo)
			stream.WriteRaw("'")
			rotateTTL += string(stream.Buffer())
			jsoniter.ConfigFastest.ReturnStream(stream)
		}
		rotateTTLArr = append(rotateTTLArr, rotateTTL)
	}
	rotateTTLArr = append(rotateTTLArr, dropTTLExpression)
	rotateTTLStr := strings.Join(rotateTTLArr, ", ")

	onCluster := ""
	if clusterName != "" {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw(" ON CLUSTER `")
		stream.WriteRaw(clusterName)
		stream.WriteRaw("` ")
		onCluster = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
	}

	val, err := getSetting(db, distributed, "rotate", settingName)
	if err != nil || val == rotateTTLStr {
		return err
	}
	for _, table := range tables {
		// Build query: ALTER TABLE <table> <onCluster>
		// MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ALTER TABLE ")
		stream.WriteRaw(table)
		stream.WriteRaw(" ")
		stream.WriteRaw(onCluster)
		stream.WriteRaw("\nMODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192")
		q := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		logger.Debug(q)
		err = db.Exec(context.Background(), q)
		if err != nil {
			stream = jsoniter.ConfigFastest.BorrowStream(nil)
			stream.WriteRaw("query: ")
			stream.WriteRaw(q)
			stream.WriteRaw("\nerror: ")
			stream.WriteRaw(strconv.Quote(err.Error()))
			errMsg := string(stream.Buffer())
			jsoniter.ConfigFastest.ReturnStream(stream)
			return errors.New(errMsg)
		}
		logger.Debug("Request OK")
		logger.Debug(q)

		stream = jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ALTER TABLE ")
		stream.WriteRaw(table)
		stream.WriteRaw(" ")
		stream.WriteRaw(onCluster)
		stream.WriteRaw(" MODIFY TTL ")
		stream.WriteRaw(rotateTTLStr)
		q = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		err = db.Exec(context.Background(), q)
		if err != nil {
			stream = jsoniter.ConfigFastest.BorrowStream(nil)
			stream.WriteRaw("query: ")
			stream.WriteRaw(q)
			stream.WriteRaw("\nerror: ")
			stream.WriteRaw(strconv.Quote(err.Error()))
			errMsg := string(stream.Buffer())
			jsoniter.ConfigFastest.ReturnStream(stream)
			return errors.New(errMsg)
		}
		logger.Debug("Request OK")
	}
	return putSetting(db, "rotate", settingName, rotateTTLStr)
}

func storagePolicyUpdate(db clickhouse.Conn, clusterName string,
	distributed bool, storagePolicy string, setting string, tables ...string) error {
	//onCluster := ""
	//if clusterName != "" {
	//	onCluster = fmt.Sprintf(" ON CLUSTER `%s` ", clusterName)
	//}
	//val, err := getSetting(db, distributed, "rotate", setting)
	//if err != nil || storagePolicy == "" || val == storagePolicy {
	//	return err
	//}
	//for _, tbl := range tables {
	//	err = db.Exec(context.Background(), fmt.Sprintf(`ALTER TABLE %s %s MODIFY SETTING storage_policy=$1`,
	//		tbl, onCluster), storagePolicy)
	//	if err != nil {
	//		return err
	//	}
	//}
	//return putSetting(db, "rotate", setting, storagePolicy)
	onCluster := ""
	if clusterName != "" {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw(" ON CLUSTER `")
		stream.WriteRaw(clusterName)
		stream.WriteRaw("` ")
		onCluster = string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
	}
	val, err := getSetting(db, distributed, "rotate", setting)
	if err != nil || storagePolicy == "" || val == storagePolicy {
		return err
	}
	for _, tbl := range tables {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ALTER TABLE ")
		stream.WriteRaw(tbl)
		stream.WriteRaw(" ")
		stream.WriteRaw(onCluster)
		stream.WriteRaw(" MODIFY SETTING storage_policy=$1")
		qStr := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		err = db.Exec(context.Background(), qStr, storagePolicy)
		if err != nil {
			return err
		}
	}
	return putSetting(db, "rotate", setting, storagePolicy)
}

type RotatePolicy struct {
	TTL    time.Duration
	MoveTo string
}

func Rotate(db clickhouse.Conn, clusterName string, distributed bool, days []RotatePolicy, dropTTLDays int,
	storagePolicy string, logger logger.ILogger) error {
	//TODO: add pluggable extension
	err := storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "v3_storage_policy",
		"time_series", "time_series_gin", "samples_v3")
	if err != nil {
		return err
	}
	err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "v1_traces_storage_policy",
		"tempo_traces", "tempo_traces_attrs_gin", "tempo_traces_kv")
	if err != nil {
		return err
	}
	err = storagePolicyUpdate(db, clusterName, distributed, storagePolicy, "metrics_15s", "metrics_15s")
	if err != nil {
		return err
	}

	//logDefaultTTLString := func(column string) string {
	//	return fmt.Sprintf(
	//		"%s + toIntervalDay(%d)",
	//		column, dropTTLDays)
	//}

	logDefaultTTLString := func(column string) string {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw(column)
		stream.WriteRaw(" + toIntervalDay(")
		stream.WriteRaw(strconv.Itoa(dropTTLDays))
		stream.WriteRaw(")")
		ret := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		return ret
	}
	//tracesDefaultTTLString := func(column string) string {
	//	return fmt.Sprintf(
	//		"%s + toIntervalDay(%d)",
	//		column, dropTTLDays)
	//}
	tracesDefaultTTLString := func(column string) string {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw(column)
		stream.WriteRaw(" + toIntervalDay(")
		stream.WriteRaw(strconv.Itoa(dropTTLDays))
		stream.WriteRaw(")")
		ret := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		return ret
	}
	minTTL := time.Minute
	dayTTL := time.Hour * 24

	err = rotateTables(
		db,
		clusterName,
		distributed,
		days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		logDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"v3_samples_days", logger, "samples_v3")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		dayTTL,
		"date",
		logDefaultTTLString("date"), "v3_time_series_days", logger,
		"time_series", "time_series_gin")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		tracesDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"v1_traces_days",
		logger, "tempo_traces")
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		dayTTL,
		"date",
		tracesDefaultTTLString("date"), "tempo_attrs_v1",
		logger, "tempo_traces_attrs_gin", "tempo_traces_kv")
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	err = rotateTables(db, clusterName, distributed, days,
		minTTL,
		"toDateTime(timestamp_ns / 1000000000)",
		logDefaultTTLString("toDateTime(timestamp_ns / 1000000000)"),
		"metrics_15s",
		logger, "metrics_15s")
	if err != nil {
		return err
	}

	return nil
}
