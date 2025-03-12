package maintenance

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	jsoniter "github.com/json-iterator/go"
	"strconv"
)

func UpdateTextCodec(db clickhouse.Conn, distributed bool, newCodec string) error {
	oldCodec, err := getSetting(db, distributed, "codec", "text")
	if err != nil {
		return err
	}
	if oldCodec == newCodec {
		return nil
	}
	for _, tbl := range [][]string{
		{"tempo_traces", "payload", "String"},
		{"samples_v4", "string", "String"},
	} {
		stream := jsoniter.ConfigFastest.BorrowStream(nil)
		stream.WriteRaw("ALTER TABLE ")
		stream.WriteRaw(tbl[0])
		stream.WriteRaw(" ALTER COLUMN `")
		stream.WriteRaw(tbl[1])
		stream.WriteRaw("` TYPE ")
		stream.WriteRaw(tbl[2])
		stream.WriteRaw(" CODEC(")
		stream.WriteRaw(newCodec)
		stream.WriteRaw(")")
		queryStr := string(stream.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream)
		err = db.Exec(context.Background(), queryStr)
		if err != nil {
			return err
		}
		//err = db.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s ALTER COLUMN `%s` TYPE %s CODEC(%s)",
		//	tbl[0], tbl[1], tbl[2], newCodec))
		//if err != nil {
		//	return err
		//}
	}
	return putSetting(db, "codec", "text", newCodec)
}

func UpdateLogsIndex(db clickhouse.Conn, distributed bool, newIndex string, newGranularity int) error {
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	stream.WriteRaw(newIndex)
	stream.WriteRaw(" GRANULARITY ")
	stream.WriteRaw(strconv.Itoa(newGranularity))
	idxName := string(stream.Buffer())
	jsoniter.ConfigFastest.ReturnStream(stream)
	//idxName := fmt.Sprintf("%s GRANULARITY %d", newIndex, newGranularity)
	oldIdx, err := getSetting(db, distributed, "index", "logs")
	if err != nil {
		return err
	}
	if oldIdx == idxName {
		return nil
	}
	db.Exec(context.Background(), "ALTER TABLE samples_v4 DROP INDEX _logs_idx")
	if newIndex != "" {

		stream2 := jsoniter.ConfigFastest.BorrowStream(nil)
		stream2.WriteRaw("ALTER TABLE samples_v4 ADD INDEX _logs_idx string TYPE ")
		stream2.WriteRaw(idxName)
		queryStr := string(stream2.Buffer())
		jsoniter.ConfigFastest.ReturnStream(stream2)

		err = db.Exec(context.Background(), queryStr)
		if err != nil {
			return err
		}
		//err = db.Exec(context.Background(), fmt.Sprintf("ALTER TABLE samples_v4 ADD INDEX _logs_idx string TYPE %s", idxName))
		//if err != nil {
		//	return err
		//}
	}
	return putSetting(db, "index", "logs", idxName)
}
