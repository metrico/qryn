package maintenance

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
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
		err = db.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s ALTER COLUMN `%s` TYPE %s CODEC(%s)",
			tbl[0], tbl[1], tbl[2], newCodec))
		if err != nil {
			return err
		}
	}
	return putSetting(db, "codec", "text", newCodec)
}

func UpdateLogsIndex(db clickhouse.Conn, distributed bool, newIndex string, newGranularity int) error {
	idxName := fmt.Sprintf("%s GRANULARITY %d", newIndex, newGranularity)
	oldIdx, err := getSetting(db, distributed, "index", "logs")
	if err != nil {
		return err
	}
	if oldIdx == idxName {
		return nil
	}
	db.Exec(context.Background(), "ALTER TABLE samples_v4 DROP INDEX _logs_idx")
	if newIndex != "" {
		err = db.Exec(context.Background(), fmt.Sprintf("ALTER TABLE samples_v4 ADD INDEX _logs_idx string TYPE %s", idxName))
		if err != nil {
			return err
		}
	}
	return putSetting(db, "index", "logs", idxName)
}
