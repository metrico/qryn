package writer

import (
	"context"
	"fmt"
	"github.com/metrico/qryn/writer/ch_wrapper"
	"github.com/metrico/qryn/writer/config"
	"github.com/metrico/qryn/writer/utils/logger"
	"runtime"
	"strconv"
	"time"
)

type SetupState struct {
	Version         string
	Type            string
	Shards          int
	SamplesChannels int
	TSChannels      int
	Preforking      bool
	Forks           int
}

func (s SetupState) ToLogLines() []string {
	shards := strconv.FormatInt(int64(s.Shards), 10)
	if s.Shards == 0 {
		shards = "can't retrieve"
	}
	return []string{
		"QRYN-WRITER SETTINGS:",
		"qryn-writer version: " + s.Version,
		"clickhouse setup type: " + s.Type,
		"shards: " + shards,
		"samples channels: " + strconv.FormatInt(int64(s.SamplesChannels), 10),
		"time-series channels: " + strconv.FormatInt(int64(s.TSChannels), 10),
		fmt.Sprintf("preforking: %v", s.Preforking),
		"forks: " + strconv.FormatInt(int64(s.Forks), 10),
	}
}

func checkSetup(conn ch_wrapper.IChClient) SetupState {
	setupType := "single-server"
	if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" && config.Cloki.Setting.DATABASE_DATA[0].Cloud {
		setupType = "Distributed + Replicated"
	} else if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" {
		setupType = "Distributed"
	} else if config.Cloki.Setting.DATABASE_DATA[0].Cloud {
		setupType = "Replicated"
	}
	shards := 1
	if config.Cloki.Setting.DATABASE_DATA[0].ClusterName != "" {
		shards = getShardsNum(conn, config.Cloki.Setting.DATABASE_DATA[0].ClusterName)
	}
	forks := 1
	if config.Cloki.Setting.HTTP_SETTINGS.Prefork {
		forks = runtime.GOMAXPROCS(0)
	}
	return SetupState{
		Version:         "",
		Type:            setupType,
		Shards:          shards,
		SamplesChannels: config.Cloki.Setting.SYSTEM_SETTINGS.ChannelsSample,
		TSChannels:      config.Cloki.Setting.SYSTEM_SETTINGS.ChannelsTimeSeries,
		Preforking:      config.Cloki.Setting.HTTP_SETTINGS.Prefork,
		Forks:           forks,
	}
}

func getShardsNum(conn ch_wrapper.IChClient, clusterName string) int {
	to, _ := context.WithTimeout(context.Background(), time.Second*30)
	rows, err := conn.Query(to, "select count(distinct shard_num) from system.clusters where cluster=$1", clusterName)
	if err != nil {
		logger.Error("[GSN001] Get shards error: ", err)
		return 0
	}
	defer rows.Close()
	var res uint64
	rows.Next()
	err = rows.Scan(&res)
	if err != nil {
		logger.Error("[GSN002] Get shards error: ", err)
		return 0
	}
	return int(res)
}
