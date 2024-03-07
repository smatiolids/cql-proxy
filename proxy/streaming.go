package proxy

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"go.uber.org/zap"
)

type metricRow struct {
	select_count uint64
	select_size  uint64
	select_rrus  uint64
	insert_count uint64
	insert_size  uint64
	insert_wrus  uint64
	update_count uint64
	update_size  uint64
	update_wrus  uint64
	delete_count uint64
	delete_size  uint64
	delete_wrus  uint64
	lwt_count    uint64
	lwt_size     uint64
	lwt_rrus     uint64
	lwt_wrus     uint64
	index_wrus   uint64
	writes_size  uint64
	reads_size   uint64
	wrus         uint64
	rrus         uint64
}

func NewMetricRow() *metricRow {
	mr := &metricRow{}

	mr.select_count = 0
	mr.select_size = 0
	mr.select_rrus = 0
	mr.insert_count = 0
	mr.insert_size = 0
	mr.insert_wrus = 0
	mr.update_count = 0
	mr.update_size = 0
	mr.update_wrus = 0
	mr.delete_count = 0
	mr.delete_size = 0
	mr.delete_wrus = 0
	mr.lwt_count = 0
	mr.lwt_size = 0
	mr.lwt_rrus = 0
	mr.lwt_wrus = 0
	mr.index_wrus = 0
	mr.writes_size = 0
	mr.reads_size = 0
	mr.wrus = 0
	mr.rrus = 0

	return mr
}

// sm.stats[timestamp]["keyspace.table"]["select_count"] = 12345
type statsrecord map[time.Time]map[string]*metricRow

// sm.counts[timestamp]["keyspace.table"]["reads|writes|all"][5|9|234...] = 1232455
type counts map[time.Time]map[string]map[string]map[uint64]uint64

// sm.latencies[timestamp]["keyspace.table"][reads|writes] = hdrhistogram.Histogram
type latencies map[time.Time]map[string]map[string]*hdrhistogram.Histogram

type streamingManager struct {
	id          string
	config      *runConfig
	MessageFeed chan *RequestResponse
	ctx         context.Context
}

var (
	once sync.Once
	sm   *streamingManager
)

var systemTables = []string{"local", "peers", "peers_v2", "schema_keyspaces", "schema_columnfamilies", "schema_columns", "schema_usertypes"}

func SingletonStreamingManager(ctx context.Context, config *runConfig, proxy *Proxy) (*streamingManager, error) {
	var err error
	if config.WriteToStreaming {
		once.Do(func() {
			proxy.logger.Debug("Starting writes to Streaming")
			go listen()
		})
		return sm, err
	} else {
		return sm, nil
	}
}

func listen() {
	fmt.Println("Listening for writes to streaming")
	for {
		select {
		case req := <-sm.MessageFeed:
			fmt.Println(req)
			// handleQuery(req)
		case <-sm.ctx.Done():
			return
		}
	}
}

func handleQuery(reqres *RequestResponse) {
	var query string

	reqres.req.client.proxy.logger.Debug("Time Tracker: ", zap.Duration("Response Time", reqres.req.rt))

	switch msg := reqres.req.msg.(type) {
	case *partialExecute:
		if prepare, ok := reqres.req.client.proxy.preparedCache.Load(hex.EncodeToString(msg.queryId)); !ok {
			reqres.req.client.proxy.logger.Error("partialExecute ERROR")
		} else {
			query = string(prepare.PreparedFrame.Body[4:])
		}
	case *partialQuery:
		query = msg.query
	case *partialBatch:
		reqres.req.client.proxy.logger.Debug("partialBatch not currently implemented")
	default:
		reqres.req.client.proxy.logger.Error("Unknown message type")
		fmt.Println(msg)
	}

	if len(query) > 0 {
		// INSERT INTO keyspace.table (fields...) VALUES (values...)
		// UPDATE keyspace.table [USING] SET field=value, ...
		// SELECT (*|fields|DISTINCT partition) FROM keyspace.table WHERE predicate=value AND ...
		// DELETE [(fields...)] FROM keyspace.table [USING] WHERE predicate=value AND ...
		find_tables := regexp.MustCompile(`(?i)(INTO|UPDATE|FROM)\s*([a-zA-Z0-9._]*)\s*`)
		// query = strings.ReplaceAll(query, "\n", " ")
		tables := find_tables.FindStringSubmatch(query)

		if len(tables) == 0 {
			reqres.req.client.proxy.logger.Debug("Encountered an unhandled query/statement", zap.String("query:", query))
			return // TODO: do something more interesting here, like panic
		}
		if isSystemTable(query) {
			return
		} else if match, _ := regexp.MatchString(`(?i)^\s*SELECT`, query); match {
			fmt.Println("SELECT")
		} else if match, _ := regexp.MatchString(`(?i)(IF NOT EXISTS|IF EXISTS)`, query); match {
			// this case still needs some way to handle UPDATE ... IF ...
			fmt.Println("LWT")
		} else if match, _ := regexp.MatchString(`(?i)^\s*INSERT`, query); match {
			fmt.Println("INSERT")
		} else if match, _ := regexp.MatchString(`(?i)^\s*UPDATE`, query); match {
			fmt.Println("UPDATE")
		} else if match, _ := regexp.MatchString(`(?i)^\s*DELETE`, query); match {
			fmt.Println("DELETE")
		}
	}
}

func isSystemTable(name string) bool {
	for _, table := range systemTables {
		if name == table {
			return true
		}
	}
	return false
}
