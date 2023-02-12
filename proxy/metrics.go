package proxy

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/procyon-projects/chrono"
	// "github.com/datastax/cql-proxy/parser"
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

// stats[timestamp]["keyspace.table"]["select_count"] = 12345
type statsrecord map[time.Time]map[string]*metricRow

type statsManager struct {
	id          string
	config      *runConfig
	stats       statsrecord
	ctx         context.Context
	MessageFeed chan *RequestResponse
}

var (
	once sync.Once
	sm   *statsManager
)

var systemTables = []string{"local", "peers", "peers_v2", "schema_keyspaces", "schema_columnfamilies", "schema_columns", "schema_usertypes"}

func NewStatsManager(ctx context.Context, config *runConfig, proxy *Proxy) (*statsManager, error) {
	var err error
	if config.TrackUsage {
		once.Do(func() {
			timebucket := time.Now().Round(time.Hour)
			sm = &statsManager{
				id:          uuid.NewString(),
				config:      config,
				stats:       map[time.Time]map[string]*metricRow{},
				ctx:         ctx,
				MessageFeed: make(chan *RequestResponse, 1024),
			}
			sm.stats[timebucket] = map[string]*metricRow{}

			maybeCreateMetricsTable(ctx, config, proxy)
			go periodicallyFlush(ctx, config, proxy)
			go listen()
		})
		return sm, err
	} else {
		return sm, nil
	}
}

func listen() {
	fmt.Println("Listening for metrics")
	for {
		select {
		case req := <-sm.MessageFeed:
			handleQuery(req)
		case <-sm.ctx.Done():
			return
		}
	}
}

func periodicallyFlush(ctx context.Context, config *runConfig, proxy *Proxy) {
	taskScheduler := chrono.NewDefaultTaskScheduler()

	_, err := taskScheduler.ScheduleWithFixedDelay(func(ctx context.Context) {
		fmt.Println("Flushing stats")
		flushCurrentStats(ctx, config, proxy)
		purgeOldStats()
	}, 60*time.Second)

	if err == nil {
		fmt.Println("Scheduled periodic flush")
	}
}

func flushCurrentStats(ctx context.Context, config *runConfig, proxy *Proxy) {
	// sm.stats[timestamp]["keyspace.table"]["select_count"] = 12345
	//timebucket := time.Now().Round(time.Hour)
	for timebucket, timeEntries := range sm.stats {
		for table_ref, mr := range timeEntries {
			statement := fmt.Sprintf(`INSERT INTO %s.%s (
				time_bucket, client_id, table_ref, 
				select_count, select_size, select_rrus, 
				insert_count, insert_size, insert_wrus, 
				update_count, update_size, update_wrus, 
				delete_count, delete_size, delete_wrus, 
				lwt_count, lwt_size, lwt_rrus, lwt_wrus, 
				index_wrus, 
				writes_size, reads_size, wrus, rrus) 
			VALUES (
				%v, %s, '%s', 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, 
				%v, %v, %v, %v, 
				%v, 
				%v, %v, %v, %v);`,
				config.UsageKeyspace, config.UsageTable,
				timebucket.UnixMilli(), sm.id, table_ref,
				mr.select_count, mr.select_size, mr.select_rrus,
				mr.insert_count, mr.insert_size, mr.insert_wrus,
				mr.update_count, mr.update_size, mr.update_wrus,
				mr.delete_count, mr.delete_size, mr.delete_wrus,
				mr.lwt_count, mr.lwt_size, mr.lwt_rrus, mr.lwt_wrus,
				mr.index_wrus,
				mr.writes_size, mr.reads_size, mr.wrus, mr.rrus)
			statement = strings.ReplaceAll(statement, "\n", "")
			statement = strings.ReplaceAll(statement, "\t", "")
			rs, err := proxy.cluster.ExecuteControlQuery(ctx, statement)
			if err == nil && rs == nil {
				proxy.logger.Info("Probably saved correctly...  ;-)")
			} else if err != nil {
				proxy.logger.Error("Error upserting stats!  Tried to execute:")
				proxy.logger.Error(statement)
			}
		}
	}
}

func purgeOldStats() {
	// TODO
}

// to do this "correctly" eventually I'll need to be able to:
//   - size the parameters into a write or the results of the write...?
//   - add the size of the writetimes for each non-partition-key field
//   - don't charge for system tables
func handleQuery(reqres *RequestResponse) {
	var keyspaceTableName string

	switch msg := reqres.req.msg.(type) {
	case *partialQuery:
		query := msg.query
		// reqres.req.client.proxy.logger.Info("Response Size:", zap.Int32("message size in bytes", reqres.req.raw.Header.BodyLength))
		// reqres.req.client.proxy.logger.Info("handling query", zap.String("Query", query))

		re := regexp.MustCompile(`(?i)FROM\s*([a-zA-Z._]*)\s*`)
		sys_re := regexp.MustCompile(`^system`)
		tables := re.FindStringSubmatch(query)

		if len(tables) == 0 {
			return // TODO: do something more interesting here, like panic
		} else if !strings.Contains(tables[1], ".") {
			keyspace := strings.ReplaceAll(reqres.req.keyspace, "\"", "")
			keyspaceTableName = keyspace + "." + tables[1]
		} else {
			keyspaceTableName = tables[1]
		}

		// if system table tracking is off and the working keyspace name starts with "system", skip
		if !sm.config.TrackSystemUsage && sys_re.MatchString(keyspaceTableName) {
			return
		}

		timebucket := time.Now().Round(time.Hour)

		mr, ok := sm.stats[timebucket][keyspaceTableName]
		if !ok {
			mr = NewMetricRow()
			//sm.stats[timebucket] = map[string]*metricRow{}
			sm.stats[timebucket][keyspaceTableName] = mr
		}

		if isSystemTable(query) {
			return
		} else if match, _ := regexp.MatchString(`(?i)^\s*SELECT`, query); match {
			handleSELECT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)(IF NOT EXISTS|IF EXISTS)`, query); match {
			// this case still needs some way to handle UPDATE ... IF ...
			handleLWT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*INSERT`, query); match {
			handleINSERT(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*UPDATE`, query); match {
			handleUPDATE(reqres, keyspaceTableName, timebucket, mr)
		} else if match, _ := regexp.MatchString(`(?i)^\s*DELETE`, query); match {
			handleDELETE(reqres, keyspaceTableName, timebucket, mr)
		}
	default:
		reqres.req.client.proxy.logger.Error("Unknown message type")
	}
}

func handleSELECT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	rrus := uint64(math.Ceil(float64(reqres_size) / 4096))

	mr.select_count++
	mr.select_size = mr.select_size + reqres_size
	mr.select_rrus = mr.select_rrus + rrus

	mr.reads_size = mr.reads_size + reqres_size
	mr.rrus = mr.rrus + rrus

	sm.stats[timebucket][keyspaceTableName] = mr
}

// LWT is counted according to a regular write but with +1 RRU
func handleLWT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / 1024))

	mr.lwt_count++
	mr.lwt_size = mr.lwt_size + reqres_size
	mr.lwt_wrus = mr.lwt_wrus + wrus
	mr.lwt_rrus = mr.lwt_rrus + 1

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus
	mr.rrus = mr.rrus + 1

	sm.stats[timebucket][keyspaceTableName] = mr
}

func handleINSERT(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / 1024))

	mr.insert_count++
	mr.insert_size = mr.insert_size + reqres_size
	mr.insert_wrus = mr.insert_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	sm.stats[timebucket][keyspaceTableName] = mr
}

func handleUPDATE(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / 1024))

	mr.update_count++
	mr.update_size = mr.update_size + reqres_size
	mr.update_wrus = mr.update_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	sm.stats[timebucket][keyspaceTableName] = mr
}

func handleDELETE(reqres *RequestResponse, keyspaceTableName string, timebucket time.Time, mr *metricRow) {
	req_size := reqres.req.raw.Header.BodyLength
	res_size := reqres.res.Header.BodyLength
	reqres_size := uint64(req_size) + uint64(res_size)
	wrus := uint64(math.Ceil(float64(reqres_size) / 1024))

	mr.delete_count++
	mr.delete_size = mr.delete_size + reqres_size
	mr.delete_wrus = mr.delete_wrus + wrus

	mr.writes_size = mr.writes_size + reqres_size
	mr.wrus = mr.wrus + wrus

	sm.stats[timebucket][keyspaceTableName] = mr
}

func isSystemTable(name string) bool {
	for _, table := range systemTables {
		if name == table {
			return true
		}
	}
	return false
}

// initialize the metrics table if it doesn't exist
func maybeCreateMetricsTable(ctx context.Context, config *runConfig, proxy *Proxy) {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		time_bucket timestamp,
		client_id uuid,
		table_ref text,

		select_count bigint,
		select_size bigint,
		select_rrus bigint,

		insert_count bigint,
		insert_size bigint,
		insert_wrus bigint,

		update_count bigint,
		update_size bigint,
		update_wrus bigint,

		delete_count bigint,
		delete_size bigint,
		delete_wrus bigint,

		lwt_count bigint,
		lwt_size bigint,
		lwt_rrus bigint,
		lwt_wrus bigint,

		index_wrus bigint,

		writes_size bigint,
		reads_size bigint,
		wrus bigint,
		rrus bigint,

		PRIMARY KEY ((time_bucket), client_id, table_ref));`, config.UsageKeyspace, config.UsageTable)

	rs, err := proxy.cluster.ExecuteControlQuery(ctx, query)

	if err == nil && rs == nil {
		proxy.logger.Info("Stats table created or already exists.")
	} else if err != nil {
		proxy.logger.Error("Error initializing usage stats table!")
	}
}

// /* UUID code borrowed from go-cql (https://github.com/gocql/) */
// type UUID [16]byte

// func MustRandomUUID() UUID {
// 	uuid, err := RandomUUID()
// 	if err != nil {
// 		panic(err)
// 	}
// 	return uuid
// }

// // RandomUUID generates a totally random UUID (version 4) as described in
// // RFC 4122.
// func RandomUUID() (UUID, error) {
// 	var u UUID
// 	_, err := io.ReadFull(rand.Reader, u[:])
// 	if err != nil {
// 		return u, err
// 	}
// 	u[6] &= 0x0F // clear version
// 	u[6] |= 0x40 // set version to 4 (random uuid)
// 	u[8] &= 0x3F // clear variant
// 	u[8] |= 0x80 // set to IETF variant
// 	return u, nil
// }
