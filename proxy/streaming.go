package proxy

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.uber.org/zap"
)

type streamingManager struct {
	config         *runConfig
	ctx            context.Context
	MessageFeed    chan *RequestResponse
	pulsarClient   pulsar.Client
	pulsarProducer pulsar.Producer
}

type ProducerService struct {
}

type messageType struct {
	Query string    `json:"query"`
	TS    time.Time `json:"ts"`
}

var (
	once             sync.Once
	sm               *streamingManager
	messageSchemaDef = "{\"type\":\"record\",\"name\":\"inter_cloud_replication\",\"namespace\":\"default\"," +
		"\"fields\":[" +
		"{\"name\":\"ts\",\"type\":\"string\"}," +
		"{\"name\":\"query\",\"type\":\"string\"}" +
		"]}"
)

var systemTables = []string{"local", "peers", "peers_v2", "schema_keyspaces", "schema_columnfamilies", "schema_columns", "schema_usertypes"}

func SingletonStreamingManager(ctx context.Context, config *runConfig, proxy *Proxy) (*streamingManager, error) {
	var err error
	if config.WriteToStreaming {
		once.Do(func() {
			proxy.logger.Debug("Starting writes to Streaming")
			sm = &streamingManager{
				config:      config,
				ctx:         ctx,
				MessageFeed: make(chan *RequestResponse, 1024),
			}
			go initStreaming()
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
			handleQuery(req)
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
		} else if match, _ := regexp.MatchString(`(?i)(IF NOT EXISTS|IF EXISTS)`, query); match {
			// this case still needs some way to handle UPDATE ... IF ...
			fmt.Println("LWT")
		} else if match, _ := regexp.MatchString(`(?i)^\s*INSERT`, query); match {
			fmt.Println("INSERT")
			PostToStreaming(query)
		} else if match, _ := regexp.MatchString(`(?i)^\s*UPDATE`, query); match {
			fmt.Println("UPDATE")
		} else if match, _ := regexp.MatchString(`(?i)^\s*DELETE`, query); match {
			fmt.Println("DELETE")
		} // else if match, _ := regexp.MatchString(`(?i)^\s*SELECT`, query); match {
		// 	fmt.Println("SELECT")
		// }

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

func initStreaming() {

	log.Println("[Astra Streaming] Starting Pulsar Client")

	var err error

	sm.pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{
		URL:            sm.config.StreamingURI,
		Authentication: pulsar.NewAuthenticationToken(sm.config.StreamingToken),
	})

	if err != nil {
		log.Fatalf("[Astra Streaming] Could not instantiate Pulsar client: %v", err)
	}

	producerJS := pulsar.NewJSONSchema(messageSchemaDef, nil)

	sm.pulsarProducer, err = sm.pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic:  sm.config.StreamingTopic,
		Schema: producerJS,
	})

	if err != nil {
		log.Fatalf("[Astra Streaming] Could not instantiate Pulsar Producer: %v", err)
	}

	// defer sm.pulsarClient.Close()

}

func PostToStreaming(query string) {
	TS := time.Now()
	msg := pulsar.ProducerMessage{
		Key: TS.Format(time.RFC3339),
		Value: &messageType{
			Query: query,
			TS:    TS,
		},
		EventTime: time.Now(),
	}
	ctx := context.Background()
	var err error

	_, err = sm.pulsarProducer.Send(ctx, &msg)
	if err != nil {
		log.Fatal(err)
	}
}
