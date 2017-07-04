package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/go-gremlin/gremlin"
	"github.com/gocql/gocql"
	"github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/streadway/amqp"
	"github.com/willfaught/gockle"
)

var (
	log    = logging.MustGetLogger("gremlin-loader")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
	session *gocql.Session
)

const (
	QueryMaxSize = 40000
	VncExchange  = "vnc_config.object-update"
	QueueName    = "gremlin.sync"
	NodesWorkers = 10
	LinksWorkers = 10
	SyncFile     = "/tmp/gremlin-synced"
)

type Notification struct {
	Oper string `json:"oper"`
	Type string `json:"type"`
	UUID string `json:"uuid"`
}

type Link struct {
	Source string `json:"outV"`
	Target string `json:"inV"`
	Type   string `json:"label"`
}

func (l Link) Create() error {
	_, err := gremlin.Query("g.V(src).as('src').V(dst).addE(type).from('src')").Bindings(
		gremlin.Bind{
			"src":  l.Source,
			"dst":  l.Target,
			"type": l.Type,
		}).Exec()
	return err
}

func (l Link) Delete() error {
	_, err := gremlin.Query("g.V(src).bothE().where(otherV().hasId(dst)).drop()").Bindings(
		gremlin.Bind{
			"src": l.Source,
			"dst": l.Target,
		}).Exec()
	return err
}

type Node struct {
	UUID       string                 `json:"id"`
	Type       string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
	Links      []Link
}

func (n Node) createUpdateQuery(base string) ([]string, error) {
	var queries []string
	if n.Type == "" {
		return nil, errors.New("Node has no type, skip.")
	}
	encoder := GremlinPropertiesEncoder{
		stringReplacer: strings.NewReplacer(`"`, `\"`, "\n", `\n`, "\r", ``, `$`, `\$`),
	}
	err := encoder.Encode(n.Properties)
	if err != nil {
		return nil, err
	}
	encodedProps := encoder.String()
	query := fmt.Sprintf("%s%s", base, encodedProps)
	// When there is to many properties, add them in multiple passes
	if len([]byte(query)) > QueryMaxSize {
		props := strings.Split(encodedProps, ".property")
		queryBase := `g.V(uuid)`
		query = base
		for _, prop := range props[1:] {
			queryTmp := fmt.Sprintf("%s.property%s", query, prop)
			if len([]byte(queryTmp)) > QueryMaxSize {
				queries = append(queries, query)
				if err != nil {
					return nil, err
				}
				query = fmt.Sprintf("%s.property%s", queryBase, prop)
			} else {
				query = queryTmp
			}
		}
	} else {
		queries = append(queries, query)
	}
	return queries, nil
}

func (n Node) Exists() (exists bool, err error) {
	var (
		data []byte
		res  []Node
	)
	data, err = gremlin.Query(`g.V(uuid).hasLabel(type)`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
		"type": n.Type,
	}).Exec()
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return len(res) > 0, err
}

func (n Node) Create() error {
	queries, err := n.createUpdateQuery(`g.addV(id, uuid, label, type)`)
	if err != nil {
		return err
	}
	for _, query := range queries {
		_, err = gremlin.Query(query).Bindings(gremlin.Bind{
			"uuid": n.UUID,
			"type": n.Type,
		}).Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n Node) CreateLinks() error {
	for _, link := range n.Links {
		err := link.Create()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n Node) Update() error {
	queries, err := n.createUpdateQuery(`g.V(uuid)`)
	if err != nil {
		return err
	}
	_, err = gremlin.Query(`g.V(uuid).properties().drop()`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
		"type": n.Type,
	}).Exec()
	if err != nil {
		return err
	}
	for _, query := range queries {
		_, err := gremlin.Query(query).Bindings(gremlin.Bind{
			"uuid": n.UUID,
			"type": n.Type,
		}).Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

// CurrentLinks returns the Links of the Node in its current state
func (n Node) CurrentLinks() ([]Link, error) {
	// TODO: get links in one query
	var (
		links    []Link
		allLinks []Link
	)
	data1, err := gremlin.Query(`g.V(uuid).outE('ref')`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data1, &links)
	for _, link := range links {
		allLinks = append(allLinks, link)
	}
	data2, err := gremlin.Query(`g.V(uuid).inE('parent')`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data2, &links)
	for _, link := range links {
		allLinks = append(allLinks, link)
	}

	return allLinks, err
}

// UpdateLinks check the current Node links in gremlin server
// and apply node.Links accordingly
func (n Node) UpdateLinks() error {
	currentLinks, err := n.CurrentLinks()
	if err != nil {
		return err
	}

	var (
		toAdd    []Link
		toRemove []Link
	)

	for _, l1 := range n.Links {
		found := false
		for _, l2 := range currentLinks {
			if l1.Source == l2.Source && l1.Target == l2.Target && l1.Type == l2.Type {
				found = true
				break
			}
		}
		if !found {
			toAdd = append(toAdd, l1)
		}
	}

	for _, l1 := range currentLinks {
		found := false
		for _, l2 := range n.Links {
			if l1.Source == l2.Source && l1.Target == l2.Target && l1.Type == l2.Type {
				found = true
				break
			}
		}
		if !found {
			toRemove = append(toRemove, l1)
		}
	}

	for _, link := range toAdd {
		err = link.Create()
		if err != nil {
			return err
		}
	}

	for _, link := range toRemove {
		err = link.Delete()
		if err != nil {
			return err
		}
	}

	return nil
}

func (n Node) Delete() error {
	_, err := gremlin.Query(`g.V(uuid).drop()`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (n Node) AddProperties(prefix string, c *gabs.Container) {
	if _, ok := c.Data().([]interface{}); ok {
		childs, _ := c.Children()
		for _, child := range childs {
			n.AddProperties(prefix, child)
		}
		return
	}
	if _, ok := c.Data().(map[string]interface{}); ok {
		childs, _ := c.ChildrenMap()
		for key, child := range childs {
			n.AddProperties(prefix+"."+key, child)
		}
		return
	}
	if str, ok := c.Data().(string); ok {
		n.AddProperty(prefix, str)
		return
	}
	if num, ok := c.Data().(float64); ok {
		n.AddProperty(prefix, num)
		return
	}
	if boul, ok := c.Data().(bool); ok {
		n.AddProperty(prefix, boul)
		return
	}
	n.AddProperty(prefix, "null")
}

func (n Node) AddProperty(prefix string, value interface{}) {
	if _, ok := n.Properties[prefix]; ok {
		n.Properties[prefix] = append(n.Properties[prefix].([]interface{}), value)
	} else {
		n.Properties[prefix] = []interface{}{value}
	}
}

func setupRabbit(rabbitURI string, rabbitVHost string, rabbitQueue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	log.Notice("Connecting to RabbitMQ...")

	conn, err := amqp.DialConfig(rabbitURI, amqp.Config{Vhost: rabbitVHost})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %s", err)
	}

	q, err := ch.QueueDeclare(
		rabbitQueue, // name
		false,       // durable
		false,       // delete when unused
		true,        // exclusive
		false,       // no-wait
		amqp.Table{"x-expires": int32(180000)}, // arguments
	)
	if err != nil {
		log.Fatalf("Failed to create queue: %s", err)
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		VncExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %s", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %s", err)
	}

	log.Notice("Connected.")

	return conn, ch, msgs
}

func synchronize(session gockle.Session, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		n := Notification{}
		json.Unmarshal(d.Body, &n)
		switch n.Oper {
		case "CREATE":
			node := getNode(session, n.UUID)
			node.Create()
			node.CreateLinks()
			log.Debugf("%s/%s created", n.Type, n.UUID)
			d.Ack(false)
		case "UPDATE":
			node := getNode(session, n.UUID)
			node.Update()
			node.UpdateLinks()
			log.Debugf("%s/%s updated", n.Type, n.UUID)
			d.Ack(false)
		case "DELETE":
			node := Node{UUID: n.UUID, Type: n.Type}
			node.Delete()
			log.Debugf("%s/%s deleted", n.Type, n.UUID)
			d.Ack(false)
		default:
			log.Errorf("Notification not handled: %s", n)
			d.Nack(false, false)
		}
	}
	log.Critical("Finish consuming")
}

func setupGremlin(gremlinCluster []string) (err error) {
	if err := gremlin.NewCluster(gremlinCluster...); err != nil {
		log.Fatal("Failed to connect to gremlin server.")
	} else {
		for i := 1; i <= 10; i++ {
			_, _, err = gremlin.CreateConnection()
			if err != nil {
				log.Warningf("Failed to connect to Gremlin server, retrying in %ds", i)
				time.Sleep(time.Duration(i) * time.Second)
			} else {
				log.Notice("Connected to Gremlin server.")
				break
			}
		}
	}
	return err
}

func setupCassandra(cassandraCluster []string) (gockle.Session, error) {
	log.Notice("Connecting to Cassandra...")
	cluster := gocql.NewCluster(cassandraCluster...)
	cluster.Keyspace = "config_db_uuid"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 1200 * time.Millisecond
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	mockableSession := gockle.NewSession(session)
	log.Notice("Connected.")
	return mockableSession, err
}

func setup(gremlinCluster []string, cassandraCluster []string, rabbitURI string, rabbitVHost string, rabbitQueue string, noLoad bool, noSync bool) {
	var (
		conn    *amqp.Connection
		msgs    <-chan amqp.Delivery
		session gockle.Session
		err     error
	)

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	err = setupGremlin(gremlinCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Gremlin server: %s", err)
	}

	session, err = setupCassandra(cassandraCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	defer session.Close()

	if noSync == false {
		conn, _, msgs = setupRabbit(rabbitURI, rabbitVHost, rabbitQueue)
		defer conn.Close()
	}

	if noLoad == false {
		os.Remove(SyncFile)
		load(session)
		ioutil.WriteFile(SyncFile, []byte(""), 0644)
	}

	if noSync == false {
		go synchronize(session, msgs)
		log.Notice("Listening for updates. To exit press CTRL+C")
		forever := make(chan bool)
		<-forever
	}
}

func getNode(session gockle.Session, uuid string) Node {
	var (
		column1   string
		valueJSON []byte
	)
	node := Node{
		UUID:       uuid,
		Properties: map[string]interface{}{},
	}
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid)
	if err != nil {
		log.Critical(err)
	} else {
		for _, row := range rows {
			column1 = string(row["column1"].([]byte))
			valueJSON = []byte(row["value"].(string))
			split := strings.Split(column1, ":")
			switch split[0] {
			case "ref":
				node.Links = append(node.Links, Link{Source: uuid, Target: split[2], Type: split[0]})
			case "parent":
				node.Links = append(node.Links, Link{Source: split[2], Target: uuid, Type: split[0]})
			case "type":
				var value string
				json.Unmarshal(valueJSON, &value)
				node.Type = value
			case "fq_name":
				var value []string
				json.Unmarshal(valueJSON, &value)
				for _, c := range value {
					node.AddProperty("fq_name", c)
				}
			case "prop":
				value, err := gabs.ParseJSON(valueJSON)
				if err != nil {
					log.Fatalf("Failed to parse %v", string(valueJSON))
				}
				node.AddProperties(split[1], value)
			}
		}
	}

	return node
}

func loadNodes(uuids <-chan string, links chan<- []Link, session gockle.Session, wg *sync.WaitGroup) {
	defer wg.Done()
	for uuid := range uuids {
		node := getNode(session, uuid)
		if err := node.Create(); err != nil {
			fmt.Println()
			log.Criticalf("Failed to create node %v : %s", node, err)
		} else {
			fmt.Print(`.`)
		}
		links <- node.Links
	}
}

func loadLinks(links <-chan []Link, session gockle.Session, wg *sync.WaitGroup) {
	defer wg.Done()
	for nodeLinks := range links {
		for _, link := range nodeLinks {
			if err := link.Create(); err != nil {
				log.Criticalf("Failed to create link %v : %s", link, err)
			} else {
				fmt.Print(`.`)
			}
		}
	}
}

func load(session gockle.Session) {
	var (
		uuid    string
		wgNodes sync.WaitGroup
		wgLinks sync.WaitGroup
	)

	count := make(map[string]interface{})
	err := session.ScanMap(`SELECT count(*) FROM obj_fq_name_table`, count)
	if err != nil {
		log.Fatalf("Failed query: %s", err)
	}

	log.Noticef("Processing %d nodes", count["count"].(int64))

	uuids := make(chan string)
	links := make(chan []Link, count["count"].(int64))

	for w := 1; w <= NodesWorkers; w++ {
		wgNodes.Add(1)
		go loadNodes(uuids, links, session, &wgNodes)
	}

	r := session.ScanIterator(`SELECT column1 FROM obj_fq_name_table`)
	for r.Scan(&uuid) {
		parts := strings.Split(uuid, ":")
		uuid = parts[len(parts)-1]
		uuids <- uuid
	}
	if err := r.Close(); err != nil {
		log.Critical(err)
	}
	close(uuids)

	wgNodes.Wait()
	close(links)
	fmt.Println()

	log.Notice("Processing links")
	for w := 1; w <= LinksWorkers; w++ {
		wgLinks.Add(1)
		go loadLinks(links, session, &wgLinks)
	}

	wgLinks.Wait()
	fmt.Println()
}

func main() {
	app := cli.App("gremlin-loader", "Load and Sync Contrail DB in Gremlin Server")
	gremlinSrvs := app.Strings(cli.StringsOpt{
		Name:   "gremlin",
		Value:  []string{"localhost:8182"},
		Desc:   "host:port of gremlin server nodes",
		EnvVar: "GREMLIN_SYNC_GREMLIN_SERVERS",
	})
	cassandraSrvs := app.Strings(cli.StringsOpt{
		Name:   "cassandra",
		Value:  []string{"localhost"},
		Desc:   "list of host of cassandra nodes, uses CQL port 9042",
		EnvVar: "GREMLIN_SYNC_CASSANDRA_SERVERS",
	})
	rabbitSrv := app.String(cli.StringOpt{
		Name:   "rabbit",
		Value:  "localhost:5276",
		Desc:   "host:port of rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_SERVER",
	})
	rabbitVHost := app.String(cli.StringOpt{
		Name:   "rabbit-vhost",
		Value:  "opencontrail",
		Desc:   "vhost of rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_VHOST",
	})
	rabbitUser := app.String(cli.StringOpt{
		Name:   "rabbit-user",
		Value:  "opencontrail",
		Desc:   "user for rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_USER",
	})
	rabbitPassword := app.String(cli.StringOpt{
		Name:   "rabbit-password",
		Desc:   "password for rabbitmq server",
		EnvVar: "GREMLIN_SYNC_RABBIT_PASSWORD",
	})
	rabbitQueue := app.String(cli.StringOpt{
		Name:   "rabbit-queue",
		Value:  QueueName,
		Desc:   "name of rabbitmq name",
		EnvVar: "GREMLIN_SYNC_RABBIT_QUEUE",
	})
	noLoad := app.Bool(cli.BoolOpt{
		Name:   "no-load",
		Value:  false,
		Desc:   "Don't load cassandra DB",
		EnvVar: "GREMLIN_SYNC_NO_LOAD",
	})
	noSync := app.Bool(cli.BoolOpt{
		Name:   "no-sync",
		Value:  false,
		Desc:   "Don't sync with RabbitMQ",
		EnvVar: "GREMLIN_SYNC_NO_SYNC",
	})
	app.Action = func() {
		var gremlinCluster = make([]string, len(*gremlinSrvs))
		for i, srv := range *gremlinSrvs {
			gremlinCluster[i] = fmt.Sprintf("ws://%s/gremlin", srv)

		}
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser, *rabbitPassword, *rabbitSrv)
		setup(gremlinCluster, *cassandraSrvs, rabbitURI, *rabbitVHost, *rabbitQueue, *noLoad, *noSync)
	}
	app.Run(os.Args)
}

type GremlinPropertiesEncoder struct {
	bytes.Buffer
	stringReplacer *strings.Replacer
}

func (p *GremlinPropertiesEncoder) EncodeBool(b bool) error {
	if b {
		p.WriteString("true")
	} else {
		p.WriteString("false")
	}
	return nil
}

func (p *GremlinPropertiesEncoder) EncodeString(s string) error {
	p.WriteByte('"')
	p.WriteString(p.stringReplacer.Replace(s))
	p.WriteByte('"')
	return nil
}

func (p *GremlinPropertiesEncoder) EncodeInt64(i int64) error {
	p.WriteString(strconv.FormatInt(i, 10))
	return nil
}

func (p *GremlinPropertiesEncoder) EncodeUint64(i uint64) error {
	p.WriteString(strconv.FormatUint(i, 10))
	return nil
}

func (p *GremlinPropertiesEncoder) EncodeMap(m map[string]interface{}) error {
	for k, v := range m {
		err := p.EncodeKVPair(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *GremlinPropertiesEncoder) EncodeKVPair(k string, v interface{}) error {
	switch v.(type) {
	case []interface{}:
		for _, i := range v.([]interface{}) {
			p.WriteString(".property(list,")
			p.EncodeString(k)
			p.WriteByte(',')
			err := p.Encode(i)
			if err != nil {
				return err
			}
			p.WriteString(")")
		}
	default:
		p.WriteString(".property(")
		p.EncodeString(k)
		p.WriteByte(',')
		err := p.Encode(v)
		if err != nil {
			return err
		}
		p.WriteString(")")
	}
	return nil
}

func (p *GremlinPropertiesEncoder) Encode(v interface{}) error {
	switch v.(type) {
	case bool:
		return p.EncodeBool(v.(bool))
	case string:
		return p.EncodeString(v.(string))
	case int:
		return p.EncodeInt64(int64(v.(int)))
	case int32:
		return p.EncodeInt64(int64(v.(int32)))
	case int64:
		return p.EncodeInt64(v.(int64))
	case uint:
		return p.EncodeUint64(uint64(v.(uint)))
	case uint32:
		return p.EncodeUint64(uint64(v.(uint32)))
	case uint64:
		return p.EncodeUint64(v.(uint64))
	case float64:
		return p.EncodeInt64(int64(v.(float64)))
	case map[string]interface{}:
		return p.EncodeMap(v.(map[string]interface{}))
	}
	return errors.New("type unsupported: " + reflect.TypeOf(v).String())
}
