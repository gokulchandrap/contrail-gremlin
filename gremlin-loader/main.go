package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/go-gremlin/gremlin"
	"github.com/gocql/gocql"
	"github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/streadway/amqp"
)

var (
	log    = logging.MustGetLogger("gremlin-loader")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
	session *gocql.Session
)

const (
	QueryMaxSize = 60000
	VncExchange  = "vnc_config.object-update"
	QueueName    = "gremlin.sync"
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
	_, err := gremlin.Query("g.V(src).as('src').V(dst).addE(type).from('src')").Bindings(gremlin.Bind{
		"src":  l.Source,
		"dst":  l.Target,
		"type": l.Type,
	}).Exec()
	return err
}

type Node struct {
	UUID       string
	Type       string
	Properties map[string]interface{}
}

func (n Node) createUpdateQuery(base string) (error, []string) {
	var queries []string
	if n.Type == "" {
		return errors.New("Node has no type, skip."), nil
	}
	encoder := GremlinPropertiesEncoder{
		stringReplacer: strings.NewReplacer(`"`, `\"`, "\n", `\n`, "\r", ``, `$`, `\$`),
	}
	err := encoder.Encode(n.Properties)
	if err != nil {
		return err, nil
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
					return err, nil
				}
				query = fmt.Sprintf("%s.property%s", queryBase, prop)
			} else {
				query = queryTmp
			}
		}
	} else {
		queries = append(queries, query)
	}
	return nil, queries
}

func (n Node) Create() error {
	err, queries := n.createUpdateQuery(`g.addV(id, uuid, label, type)`)
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

func (n Node) Update() error {
	err, queries := n.createUpdateQuery(`g.V(uuid)`)
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

func (n Node) Links() (error, []Link) {
	data, err := gremlin.Query(`g.V(uuid).bothE()`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return err, nil
	}
	var links []Link
	json.Unmarshal(data, &links)
	return nil, links
}

func setupRabbit(rabbitURI string, rabbitVHost string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
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
		QueueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
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
		true,   // auto-ack
		true,   // exclusive
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

func sync(session *gocql.Session, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		n := Notification{}
		json.Unmarshal(d.Body, &n)
		switch n.Oper {
		case "CREATE":
			node, links := getNode(session, n.UUID)
			node.Create()
			for _, link := range links {
				link.Create()
			}
			log.Debugf("%s/%s created", n.Type, n.UUID)
		case "UPDATE":
			node, newLinks := getNode(session, n.UUID)
			node.Update()
			_, currentLinks := node.Links()
			// TODO: update links
			log.Critical(currentLinks)
			log.Critical(newLinks)
			log.Debugf("%s/%s updated", n.Type, n.UUID)
		case "DELETE":
			node := Node{UUID: n.UUID, Type: n.Type}
			node.Delete()
			log.Debugf("%s/%s deleted", n.Type, n.UUID)
		}
	}
	log.Critical("Finish consuming")
}

func setupGremlin(gremlinCluster []string) {
	if err := gremlin.NewCluster(gremlinCluster...); err != nil {
		log.Fatal("Failed to connect to gremlin server.")
	} else {
		log.Notice("Connected to Gremlin server.")
	}
}

func setupCassandra(cassandraCluster []string) *gocql.Session {
	log.Notice("Connecting to Cassandra...")
	cluster := gocql.NewCluster(cassandraCluster...)
	cluster.Keyspace = "config_db_uuid"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 1200 * time.Millisecond
	session, _ := cluster.CreateSession()
	log.Notice("Connected.")
	return session

}

func setup(gremlinCluster []string, cassandraCluster []string, rabbitURI string, rabbitVHost string, noLoad bool, noSync bool) {

	var (
		conn    *amqp.Connection
		msgs    <-chan amqp.Delivery
		session *gocql.Session
	)

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	setupGremlin(gremlinCluster)

	session = setupCassandra(cassandraCluster)
	defer session.Close()

	if noSync == false {
		conn, _, msgs = setupRabbit(rabbitURI, rabbitVHost)
		defer conn.Close()
	}

	if noLoad == false {
		load(session)
	}

	if noSync == false {
		go sync(session, msgs)
		log.Notice("Listening for updates. To exit press CTRL+C")
		forever := make(chan bool)
		<-forever
	}
}

func getNode(session *gocql.Session, uuid string) (Node, []Link) {
	var (
		key       string
		column1   string
		valueJSON []byte
		links     []Link
	)
	node := Node{
		UUID:       uuid,
		Properties: map[string]interface{}{},
	}
	r := session.Query(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid).Iter()
	for r.Scan(&key, &column1, &valueJSON) {
		split := strings.Split(column1, ":")
		switch split[0] {
		case "ref":
			links = append(links, Link{Source: uuid, Target: split[2], Type: split[0]})
		case "parent":
			links = append(links, Link{Source: split[2], Target: uuid, Type: split[0]})
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
	if err := r.Close(); err != nil {
		log.Critical(err)
	}

	return node, links
}

func load(session *gocql.Session) {
	var (
		uuid  string
		links []Link
	)

	log.Notice("Processing nodes")

	r := session.Query(`SELECT column1 FROM obj_fq_name_table`).Iter()
	for r.Scan(&uuid) {
		parts := strings.Split(uuid, ":")
		uuid = parts[len(parts)-1]
		node, nodeLinks := getNode(session, uuid)
		if err := node.Create(); err != nil {
			fmt.Println()
			log.Criticalf("Failed to create node %v : %s", node, err)
		} else {
			fmt.Print(`.`)
		}
		for _, link := range nodeLinks {
			links = append(links, link)
		}
	}
	if err := r.Close(); err != nil {
		log.Critical(err)
	}

	fmt.Println()
	log.Notice("Processing links")

	for _, link := range links {
		if err := link.Create(); err != nil {
			log.Criticalf("Failed to create link %v : %s", link, err)
		} else {
			fmt.Print(`.`)
		}
	}

	fmt.Println()

}

func main() {
	app := cli.App("gremlin-loader", "Load and Sync Contrail DB in Gremlin Server")
	gremlinSrvs := app.StringsOpt("gremlin", []string{"localhost:8182"}, "host:port of gremlin server nodes")
	cassandraSrvs := app.StringsOpt("cassandra", []string{"localhost"}, "list of host of cassandra nodes, uses CQL port 9042")
	rabbitSrv := app.StringOpt("rabbit", "localhost:5276", "host:port of rabbitmq server")
	rabbitVHost := app.StringOpt("rabbit-vhost", "opencontrail", "vhost of rabbitmq server")
	rabbitUser := app.StringOpt("rabbit-user", "opencontrail", "user for rabbitmq server")
	rabbitPassword := app.StringOpt("rabbit-password", "", "password for rabbitmq server")
	noLoad := app.BoolOpt("no-load", false, "Don't load cassandra DB")
	noSync := app.BoolOpt("no-sync", false, "Don't sync with RabbitMQ")
	app.Action = func() {
		var gremlinCluster = make([]string, len(*gremlinSrvs))
		for i, srv := range *gremlinSrvs {
			gremlinCluster[i] = fmt.Sprintf("ws://%s/gremlin", srv)

		}
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser, *rabbitPassword, *rabbitSrv)
		setup(gremlinCluster, *cassandraSrvs, rabbitURI, *rabbitVHost, *noLoad, *noSync)
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
