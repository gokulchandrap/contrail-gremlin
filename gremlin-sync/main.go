package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
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
	log    = logging.MustGetLogger("gremlin-sync")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
)

const (
	QueryMaxSize = 40000
	VncExchange  = "vnc_config.object-update"
	QueueName    = "gremlin.sync"
)

type Notification struct {
	Oper string `json:"oper"`
	Type string `json:"type"`
	UUID string `json:"uuid"`
}

type Edge struct {
	Source     string `json:"outV"`
	SourceType string `json:"outVLabel"`
	Target     string `json:"inV"`
	TargetType string `json:"inVLabel"`
	Type       string `json:"label"`
}

func (l Edge) Create() error {
	_, err := gremlin.Query("g.V(src).as('src').V(dst).addE(type).from('src')").Bindings(
		gremlin.Bind{
			"src":  l.Source,
			"dst":  l.Target,
			"type": l.Type,
		}).Exec()
	return err
}

func (l Edge) Exists() (exists bool, err error) {
	var (
		data []byte
		res  []bool
	)
	data, err = gremlin.Query(`g.V(src).out(type).hasId(dst).hasNext()`).Bindings(
		gremlin.Bind{
			"src":  l.Source,
			"dst":  l.Target,
			"type": l.Type,
		}).Exec()
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return res[0], err
}

func (l Edge) Delete() error {
	_, err := gremlin.Query("g.V(src).bothE().where(otherV().hasId(dst)).drop()").Bindings(
		gremlin.Bind{
			"src": l.Source,
			"dst": l.Target,
		}).Exec()
	return err
}

type Vertex struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
	Edges      []Edge
}

func (n *Vertex) SetDeleted() error {
	_, err := gremlin.Query("g.V(_id).property('deleted', _deleted)").Bindings(
		gremlin.Bind{
			"_id":      n.ID,
			"_deleted": time.Now().Unix(),
		}).Exec()
	return err
}

func (n Vertex) propertiesQuery() (string, gremlin.Bind, error) {
	if n.Type == "" {
		return "", gremlin.Bind{}, errors.New("Node has no type, skip.")
	}

	var buffer bytes.Buffer
	bindings := gremlin.Bind{}
	for propName, propValue := range n.Properties {
		bindName := `_` + strings.Replace(propName, `.`, `_`, -1)
		buffer.WriteString(".property('")
		buffer.WriteString(propName)
		buffer.WriteString(`',`)
		buffer.WriteString(bindName)
		buffer.WriteString(`)`)
		bindings[bindName] = propValue
	}

	return buffer.String(), bindings, nil
}

func (n Vertex) Exists() (exists bool, err error) {
	var (
		data []byte
		res  []bool
	)
	data, err = gremlin.Query(`g.V(_id).hasLabel(_type).hasNext()`).Bindings(gremlin.Bind{
		"_id":   n.ID,
		"_type": n.Type,
	}).Exec()
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return res[0], err
}

func (n Vertex) Create() error {
	props, bindings, err := n.propertiesQuery()
	if err != nil {
		return err
	}
	bindings["_id"] = n.ID
	bindings["_type"] = n.Type
	query := `g.addV(id, _id, label, _type)` + props + `.iterate()`
	_, err = gremlin.Query(query).Bindings(bindings).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (n Vertex) CreateLinks() error {
	for _, link := range n.Edges {
		err := link.Create()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n Vertex) Update() error {
	query := `g.V(_id).properties().drop()`
	_, err := gremlin.Query(query).Bindings(gremlin.Bind{
		"_id": n.ID,
	}).Exec()
	if err != nil {
		return err
	}
	props, bindings, err := n.propertiesQuery()
	if err != nil {
		return err
	}
	bindings["_id"] = n.ID
	query = `g.V(_id)` + props + `.iterate()`
	_, err = gremlin.Query(query).Bindings(bindings).Exec()
	if err != nil {
		return err
	}
	return nil
}

// CurrentLinks returns the Links of the Node in its current state
func (n Vertex) CurrentLinks() (links []Edge, err error) {
	var data []byte
	data, err = gremlin.Query(`g.V(_id).bothE()`).Bindings(gremlin.Bind{
		"_id": n.ID,
	}).Exec()
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &links)

	return links, err
}

func (n Vertex) DiffLinks() ([]Edge, []Edge, error) {
	var (
		toAdd    []Edge
		toRemove []Edge
	)

	currentLinks, err := n.CurrentLinks()
	if err != nil {
		return toAdd, toRemove, err
	}

	for _, l1 := range n.Edges {
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
		for _, l2 := range n.Edges {
			if l1.Source == l2.Source && l1.Target == l2.Target && l1.Type == l2.Type {
				found = true
				break
			}
		}
		if !found {
			toRemove = append(toRemove, l1)
		}
	}

	return toAdd, toRemove, nil
}

// UpdateLinks check the current Node links in gremlin server
// and apply node.Links accordingly
func (n Vertex) UpdateLinks() error {
	toAdd, toRemove, err := n.DiffLinks()
	if err != nil {
		return err
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

func (n Vertex) Delete() error {
	_, err := gremlin.Query(`g.V(_id).drop()`).Bindings(gremlin.Bind{
		"_id": n.ID,
	}).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (n Vertex) AddProperties(prefix string, c *gabs.Container) {
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

func (n Vertex) AddProperty(prefix string, value interface{}) {
	if val, ok := n.Properties[prefix]; ok {
		switch val.(type) {
		case []interface{}:
			n.Properties[prefix] = append(n.Properties[prefix].([]interface{}), value)
		default:
			n.Properties[prefix] = []interface{}{val, value}
		}
	} else {
		n.Properties[prefix] = value
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

type Sync struct {
	connected bool
	session   gockle.Session
	msgs      <-chan amqp.Delivery
	historize bool
	pending   []Notification
}

func NewSync(session gockle.Session, msgs <-chan amqp.Delivery, historize bool) *Sync {
	return &Sync{
		connected: false,
		session:   session,
		msgs:      msgs,
		historize: historize,
		pending:   make([]Notification, 0),
	}
}

func (s *Sync) synchronize() {
	for d := range s.msgs {
		n := Notification{}
		json.Unmarshal(d.Body, &n)
		if !s.connected {
			s.handlePendingNotification(n)
			d.Ack(false)
		} else {
			if s.handleNotification(n) {
				d.Ack(false)
			} else {
				d.Nack(false, false)
			}
		}
	}
}

func (s *Sync) removePendingNotification(n Notification, i int) {
	log.Debugf("[%s] %s/%s [-]", n.Oper, n.Type, n.UUID)
	s.pending = append(s.pending[:i], s.pending[i+1:]...)
}

func (s *Sync) handlePendingNotification(n Notification) {
	switch n.Oper {
	// On DELETE, remove previous notifications in the pending list
	case "DELETE":
		for i := 0; i < len(s.pending); i++ {
			n2 := s.pending[i]
			if n2.UUID == n.UUID {
				s.removePendingNotification(n2, i)
				i--
			}
		}
	// Reduce resource updates
	case "UPDATE":
		for i := 0; i < len(s.pending); i++ {
			n2 := s.pending[i]
			if n2.UUID == n.UUID && n2.Oper == n.Oper {
				s.removePendingNotification(n2, i)
				i--
			}
		}
	}
	s.pending = append(s.pending, n)
	log.Debugf("[%s] %s/%s [+]", n.Oper, n.Type, n.UUID)
}

func (s Sync) handleNotification(n Notification) bool {
	switch n.Oper {
	case "CREATE":
		node, err := s.getContrailResource(n.UUID)
		if err != nil {
			log.Errorf("[%s] %s/%s failed: %s", n.Oper, n.Type, n.UUID, err)
		} else {
			node.Create()
			node.CreateLinks()
			log.Debugf("[%s] %s/%s", n.Oper, n.Type, n.UUID)
		}
		return true
	case "UPDATE":
		node, err := s.getContrailResource(n.UUID)
		if err != nil {
			log.Errorf("[%s] %s/%s failed: %s", n.Oper, n.Type, n.UUID, err)
		} else {
			node.Update()
			node.UpdateLinks()
			log.Debugf("[%s] %s/%s", n.Oper, n.Type, n.UUID)
		}
		return true
	case "DELETE":
		node := Vertex{ID: n.UUID}
		var err error
		if s.historize {
			err = node.SetDeleted()
		} else {
			err = node.Delete()
		}
		if err != nil {
			log.Errorf("[%s] %s/%s failed: %s", n.Oper, n.Type, n.UUID, err)
		} else {
			log.Debugf("[%s] %s/%s", n.Oper, n.Type, n.UUID)
		}
		return true
	default:
		log.Errorf("Notification not handled: %s", n)
		return false
	}
}

func (s *Sync) getContrailResource(uuid string) (Vertex, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := s.session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid)
	if err != nil {
		log.Criticalf("[%s] %s", uuid, err)
		return Vertex{}, err
	}
	node := Vertex{
		ID:         uuid,
		Properties: map[string]interface{}{},
	}
	for _, row := range rows {
		column1 = string(row["column1"].([]byte))
		valueJSON = []byte(row["value"].(string))
		split := strings.Split(column1, ":")
		switch split[0] {
		case "parent", "ref":
			node.Edges = append(node.Edges, Edge{
				Source:     uuid,
				Target:     split[2],
				TargetType: split[1],
				Type:       split[0],
			})
		case "backref", "children":
			var label string
			if split[0] == "backref" {
				label = "ref"
			} else {
				label = "parent"
			}
			node.Edges = append(node.Edges, Edge{
				Source:     split[2],
				SourceType: split[1],
				Target:     uuid,
				Type:       label,
			})
		case "type":
			var value string
			json.Unmarshal(valueJSON, &value)
			node.Type = value
		case "fq_name":
			var value []string
			json.Unmarshal(valueJSON, &value)
			node.AddProperty("fq_name", value)
		case "prop":
			value, err := gabs.ParseJSON(valueJSON)
			if err != nil {
				log.Criticalf("Failed to parse %v", string(valueJSON))
			} else {
				node.AddProperties(split[1], value)
			}
		}
	}

	if created, ok := node.Properties["id_perms.created"]; ok {
		if time, err := time.Parse(time.RFC3339Nano, created.(string)+`Z`); err == nil {
			node.AddProperty("created", time.Unix())
		}
	}
	if updated, ok := node.Properties["id_perms.last_modified"]; ok {
		if time, err := time.Parse(time.RFC3339Nano, updated.(string)+`Z`); err == nil {
			node.AddProperty("updated", time.Unix())
		}
	}

	node.AddProperty("deleted", 0)

	return node, nil
}

func (s *Sync) setupGremlin(gremlinCluster []string) (err error) {
	if err := gremlin.NewCluster(gremlinCluster...); err != nil {
		log.Fatal("Failed to connect to gremlin server.")
	} else {
		for {
			_, _, err = gremlin.CreateConnection()
			if err != nil {
				log.Warningf("Failed to connect to Gremlin server, retrying in 1s")
				time.Sleep(time.Duration(1) * time.Second)
			} else {
				log.Notice("Connected to Gremlin server.")
				for _, n := range s.pending {
					s.handleNotification(n)
				}
				s.pending = make([]Notification, 0)
				s.connected = true
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
	cluster.Timeout = 2000 * time.Millisecond
	cluster.DisableInitialHostLookup = true
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	mockableSession := gockle.NewSession(session)
	log.Notice("Connected.")
	return mockableSession, err
}

func setup(gremlinCluster []string, cassandraCluster []string, rabbitURI string, rabbitVHost string, rabbitQueue string, historize bool) {
	var (
		conn    *amqp.Connection
		msgs    <-chan amqp.Delivery
		session gockle.Session
		err     error
	)

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	session, err = setupCassandra(cassandraCluster)
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	defer session.Close()

	conn, _, msgs = setupRabbit(rabbitURI, rabbitVHost, rabbitQueue)
	defer conn.Close()

	sync := NewSync(session, msgs, historize)

	go sync.setupGremlin(gremlinCluster)
	go sync.synchronize()

	log.Notice("Listening for updates.")
	log.Notice("To exit press CTRL+C")
	forever := make(chan bool)
	<-forever
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
		Value:  "localhost:5672",
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
	historize := app.Bool(cli.BoolOpt{
		Name:   "historize",
		Value:  false,
		Desc:   "Mark nodes as deleted but don't drop them",
		EnvVar: "GREMLIN_SYNC_HISTORIZE",
	})
	app.Action = func() {
		var gremlinCluster = make([]string, len(*gremlinSrvs))
		for i, srv := range *gremlinSrvs {
			gremlinCluster[i] = fmt.Sprintf("ws://%s/gremlin", srv)

		}
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser,
			*rabbitPassword, *rabbitSrv)
		setup(gremlinCluster, *cassandraSrvs, rabbitURI, *rabbitVHost,
			*rabbitQueue, *historize)
	}
	app.Run(os.Args)
}
