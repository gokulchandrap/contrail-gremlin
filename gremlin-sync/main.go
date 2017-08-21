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
	Workers      = 5
	SyncFile     = "/tmp/gremlin-synced"
)

const (
	NodeCreate = iota
	RawNodeCreate
	NodeUpdate
	NodeDelete
	LinkCreate
	LinkDelete
	NodeSyncStart
	LinkSyncStart
	SyncEnd
)

type Notification struct {
	Oper string `json:"oper"`
	Type string `json:"type"`
	UUID string `json:"uuid"`
}

type Link struct {
	Source     string `json:"outV"`
	SourceType string `json:"outVLabel"`
	Target     string `json:"inV"`
	TargetType string `json:"inVLabel"`
	Type       string `json:"label"`
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

func (l Link) Exists() (exists bool, err error) {
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
	Created    int64
	Updated    int64
	Deleted    int64
}

func (n *Node) SetDeleted() error {
	n.Deleted = time.Now().Unix()
	_, err := gremlin.Query("g.V(uuid).property('deleted', time)").Bindings(
		gremlin.Bind{
			"uuid": n.UUID,
			"time": n.Deleted,
		}).Exec()
	return err
}

func (n Node) createUpdateQuery(base string) ([]string, error) {
	var queries []string
	if n.Type == "" {
		return nil, errors.New("Node has no type, skip.")
	}
	if n.Created != 0 {
		base += fmt.Sprintf(`.property("created", %d)`, n.Created)
	}
	if n.Updated != 0 {
		base += fmt.Sprintf(`.property("updated", %d)`, n.Updated)
	}
	base += fmt.Sprintf(`.property("deleted", %d)`, n.Deleted)
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
		res  []bool
	)
	data, err = gremlin.Query(`g.V(uuid).hasLabel(type).hasNext()`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
		"type": n.Type,
	}).Exec()
	if err != nil {
		return exists, err
	}
	json.Unmarshal(data, &res)
	return res[0], err
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
		refLinks    []Link
		parentLinks []Link
		allLinks    []Link
	)
	data1, err := gremlin.Query(`g.V(uuid).outE('ref')`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data1, &refLinks)
	for _, link := range refLinks {
		allLinks = append(allLinks, link)
	}

	data2, err := gremlin.Query(`g.V(uuid).inE('parent')`).Bindings(gremlin.Bind{
		"uuid": n.UUID,
	}).Exec()
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data2, &parentLinks)
	for _, link := range parentLinks {
		allLinks = append(allLinks, link)
	}

	return allLinks, err
}

func (n Node) DiffLinks() ([]Link, []Link, error) {
	var (
		toAdd    []Link
		toRemove []Link
	)

	currentLinks, err := n.CurrentLinks()
	if err != nil {
		return toAdd, toRemove, err
	}

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

	return toAdd, toRemove, nil
}

// UpdateLinks check the current Node links in gremlin server
// and apply node.Links accordingly
func (n Node) UpdateLinks() error {
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
			node, err := getContrailNode(session, n.UUID)
			if err != nil {
				node.Create()
				node.CreateLinks()
				log.Debugf("%s/%s created", n.Type, n.UUID)
			}
			d.Ack(false)
		case "UPDATE":
			node, err := getContrailNode(session, n.UUID)
			if err != nil {
				node.Update()
				node.UpdateLinks()
				log.Debugf("%s/%s updated", n.Type, n.UUID)
			}
			d.Ack(false)
		case "DELETE":
			node := Node{UUID: n.UUID}
			err := node.SetDeleted()
			if err != nil {
				log.Debugf("%s/%s deleted", n.Type, n.UUID)
			}
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

func setup(gremlinCluster []string, cassandraCluster []string, rabbitURI string, rabbitVHost string, rabbitQueue string, noLoad bool, noReload bool, noSync bool, reloadInterval int) {
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
		load(session)
	}

	if noReload == false {
		log.Noticef("Configured reload with %dm interval.", reloadInterval)
		ticker := time.NewTicker(time.Minute * time.Duration(reloadInterval))
		go func() {
			for _ = range ticker.C {
				load(session)
			}
		}()
	}

	if noSync == false {
		log.Notice("Listening for updates.")
		go synchronize(session, msgs)
	}

	if noSync == false || noReload == false {
		log.Notice("To exit press CTRL+C")
		forever := make(chan bool)
		<-forever
	}
}

func getContrailNode(session gockle.Session, uuid string) (Node, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid)
	if err != nil {
		log.Criticalf("[%s] %s", uuid, err)
		return Node{}, err
	}
	node := Node{
		UUID:       uuid,
		Properties: map[string]interface{}{},
	}
	for _, row := range rows {
		column1 = string(row["column1"].([]byte))
		valueJSON = []byte(row["value"].(string))
		split := strings.Split(column1, ":")
		switch split[0] {
		case "ref":
			node.Links = append(node.Links, Link{
				Source:     uuid,
				Target:     split[2],
				TargetType: split[1],
				Type:       split[0],
			})
		case "parent":
			node.Links = append(node.Links, Link{
				Source:     split[2],
				SourceType: split[1],
				Target:     uuid,
				Type:       split[0],
			})
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
				log.Criticalf("Failed to parse %v", string(valueJSON))
			} else {
				node.AddProperties(split[1], value)
			}
		}
	}

	if created, ok := node.Properties["id_perms.created"]; ok {
		created := created.([]interface{})[0]
		if time, err := time.Parse(time.RFC3339Nano, created.(string)+`Z`); err == nil {
			node.Created = time.Unix()
		}
	}
	if updated, ok := node.Properties["id_perms.last_modified"]; ok {
		updated := updated.([]interface{})[0]
		if time, err := time.Parse(time.RFC3339Nano, updated.(string)+`Z`); err == nil {
			node.Updated = time.Unix()
		}
	}

	return node, nil
}

type LoaderOps struct {
	opers []LoaderOp
}

func NewLoaderOps() LoaderOps {
	return LoaderOps{opers: make([]LoaderOp, 0)}
}

func (l *LoaderOps) Add(oper int64, obj interface{}) {
	l.opers = append(l.opers, LoaderOp{oper: oper, obj: obj})
}

type LoaderOp struct {
	oper int64
	obj  interface{}
}

type Loader struct {
	count   chan int64
	wgCount *sync.WaitGroup
	opers   chan LoaderOps
	links   chan Node
	wg      *sync.WaitGroup
	session gockle.Session
}

func NewLoader(session gockle.Session) Loader {
	return Loader{
		count:   make(chan int64),
		wgCount: &sync.WaitGroup{},
		wg:      &sync.WaitGroup{},
		session: session,
	}
}

func (l *Loader) report() {
	nodeCreateCount := 0
	nodeUpdateCount := 0
	nodeDeleteCount := 0
	linkCreateCount := 0
	linkDeleteCount := 0

	nodeSyncStatus := `W`
	linkSyncStatus := `W`

	for c := range l.count {
		switch c {
		case NodeCreate:
			nodeCreateCount++
		case NodeUpdate:
			nodeUpdateCount++
		case NodeDelete:
			nodeDeleteCount++
		case LinkCreate:
			linkCreateCount++
		case LinkDelete:
			linkDeleteCount++
		case NodeSyncStart:
			nodeSyncStatus = `R`
		case LinkSyncStart:
			linkSyncStatus = `R`
		case SyncEnd:
			if nodeSyncStatus == `R` {
				nodeSyncStatus = `D`
			}
			if linkSyncStatus == `R` {
				linkSyncStatus = `D`
			}
		}
		fmt.Printf("\rProcessing [nodes:%d/%d/%d/%s] [links: %d/%d/%s]",
			nodeCreateCount, nodeUpdateCount, nodeDeleteCount, nodeSyncStatus,
			linkCreateCount, linkDeleteCount, linkSyncStatus)
	}
	fmt.Println()
}

func (l *Loader) waitCount() {
	close(l.count)
	l.wgCount.Wait()
	// just to line up the display
	time.Sleep(time.Second * 1)
}

func (l *Loader) getContrailNodes() (map[string]Node, error) {
	var (
		key     string
		column1 string
	)
	contrailNodes := make(map[string]Node)
	r := l.session.ScanIterator(`SELECT key, column1 FROM obj_fq_name_table`)
	for r.Scan(&key, &column1) {
		parts := strings.Split(column1, ":")
		uuid := parts[len(parts)-1]
		contrailNodes[uuid] = Node{UUID: uuid, Type: key}
	}
	if err := r.Close(); err != nil {
		return contrailNodes, err
	}
	return contrailNodes, nil
}

func (l *Loader) getGremlinNodes() (map[string]Node, error) {
	var (
		nodes []Node
	)
	gremlinNodes := make(map[string]Node)
	data, err := gremlin.Query(`g.V().has('deleted', 0)`).Exec()
	if err != nil {
		return gremlinNodes, err
	}
	json.Unmarshal(data, &nodes)
	for _, node := range nodes {
		gremlinNodes[node.UUID] = node
	}
	return gremlinNodes, nil
}

func (l *Loader) syncNodes() (err error) {
	defer close(l.opers)

	var (
		contrailNodes map[string]Node
		gremlinNodes  map[string]Node
	)

	contrailNodes, err = l.getContrailNodes()
	if err != nil {
		return err
	}
	gremlinNodes, err = l.getGremlinNodes()
	if err != nil {
		return err
	}
	log.Debugf("Active nodes [contrail:%d] [gremlin:%d]",
		len(contrailNodes), len(gremlinNodes))

	l.links = make(chan Node, len(contrailNodes))

	l.count <- NodeSyncStart
	for uuid, cNode := range contrailNodes {
		opers := NewLoaderOps()
		if gNode, ok := gremlinNodes[uuid]; ok {
			opers.Add(NodeUpdate, gNode)
			l.opers <- opers
		} else {
			opers.Add(NodeCreate, cNode)
			l.opers <- opers
		}
	}
	for uuid, gNode := range gremlinNodes {
		opers := NewLoaderOps()
		if _, ok := contrailNodes[uuid]; !ok {
			opers.Add(NodeDelete, gNode)
			l.opers <- opers
		}
	}

	return nil
}

func (l *Loader) syncLinks() {
	l.count <- LinkSyncStart
	defer close(l.opers)
	for node := range l.links {
		toAdd, toRemove, err := node.DiffLinks()
		if err != nil {
			continue
		}

		for _, link := range toAdd {
			opers := NewLoaderOps()
			if link.SourceType != "" {
				source := Node{UUID: link.Source, Type: link.SourceType}
				if exists, _ := source.Exists(); !exists {
					opers.Add(RawNodeCreate, source)
				}
			}
			if link.TargetType != "" {
				target := Node{UUID: link.Target, Type: link.TargetType}
				if exists, _ := target.Exists(); !exists {
					opers.Add(RawNodeCreate, target)
				}
			}
			opers.Add(LinkCreate, link)
			l.opers <- opers
		}

		for _, link := range toRemove {
			opers := NewLoaderOps()
			opers.Add(LinkDelete, link)
			l.opers <- opers
		}
	}
}

func (l *Loader) worker() {
	defer l.wg.Done()
	for ops := range l.opers {
		for _, o := range ops.opers {
			switch o.oper {
			case RawNodeCreate:
				node := o.obj.(Node)
				if err := node.Create(); err != nil {
					log.Criticalf("Failed to create node %v : %s", node, err)
				} else {
					l.count <- NodeCreate
				}
			case NodeCreate:
				node, err := getContrailNode(l.session, o.obj.(Node).UUID)
				if err != nil {
					log.Criticalf("Failed to retrieve node %s: %s", o.obj.(Node).UUID, err)
				} else {
					if err := node.Create(); err != nil {
						log.Criticalf("Failed to create node %v : %s", node, err)
					} else {
						l.count <- NodeCreate
						l.links <- node
					}
				}
			case NodeUpdate:
				node, err := getContrailNode(l.session, o.obj.(Node).UUID)
				if err != nil {
					log.Criticalf("Failed to retrieve node %s: %s", o.obj.(Node).UUID, err)
				} else {
					if err := node.Update(); err != nil {
						log.Criticalf("Failed to update node %v : %s", node, err)
					} else {
						l.count <- NodeUpdate
						l.links <- node
					}
				}
			case NodeDelete:
				node := o.obj.(Node)
				err := node.SetDeleted()
				if err != nil {
					log.Criticalf("Failed to delete node %v : %s", node, err)
				} else {
					l.count <- NodeDelete
				}
			case LinkCreate:
				link := o.obj.(Link)
				if err := link.Create(); err != nil {
					log.Criticalf("Failed to create link %v : %s", link, err)
				} else {
					l.count <- LinkCreate
				}
			case LinkDelete:
				link := o.obj.(Link)
				if err := link.Delete(); err != nil {
					log.Criticalf("Failed to delete link %v : %s", link, err)
				} else {
					l.count <- LinkDelete
				}
			}
		}
	}
}

func (l *Loader) setupWorkers() {
	l.opers = make(chan LoaderOps)
	for w := 1; w <= Workers; w++ {
		l.wg.Add(1)
		go l.worker()
	}
}

func (l *Loader) waitWorkers() {
	l.wg.Wait()
	l.count <- SyncEnd
}

func (l *Loader) Run() {
	os.Remove(SyncFile)
	go l.report()

	l.setupWorkers()
	err := l.syncNodes()
	if err != nil {
		log.Criticalf("Sync failed: %s", err)
		return
	}
	l.waitWorkers()
	// close channel created in syncNodes
	close(l.links)

	l.setupWorkers()
	l.syncLinks()
	l.waitWorkers()

	l.waitCount()
	ioutil.WriteFile(SyncFile, []byte(""), 0644)
}

func load(session gockle.Session) {
	loader := NewLoader(session)
	loader.Run()
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
	noLoad := app.Bool(cli.BoolOpt{
		Name:   "no-load",
		Value:  false,
		Desc:   "Don't load cassandra DB",
		EnvVar: "GREMLIN_SYNC_NO_LOAD",
	})
	noReload := app.Bool(cli.BoolOpt{
		Name:   "no-reload",
		Value:  false,
		Desc:   "Don't reload cassandra DB",
		EnvVar: "GREMLIN_SYNC_NO_RELOAD",
	})
	noSync := app.Bool(cli.BoolOpt{
		Name:   "no-sync",
		Value:  false,
		Desc:   "Don't sync with RabbitMQ",
		EnvVar: "GREMLIN_SYNC_NO_SYNC",
	})
	reloadInterval := app.Int(cli.IntOpt{
		Name:   "reload-interval",
		Value:  30,
		Desc:   "Time in minutes between reloads",
		EnvVar: "GREMLIN_SYNC_RELOAD_INTERVAL",
	})
	app.Action = func() {
		var gremlinCluster = make([]string, len(*gremlinSrvs))
		for i, srv := range *gremlinSrvs {
			gremlinCluster[i] = fmt.Sprintf("ws://%s/gremlin", srv)

		}
		rabbitURI := fmt.Sprintf("amqp://%s:%s@%s/", *rabbitUser, *rabbitPassword, *rabbitSrv)
		setup(gremlinCluster, *cassandraSrvs, rabbitURI, *rabbitVHost, *rabbitQueue,
			*noLoad, *noReload, *noSync, *reloadInterval)
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
