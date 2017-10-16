package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/gocql/gocql"
	"github.com/jawher/mow.cli"
	logging "github.com/op/go-logging"
	"github.com/willfaught/gockle"
)

var (
	log    = logging.MustGetLogger("gremlin-loader")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
)

const (
	// Readers numbers of workers reading cassandra resources
	Readers = 10
)

const (
	DumpStart = iota
	ResourceRead
	NormalVertex
	MissingVertex
	DuplicateVertex
	IncompleteVertex
	DumpEnd
)

type VertexEdge struct {
	vID     string
	oVertex Vertex
}

type Property struct {
	ID    int64       `json:"id"`
	Value interface{} `json:"value"`
}

type Edge struct {
	ID   int64  `json:"id"`
	OutV string `json:"outV,omitempty"`
	InV  string `json:"inV,omitempty"`
}

type Vertex struct {
	ID         string                `json:"id"`
	Label      string                `json:"label"`
	Properties map[string][]Property `json:"properties,omitempty"`
	InE        map[string][]Edge     `json:"inE,omitempty"`
	OutE       map[string][]Edge     `json:"outE,omitempty"`
}

func (v *Vertex) AddProperties(prefix string, c *gabs.Container, l *Dumper) {
	if _, ok := c.Data().([]interface{}); ok {
		childs, _ := c.Children()
		for _, child := range childs {
			v.AddProperties(prefix, child, l)
		}
		return
	}
	if _, ok := c.Data().(map[string]interface{}); ok {
		childs, _ := c.ChildrenMap()
		for key, child := range childs {
			v.AddProperties(prefix+"."+key, child, l)
		}
		return
	}
	if str, ok := c.Data().(string); ok {
		v.AddProperty(prefix, str, l)
		return
	}
	if num, ok := c.Data().(float64); ok {
		v.AddProperty(prefix, num, l)
		return
	}
	if boul, ok := c.Data().(bool); ok {
		v.AddProperty(prefix, boul, l)
		return
	}
	v.AddProperty(prefix, "null", l)
}

func (v *Vertex) AddProperty(prefix string, value interface{}, l *Dumper) {
	if props, ok := v.Properties[prefix]; !ok {
		v.Properties[prefix] = []Property{
			Property{ID: atomic.AddInt64(l.propID, 1), Value: value},
		}
	} else {
		currentValue := props[0].Value
		switch currentValue.(type) {
		case []interface{}:
			v.Properties[prefix][0].Value = append(currentValue.([]interface{}), value)
		default:
			v.Properties[prefix][0].Value = []interface{}{currentValue, value}
		}
	}
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

func setup(cassandraCluster []string, filePath string) {
	var (
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

	load(session, filePath)
}

func (l *Dumper) getEdgeID(edgeUUID string) int64 {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.edgeIDs[edgeUUID]; !ok {
		l.edgeIDs[edgeUUID] = atomic.AddInt64(l.edgeID, 1)
	}
	return l.edgeIDs[edgeUUID]
}

func (l *Dumper) getContrailResource(session gockle.Session, uuid string) (Vertex, error) {
	var (
		column1   string
		valueJSON []byte
	)
	rows, err := session.ScanMapSlice(`SELECT key, column1, value FROM obj_uuid_table WHERE key=?`, uuid)
	if err != nil {
		log.Criticalf("[%s] %s", uuid, err)
		return Vertex{}, err
	}
	vertex := Vertex{
		ID:         uuid,
		Properties: map[string][]Property{},
		InE:        map[string][]Edge{},
		OutE:       map[string][]Edge{},
	}
	for _, row := range rows {
		column1 = string(row["column1"].([]byte))
		valueJSON = []byte(row["value"].(string))
		split := strings.Split(column1, ":")
		switch split[0] {
		case "parent", "ref":
			label := split[0]
			edgeUUID := uuid + "-" + split[2]
			id := l.getEdgeID(edgeUUID)
			edge := Edge{ID: id, InV: split[2]}
			if _, ok := vertex.OutE[label]; !ok {
				vertex.OutE[label] = []Edge{edge}
			} else {
				vertex.OutE[label] = append(vertex.OutE[label], edge)
			}
			ve := VertexEdge{
				vID: uuid,
				oVertex: Vertex{
					ID:    split[2],
					Label: split[1],
					InE: map[string][]Edge{
						label: []Edge{Edge{ID: id, OutV: uuid}},
					},
				},
			}
			l.seen <- ve
		case "children", "backref":
			var label string
			if split[0] == "backref" {
				label = "ref"
			} else {
				label = "parent"
			}
			edgeUUID := split[2] + "-" + uuid
			id := l.getEdgeID(edgeUUID)
			edge := Edge{ID: id, OutV: split[2]}
			if _, ok := vertex.InE[label]; !ok {
				vertex.InE[label] = []Edge{edge}
			} else {
				vertex.InE[label] = append(vertex.InE[label], edge)
			}
			ve := VertexEdge{
				vID: uuid,
				oVertex: Vertex{
					ID:    split[2],
					Label: split[1],
					OutE: map[string][]Edge{
						label: []Edge{Edge{ID: id, InV: uuid}},
					},
				},
			}
			l.seen <- ve
		case "type":
			var value string
			json.Unmarshal(valueJSON, &value)
			vertex.Label = value
		case "fq_name":
			var value []string
			json.Unmarshal(valueJSON, &value)
			vertex.AddProperty("fq_name", value, l)
		case "prop":
			value, err := gabs.ParseJSON(valueJSON)
			if err != nil {
				log.Criticalf("Failed to parse %v", string(valueJSON))
			} else {
				vertex.AddProperties(split[1], value, l)
			}
		}
	}

	if len(vertex.Label) == 0 {
		vertex.Label = "_incomplete"
		vertex.AddProperty("_incomplete", true, l)
	}
	if _, ok := vertex.Properties["fq_name"]; !ok {
		vertex.AddProperty("_incomplete", true, l)
	}
	if _, ok := vertex.Properties["id_perms.created"]; !ok {
		vertex.AddProperty("_incomplete", true, l)
	}

	// Add updated/created properties timestamps
	if created, ok := vertex.Properties["id_perms.created"]; ok {
		for _, prop := range created {
			if time, err := time.Parse(time.RFC3339Nano, prop.Value.(string)+`Z`); err == nil {
				vertex.AddProperty("created", time.Unix(), l)
			}
		}
	}
	if updated, ok := vertex.Properties["id_perms.last_modified"]; ok {
		for _, prop := range updated {
			if time, err := time.Parse(time.RFC3339Nano, prop.Value.(string)+`Z`); err == nil {
				vertex.AddProperty("updated", time.Unix(), l)
			}
		}
	}

	return vertex, nil
}

type Dumper struct {
	count     chan int64
	wgCount   *sync.WaitGroup
	wgRead    *sync.WaitGroup
	wgWrite   *sync.WaitGroup
	wgChecker *sync.WaitGroup
	uuids     chan string
	seen      chan VertexEdge
	write     chan Vertex
	session   gockle.Session
	propID    *int64
	edgeID    *int64
	edgeIDs   map[string]int64
	filePath  string
	sync.Mutex
}

func NewLoader(session gockle.Session, filePath string) *Dumper {
	return &Dumper{
		count:     make(chan int64),
		uuids:     make(chan string),
		seen:      make(chan VertexEdge),
		write:     make(chan Vertex),
		wgCount:   &sync.WaitGroup{},
		wgRead:    &sync.WaitGroup{},
		wgWrite:   &sync.WaitGroup{},
		wgChecker: &sync.WaitGroup{},
		session:   session,
		propID:    new(int64),
		edgeID:    new(int64),
		edgeIDs:   map[string]int64{},
		filePath:  filePath,
	}
}

func (l *Dumper) reporter() {
	l.wgCount.Add(1)
	defer l.wgCount.Done()
	readCount := 0
	normalCount := 0
	missingCount := 0
	duplicateCount := 0
	incompleteCount := 0

	dumpStatus := `W`

	for c := range l.count {
		switch c {
		case ResourceRead:
			readCount++
		case NormalVertex:
			normalCount++
		case MissingVertex:
			missingCount++
		case DuplicateVertex:
			duplicateCount++
		case IncompleteVertex:
			incompleteCount++
		case DumpStart:
			dumpStatus = `R`
		case DumpEnd:
			dumpStatus = `D`
		}
		fmt.Printf("\rProcessing nodes [read:%d correct:%d incomplete:%d missing:%d dup:%d] %s",
			readCount, normalCount, incompleteCount, missingCount, duplicateCount, dumpStatus)
	}
	fmt.Println()
}

func (l *Dumper) writer() {
	l.wgWrite.Add(1)
	defer l.wgWrite.Done()
	f, err := os.Create(l.filePath)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	// To handle duplicate uuids in the fq_name table
	written := make(map[string]bool)
	for v := range l.write {
		if _, ok := written[v.ID]; ok {
			l.count <- DuplicateVertex
			continue
		} else {
			written[v.ID] = true
		}
		vJSON, err := json.Marshal(v)
		if err != nil {
			log.Criticalf("Failed to convert %v to json", v)
		} else {
			_, err := f.Write(vJSON)
			if err != nil {
				log.Criticalf("Failed to write %v to file", v)
			} else {
				if _, ok := v.Properties["_missing"]; ok {
					l.count <- MissingVertex
				} else if _, ok := v.Properties["_incomplete"]; ok {
					l.count <- IncompleteVertex
				} else {
					l.count <- NormalVertex
				}
			}
			f.WriteString("\n")
		}
	}
	f.Sync()
}

func (l *Dumper) getContrailResources() error {
	var (
		column1 string
	)
	r := l.session.ScanIterator(`SELECT column1 FROM obj_fq_name_table`)
	for r.Scan(&column1) {
		parts := strings.Split(column1, ":")
		uuid := parts[len(parts)-1]
		l.uuids <- uuid
	}
	if err := r.Close(); err != nil {
		return err
	}
	return nil
}

func (l *Dumper) getNodes() (err error) {
	defer close(l.uuids)
	l.count <- DumpStart
	err = l.getContrailResources()
	if err != nil {
		return err
	}
	return nil
}

func (l *Dumper) reader() {
	l.wgRead.Add(1)
	defer l.wgRead.Done()
	for uuid := range l.uuids {
		vertex, err := l.getContrailResource(l.session, uuid)
		if err != nil {
			log.Warningf("%s", err)
		} else {
			l.count <- ResourceRead
			l.write <- vertex
		}
	}
}

func (l *Dumper) checker() {
	l.wgChecker.Add(1)
	defer l.wgChecker.Done()
	seen := make(map[string]interface{})
	for ve := range l.seen {
		seen[ve.vID] = true
		if _, ok := seen[ve.oVertex.ID]; !ok {
			seen[ve.oVertex.ID] = ve.oVertex
		}
	}
	missing := make(map[string][]Vertex)
	for _, v := range seen {
		switch v.(type) {
		case Vertex:
			v := v.(Vertex)
			if _, ok := missing[v.ID]; !ok {
				missing[v.ID] = []Vertex{v}
			} else {
				missing[v.ID] = append(missing[v.ID], v)
			}
		}
	}
	for _, vs := range missing {
		v := Vertex{
			ID:    vs[0].ID,
			Label: vs[0].Label,
			Properties: map[string][]Property{
				"_missing": []Property{
					Property{ID: atomic.AddInt64(l.propID, 1), Value: true},
				},
			},
			InE:  map[string][]Edge{},
			OutE: map[string][]Edge{},
		}
		// merge all edges
		for _, v2 := range vs {
			for label, edges := range v2.InE {
				for _, e := range edges {
					v.InE[label] = append(v.InE[label], e)
				}
			}
			for label, edges := range v2.OutE {
				for _, e := range edges {
					v.OutE[label] = append(v.OutE[label], e)
				}
			}
		}
		l.write <- v
	}
}

func (l *Dumper) setupReaders() {
	for w := 1; w <= Readers; w++ {
		go l.reader()
	}
}

func (l *Dumper) teardown() {
	l.wgRead.Wait()
	close(l.seen)
	l.wgChecker.Wait()
	l.count <- DumpEnd
	close(l.write)
	l.wgWrite.Wait()
	close(l.count)
	l.wgCount.Wait()
	// just to line up the display
	time.Sleep(time.Second * 1)
}

func (l *Dumper) Run() {
	go l.reporter()
	go l.writer()
	go l.checker()

	l.setupReaders()
	err := l.getNodes()
	if err != nil {
		log.Criticalf("Sync failed: %s", err)
		return
	}
	l.teardown()
}

func load(session gockle.Session, filePath string) {
	loader := NewLoader(session, filePath)
	loader.Run()
}

func main() {
	app := cli.App("gremlin-loader", "Load and Sync Contrail DB in Gremlin Server")
	cassandraSrvs := app.Strings(cli.StringsOpt{
		Name:   "cassandra",
		Value:  []string{"localhost"},
		Desc:   "list of host of cassandra nodes, uses CQL port 9042",
		EnvVar: "GREMLIN_DUMP_CASSANDRA_SERVERS",
	})
	filePath := app.String(cli.StringArg{
		Name:   "DST",
		Desc:   "Output file path",
		EnvVar: "GREMLIN_DUMP_FILE_PATH",
	})
	app.Action = func() {
		setup(*cassandraSrvs, *filePath)
	}
	app.Run(os.Args)
}
