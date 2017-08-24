package main

import (
	"encoding/json"
	"testing"

	"github.com/go-gremlin/gremlin"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/willfaught/gockle"
)

func checkNode(t *testing.T, query string, bindings gremlin.Bind) []string {
	var uuids []string
	results, err := gremlin.Query(query).Bindings(bindings).Exec()
	if err != nil {
		t.Errorf("Failed to run query: %s", query)
		t.SkipNow()
	}
	json.Unmarshal(results, &uuids)
	return uuids
}

func TestNodeLink(t *testing.T) {
	var uuids []string

	err := setupGremlin([]string{"ws://localhost:8182/gremlin"})
	if err != nil {
		t.Errorf("Failed to connect to gremlin server")
	}

	vnUUID := uuid.NewV4().String()
	vn := Node{
		UUID: vnUUID,
		Type: "virtual_machine",
	}
	vn.Create()

	vmiUUID := uuid.NewV4().String()
	vmi := Node{
		UUID: vmiUUID,
		Type: "virtual_machine_interface",
		Links: []Link{
			Link{
				Source: vmiUUID,
				Target: vnUUID,
				Type:   "ref",
			},
		},
	}
	vmi.Create()
	vmi.CreateLinks()

	uuids = checkNode(t, `g.V(uuid).in('ref').id()`, gremlin.Bind{"uuid": vnUUID})

	assert.Equal(t, 1, len(uuids), "One resource must be linked")
	assert.Equal(t, vmiUUID, uuids[0], "VMI not correctly linked to VN")

	projectUUID := uuid.NewV4().String()
	project := Node{
		UUID: projectUUID,
		Type: "project",
	}
	project.Create()

	vmi.Links = append(vmi.Links, Link{
		Source: projectUUID,
		Target: vmiUUID,
		Type:   "parent",
	})
	vmi.UpdateLinks()

	uuids = checkNode(t, `g.V(uuid).both().id()`, gremlin.Bind{"uuid": vmiUUID})

	assert.Equal(t, 2, len(uuids), "Two resources must be linked")
}

func TestNodeProperties(t *testing.T) {
	err := setupGremlin([]string{"ws://localhost:8182/gremlin"})
	if err != nil {
		t.Errorf("Failed to connect to gremlin server")
	}

	nodeUUID := uuid.NewV4().String()
	query := "SELECT key, column1, value FROM obj_uuid_table WHERE key=?"

	session := &gockle.SessionMock{}
	session.When("Close").Return()
	session.When("ScanMapSlice", query, []interface{}{nodeUUID}).Return(
		[]map[string]interface{}{
			{"column1": []byte("type"), "value": `"virtual_machine"`},
			{"column1": []byte("prop:integer"), "value": `12`},
			{"column1": []byte("prop:string"), "value": `"str"`},
			{"column1": []byte("prop:list"), "value": `["a", "b", "c"]`},
			{"column1": []byte("prop:object"), "value": `{"bool": false, "subObject": {"foo": "bar"}}`},
		},
		nil,
	)

	node, _ := getContrailNode(session, nodeUUID)
	node.Create()

	var uuids []string

	uuids = checkNode(t, `g.V(uuid).has('integer', 12).id()`, gremlin.Bind{"uuid": nodeUUID})
	assert.Equal(t, nodeUUID, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('string', 'str').id()`, gremlin.Bind{"uuid": nodeUUID})
	assert.Equal(t, nodeUUID, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('list', ['a', 'b', 'c']).id()`, gremlin.Bind{"uuid": nodeUUID})
	assert.Equal(t, nodeUUID, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('object.bool', false).id()`, gremlin.Bind{"uuid": nodeUUID})
	assert.Equal(t, nodeUUID, uuids[0])
	uuids = checkNode(t, `g.V(uuid).has('object.subObject.foo', 'bar').id()`, gremlin.Bind{"uuid": nodeUUID})
}

func TestNodeExists(t *testing.T) {
	err := setupGremlin([]string{"ws://localhost:8182/gremlin"})
	if err != nil {
		t.Errorf("Failed to connect to gremlin server")
	}

	nodeUUID := uuid.NewV4().String()
	node := Node{
		UUID: nodeUUID,
		Type: "label",
		Properties: map[string]interface{}{
			"prop": "value",
		},
	}
	node.Create()
	exists, _ := node.Exists()
	assert.Equal(t, true, exists)

	node.UUID = uuid.NewV4().String()
	exists, _ = node.Exists()
	assert.Equal(t, false, exists)
}

func TestLinkExists(t *testing.T) {
	err := setupGremlin([]string{"ws://localhost:8182/gremlin"})
	if err != nil {
		t.Errorf("Failed to connect to gremlin server")
	}

	node1UUID := uuid.NewV4().String()
	node1 := Node{
		UUID: node1UUID,
		Type: "foo",
	}
	node1.Create()

	node2UUID := uuid.NewV4().String()
	node2 := Node{
		UUID: node2UUID,
		Type: "bar",
	}
	node2.Create()

	link := Link{
		Source: node1UUID,
		Target: node2UUID,
		Type:   "foobar",
	}
	exists, _ := link.Exists()
	assert.Equal(t, exists, false)
	link.Create()
	exists, _ = link.Exists()
	assert.Equal(t, exists, true)
}

func TestLinkDiff(t *testing.T) {
	err := setupGremlin([]string{"ws://localhost:8182/gremlin"})
	if err != nil {
		t.Errorf("Failed to connect to gremlin server")
	}

	node1UUID := uuid.NewV4().String()
	node1 := Node{
		UUID: node1UUID,
		Type: "foo",
	}
	node1.Create()

	node2UUID := uuid.NewV4().String()
	node2 := Node{
		UUID: node2UUID,
		Type: "bar",
		Links: []Link{
			Link{
				Source: node2UUID,
				Target: node1UUID,
				Type:   "ref",
			},
		},
	}
	node2.Create()

	toAdd, toRemove, _ := node2.DiffLinks()
	assert.Equal(t, 1, len(toAdd))
	assert.Equal(t, 0, len(toRemove))

	node2.CreateLinks()

	toAdd, toRemove, _ = node2.DiffLinks()
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 0, len(toRemove))

	node2b := Node{
		UUID: node2UUID,
		Type: "bar",
	}
	toAdd, toRemove, _ = node2b.DiffLinks()
	assert.Equal(t, 0, len(toAdd))
	assert.Equal(t, 1, len(toRemove))
}
