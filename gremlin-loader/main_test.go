package main

import (
	"encoding/json"
	"testing"

	"github.com/go-gremlin/gremlin"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNodeLink(t *testing.T) {
	var uuids []string

	setupGremlin([]string{"ws://localhost:8182/gremlin"})

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

	results, _ := gremlin.Query("g.V(vn).in('ref').id()").Bindings(
		gremlin.Bind{
			"vn": vnUUID,
		},
	).Exec()
	json.Unmarshal(results, &uuids)

	assert.Equal(t, len(uuids), 1, "One resource must be linked")
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

	results, _ = gremlin.Query("g.V(vmi).both().id()").Bindings(
		gremlin.Bind{
			"vmi": vmiUUID,
		},
	).Exec()
	json.Unmarshal(results, &uuids)

	assert.Equal(t, len(uuids), 2, "Two resources must be linked")
}
