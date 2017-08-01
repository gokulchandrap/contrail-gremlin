package main

import (
	"os"

	"github.com/go-gremlin/gremlin"
	logging "github.com/op/go-logging"
)

var (
	log    = logging.MustGetLogger("gremlin-probe")
	format = logging.MustStringFormatter(
		`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`)
)

func main() {
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)

	if err := gremlin.NewCluster("ws://127.0.0.1:8182/gremlin"); err != nil {
		log.Fatal("Failed to setup gremlin server.")
	}

	_, err := gremlin.Query(`g`).Exec()
	if err != nil {
		log.Fatal("Failed to contact gremlin server.")
	}
}
