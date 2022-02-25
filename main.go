package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/robfig/cron/v3"
)

var (
	url    = flag.String("url", "http://103.71.173.51:9200", "the elasticsearch url")
	days   = flag.Int("days", 3, "indices retention days")
	direct = flag.Bool("direct", false, "clean the indices now")
)

type Indice struct {
	Health       string `json:"health"`
	Status       string `json:"status"`
	Index        string `json:"index"`
	UUID         string `json:"uuid"`
	Pri          string `json:"pri"`
	Rep          string `json:"rep"`
	DocsCount    string `json:"docs.count"`
	DocsDeleted  string `json:"docs.deleted"`
	StoreSize    string `json:"store.size"`
	PriStoreSize string `json:"pri.store.size"`
}

func deleteIndices() error {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{*url},
		Username:  "elastic",
		Password:  "yYeAW3aJOGptkYp5I4f0",
	})
	if err != nil {
		return fmt.Errorf("new es client: %w", err)
	}

	catIndices := es.Cat.Indices
	res, err := catIndices(catIndices.WithFormat("json"))
	if err != nil {
		return fmt.Errorf("es cat indices: %w", err)
	}

	indices := []*Indice{}
	// json decode the indices
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return fmt.Errorf("json decode: %w", err)
	}
	// sort the indices by index name
	sort.Slice(indices, func(i, j int) bool {
		return indices[i].Index < indices[j].Index
	})
	// delete the index of 3 days ago
	condition := time.Now().AddDate(0, 0, -(*days)).Format("2006.01.02")
	for _, indice := range indices {
		if strings.Contains(indice.Index, "traefik") {
			date := indice.Index[len(indice.Index)-10:]
			if date <= condition {
				if _, err := es.Indices.Delete([]string{indice.Index}); err != nil {
					return fmt.Errorf("delete index %s: %w", indice.Index, err)
				}
				log.Printf("delete index: %s", indice.Index)
			}
		}
	}

	return nil
}

func main() {
	flag.Parse()

	if *direct {
		if err := deleteIndices(); err != nil {
			log.Println(err)
		}
		return
	}

	newCron := cron.New()
	newCron.AddFunc("@daily", func() {
		if err := deleteIndices(); err != nil {
			log.Println(err)
		}
	})
	newCron.Start()
	defer newCron.Stop()

	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("received signal: %v", <-s)
}
