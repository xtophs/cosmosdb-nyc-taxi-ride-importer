package main

import (
	"log"
	"time"

	"github.com/Azure/go-autorest/autorest/utils"
)

// TaxiImporter imports NYC taxi ride data into cosmosdb
type TaxiImporter interface {
	fetch(urls <-chan string, records chan<- Record)
	parse(records <-chan Record)
}

// CosmosImporter struct
type CosmosImporter struct {
	manager *RecordManager
	writer  *CosmosWriter
}

// NewCosmosImporter returns an initialized TaxiImporter interface
func NewCosmosImporter(r *RecordManager) (TaxiImporter, error) {

	w, err := NewCosmosWriter(utils.GetEnvVarOrExit("AZURE_DATABASE"), utils.GetEnvVarOrExit("AZURE_DATABASE_PASSWORD"))
	if err != nil {
		return nil, err
	}
	return &CosmosImporter{
		manager: r,
		writer:  w,
	}, nil
}

func (i *CosmosImporter) fetch(urls <-chan string, records chan<- Record) {
	// TODO: add concurrency again
	i.manager.fetch(urls, records)
	return
}

func (i *CosmosImporter) parse(records <-chan Record) {

	// TODO: add concurrency again
	start := time.Now()

	for record := range records {
		i.manager.nexter.Next()
		if record.Type != 'g' && record.Type != 'y' {
			log.Printf("unknown record type %d, %v", record.Type, record)
			i.manager.badUnknowns.Add(1)
			i.manager.skippedRecs.Add(1)
		} else {
			i.writer.write(record, i.manager)
		}
	}
	log.Printf("writing %v docs took %v\n", len(records), time.Since(start))
}

func (i *CosmosImporter) close() {
	i.writer.Close()
}
