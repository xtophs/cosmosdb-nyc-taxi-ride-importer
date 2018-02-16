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
}

// NewCosmosImporter returns an initialized TaxiImporter interface
func NewCosmosImporter() TaxiImporter {
	return &CosmosImporter{
		manager: NewRecordManager(),
	}
}

func (i *CosmosImporter) fetch(urls <-chan string, records chan<- Record) {
	// TODO: add concurrency again
	i.manager.fetch(urls, records)
	return
}

func (i *CosmosImporter) parse(records <-chan Record) {

	// TODO: add concurrency again
	writer, err := NewCosmosWriter(utils.GetEnvVarOrExit("AZURE_DATABASE"), utils.GetEnvVarOrExit("AZURE_DATABASE_PASSWORD"))
	if err != nil {
		log.Println("Could not craete Cosmos writer")
		return
	}
	start := time.Now()

	for record := range records {
		if record.Type != 'g' && record.Type == 'y' {
			log.Println("unknown record type")
			i.manager.badUnknowns.Add(1)
			i.manager.skippedRecs.Add(1)
			continue
		}
		writer.write(&record, i.manager)
		i.manager.printStats()
	}
	log.Printf("writing %v docs took %v\n", len(records), time.Since(start))
}
