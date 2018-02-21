package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/pilosa/pdk"
	//"github.com/pkg/errors"
)

/***************
use case setup
***************/

/***********************
use case implementation
***********************/

func main() {
	m := NewMain()
	m.URLFile = "yellow1.txt"
	err := m.Run()
	if err != nil {
		log.Fatal(err)
	}
}

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

type Main struct {
	PilosaHost       string
	URLFile          string
	FetchConcurrency int
	Concurrency      int
	Index            string
	BufferSize       int
	UseReadAll       bool

	urls []string

	recordManager *RecordManager
}

func NewMain() *Main {
	m := &Main{
		Concurrency:      1,
		FetchConcurrency: 1,
		urls:             make([]string, 0),
		recordManager:    NewRecordManager(),
	}

	return m
}

func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	ticker := m.recordManager.printStats()

	urls := make(chan string, 100)
	records := make(chan Record, 20000)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Rides: %d, Bytes: %s", m.recordManager.nexter.Last(), pdk.Bytes(m.recordManager.BytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup

	importer, err := NewCosmosImporter(m.recordManager)
	if err != nil {
		log.Panicf("Can't Open Importer: %s", err.Error())
	}

	for i := 0; i < m.FetchConcurrency; i++ {
		wg.Add(1)
		go func() {
			importer.fetch(urls, records)
			wg.Done()
		}()
	}
	var wg2 sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg2.Add(1)
		go func() {
			importer.parse(records)
			wg2.Done()
		}()
	}
	wg.Wait()
	close(records)
	wg2.Wait()

	time.Sleep(30 * time.Second)
	ticker.Stop()
	return err
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	if err := s.Err(); err != nil {
		return err
	}
	return nil
}

type BitFrame struct {
	Bit   uint64
	Frame string
}
