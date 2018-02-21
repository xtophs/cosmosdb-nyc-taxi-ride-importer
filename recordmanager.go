package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pilosa/pdk"
)

// RecordManager fetches
type RecordManager struct {
	UseReadAll bool
	totalBytes int64
	bytesLock  sync.Mutex
	nexter     *Nexter

	totalRecs      *Counter
	skippedRecs    *Counter
	nullLocs       *Counter
	badLocs        *Counter
	badSpeeds      *Counter
	badTotalAmnts  *Counter
	badDurations   *Counter
	badPassCounts  *Counter
	badDist        *Counter
	badUnknowns    *Counter
	readRecords    *Counter
	writtenRecords *Counter
}

//NewRecordManager returns a new RecordManager
func NewRecordManager() *RecordManager {
	return &RecordManager{
		UseReadAll: false,
		nexter:     &Nexter{id: 0},

		totalRecs:      &Counter{},
		skippedRecs:    &Counter{},
		nullLocs:       &Counter{},
		badLocs:        &Counter{},
		badSpeeds:      &Counter{},
		badTotalAmnts:  &Counter{},
		badDurations:   &Counter{},
		badPassCounts:  &Counter{},
		badDist:        &Counter{},
		badUnknowns:    &Counter{},
		readRecords:    &Counter{},
		writtenRecords: &Counter{},
	}

}

func stringCopy(s string) string {
	var b []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
	h.Len = len(s)
	h.Cap = len(s)
	return string(b)
}

func (f *RecordManager) fetch(urls <-chan string, records chan<- Record) {
	fmt.Println("RecordManager fetch")
	failedURLs := make(map[string]int)
	for {
		url, ok := getNextURL(urls, failedURLs)
		fmt.Printf("next url %s\n", url)
		if !ok {
			break
		}
		var typ rune
		if strings.Contains(url, "green") {
			typ = 'g'
		} else if strings.Contains(url, "yellow") {
			typ = 'y'
		} else {
			typ = 'x'
		}
		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("fetching %s, err: %v", url, err)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				log.Printf("opening %s, err: %v", url, err)
				continue
			}
			content = f
		}
		var scan *bufio.Scanner
		if f.UseReadAll {
			// we're using ReadAll here to ensure that we can read the entire
			// file/url before we start putting it into Pilosa. Not great for memory
			// usage or smooth performance, but we want to ensure repeatable results
			// in the simplest way possible.
			contentBytes, err := ioutil.ReadAll(content)
			if err != nil {
				failedURLs[url]++
				if failedURLs[url] > 10 {
					log.Fatalf("Unrecoverable failure while fetching url: %v, err: %v. Could not read fully after 10 tries.", url, err)
				}
				continue
			}
			err = content.Close()
			if err != nil {
				log.Printf("closing %s, err: %v", url, err)
			}

			buf := bytes.NewBuffer(contentBytes)
			scan = bufio.NewScanner(buf)
		} else {
			scan = bufio.NewScanner(content)
		}

		// discard header line
		correctLine := false
		if scan.Scan() {
			header := scan.Text()
			if strings.HasPrefix(header, "vendor_") {
				correctLine = true
			}
		}
		for scan.Scan() {
			f.totalRecs.Add(1)
			record := scan.Text()
			f.AddBytes(len(record))
			if correctLine {
				// last field needs to be shifted over by 1
				lastcomma := strings.LastIndex(record, ",")
				if lastcomma == -1 {
					f.skippedRecs.Add(1)
					continue
				}
				record = record[:lastcomma] + "," + record[lastcomma:]
			}
			f.readRecords.Add(1)
			records <- Record{Val: record, Type: typ}
		}
		fmt.Println("done scanning")
		err := scan.Err()
		if err != nil {
			log.Printf("scan error on %s, err: %v", url, err)
		}
		delete(failedURLs, url)
	}
}

func (f *RecordManager) AddBytes(n int) {
	f.bytesLock.Lock()
	f.totalBytes += int64(n)
	f.bytesLock.Unlock()
}

func (f *RecordManager) BytesProcessed() (num int64) {
	f.bytesLock.Lock()
	num = f.totalBytes
	f.bytesLock.Unlock()
	return
}

// getNextURL fetches the next url from the channel, or if it is emtpy, gets a
// url from the failedURLs map after 10 seconds of waiting on the channel. As
// long as it gets a url, its boolean return value is true - if it does not get
// a url, it returns false.
func getNextURL(urls <-chan string, failedURLs map[string]int) (string, bool) {
	url, open := <-urls
	if !open {
		for url, _ := range failedURLs {
			return url, true
		}
		return "", false
	}
	return url, true
}

func (m *RecordManager) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Rides: %d, Bytes: %s, Records: %v, Duration: %v, Rate: %v/s", m.nexter.Last(), pdk.Bytes(bytes), m.totalRecs.Get(), duration, pdk.Bytes(float64(bytes)/duration.Seconds()))
			log.Printf("Read: %d, Written: %d", m.readRecords.Get(), m.writtenRecords.Get())
			log.Printf("Skipped: %v, badLocs: %v, nullLocs: %v, badSpeeds: %v, badTotalAmnts: %v, badDurations: %v, badUnknowns: %v, badPassCounts: %v, badDist: %v", m.skippedRecs.Get(), m.badLocs.Get(), m.nullLocs.Get(), m.badSpeeds.Get(), m.badTotalAmnts.Get(), m.badDurations.Get(), m.badUnknowns.Get(), m.badPassCounts.Get(), m.badDist.Get())
		}
	}()
	return t
}
