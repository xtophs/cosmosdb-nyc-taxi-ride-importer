package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	mgo "gopkg.in/mgo.v2"
)

func testFetch(t *testing.T) {
	os.Setenv("AZURE_DATABASE", "")
	os.Setenv("AZURE_DATABASE_PASSWORD", "")

	fmt.Println("Test fetch")
	// setup channels

	//url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv"
	url1 := "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-02.csv"

	urls := make(chan string, 1)
	recs := make(chan Record, 1000)

	i := NewCosmosImporter()

	go func() {
		fmt.Printf("sending Url %s\n", url1)
		urls <- url1
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		i.fetch(urls, recs)
		wg.Done()
	}()

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		for record := range recs {
			t.Logf("Received: %v\n", record)
		}
		wg2.Done()
	}()

	wg.Wait()
	close(recs)
	wg2.Wait()
}

func TestParseGreen(t *testing.T) {

	// from https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2013-08.csv
	var s = "2,2013-08-05 12:55:11,2013-08-05 12:59:50,N,1,0,0,0,0,1,123.4,3.9,0,0,0,0,,3.9,2,,,"

	rec := &Record{'g', s}

	ride := rec.toRide()
	if ride == nil {
		t.Fatalf("Green cab record didn't parse")
	}
	if ride.CabType != 0 {
		t.Fatalf("Green cab type wrong")
	}

	if ride.VendorID != "2" {
		t.Fatalf("Green cab bad vendodr")
	}

	pt, _ := time.Parse("2013-08-01 08:14:37", "2013-08-05 12:55:11")
	if ride.PickupTime != pt {
		t.Fatalf("Green can pickup time bad")
	}

	dt, _ := time.Parse("2013-08-01 08:14:37", "2013-08-05 12:59:50")
	if ride.DropTime != dt {
		t.Fatal("Green Cab drop time bad")
	}
	if ride.PickupDay != pt.Day() {
		t.Fatal("Greeb cab pickup day bad")
	}

	if ride.PickupMonth != int(pt.Month()) {
		t.Fatalf("Green cab pickup Month bad")
	}

	if ride.PickupYear != pt.Year() {
		t.Fatalf("Green cab pickup year bad")
	}

	if ride.DropDay != dt.Day() {
		t.Fatal("Greeb cab drop off day bad")
	}

	if ride.DropMonth != int(dt.Month()) {
		t.Fatalf("Green cab drop off Month bad")
	}

	if ride.DropYear != dt.Year() {
		t.Fatalf("Green cab drop off year bad")
	}

	if ride.PassengerCount != 1 {
		t.Fatalf("Green cap passenger count bad")
	}

	if ride.DistMiles != 123.4 {
		t.Fatalf("Green cab bad distance")
	}

	if ride.TotalDollars != 3.9 {
		t.Fatalf("Green cab bad total $")
	}

	if ride.DurationMinutes != dt.Sub(pt).Minutes() {
		t.Fatalf("Green cab bad duration")
	}

	if ride.SpeedMph != ride.DistMiles/dt.Sub(pt).Hours() {
		t.Fatalf("Green cab bad speed")
	}

	// ride.pickupLat, _ = strconv.ParseFloat(fields[greenFields["pickup_latitude"]], 64)
	// ride.pickupLon, _ = strconv.ParseFloat(fields[greenFields["pickup_longitude"]], 64)
	// ride.dropLat, _ = strconv.ParseFloat(fields[greenFields["dropoff_latitude"]], 64)
	// ride.dropLon, _ = strconv.ParseFloat(fields[greenFields["dropoff_longitude"]], 64)

}

func TestParseYellow(t *testing.T) {
	// from https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-02.csv
	var s = "DDS,2009-02-03 08:25:00,2009-02-03 08:33:39,12,1.6000000000000001,-73.992767999999998,40.758324999999999,,,-73.994709999999998,40.739722999999998,CASH,6.9000000000000004,0,,0,0,6.9000000000000004"
	rec := &Record{'y', s}
	ride := rec.toRide()

	compareYellowRide(ride, t)
}

func compareYellowRide(ride *Ride, t *testing.T) {
	if ride == nil {
		t.Fatalf("Yellow cab record didn't parse")
	}
	if ride.CabType != 1 {
		t.Fatalf("Yellow cab type wrong")
	}

	if ride.VendorID != "DDS" {
		t.Fatalf("Yellow cab bad vendodr")
	}

	pt, _ := time.Parse("2013-08-01 08:14:37", "2009-02-03 08:25:00")
	if ride.PickupTime != pt {
		t.Fatalf("Yellow can pickup time bad")
	}

	dt, _ := time.Parse("2013-08-01 08:14:37", "2009-02-03 08:33:39")
	if ride.DropTime != dt {
		t.Fatal("Yellow Cab drop time bad")
	}
	if ride.PickupDay != pt.Day() {
		t.Fatal("Greeb cab pickup day bad")
	}

	if ride.PickupMonth != int(pt.Month()) {
		t.Fatalf("Yellow cab pickup Month bad")
	}

	if ride.PickupYear != pt.Year() {
		t.Fatalf("Yellow cab pickup year bad")
	}

	if ride.DropDay != dt.Day() {
		t.Fatal("Greeb cab drop off day bad")
	}

	if ride.DropMonth != int(dt.Month()) {
		t.Fatalf("Yellow cab drop off Month bad")
	}

	if ride.DropYear != dt.Year() {
		t.Fatalf("Yellow cab drop off year bad")
	}

	if ride.PassengerCount != 12 {
		t.Fatalf("Yellow cap passenger count bad")
	}

	if ride.DistMiles != 1.6000000000000001 {
		t.Fatalf("Yellow cab bad distance")
	}

	if ride.TotalDollars != 6.9000000000000004 {
		t.Fatalf("Yellow cab bad total $")
	}

	if ride.DurationMinutes != dt.Sub(pt).Minutes() {
		t.Fatalf("Yellow cab bad duration")
	}

	if ride.SpeedMph != ride.DistMiles/dt.Sub(pt).Hours() {
		t.Fatalf("Yellow cab bad speed")
	}

	if ride.PickupLon != -73.992767999999998 {
		t.Fatalf("Yellow cab bad pickup lon")
	}

	if ride.PickupLat != 40.758324999999999 {
		t.Fatalf("Yellow cab bad pickup lat")
	}

	if ride.DropLon != -73.994709999999998 {
		t.Fatalf("Yellow cab bad pickup lon")
	}

	if ride.DropLat != 40.739722999999998 {
		t.Fatalf("Yellow cab bad pickup lat")
	}
}

func TestWriteToCosmos(t *testing.T) {

	t.Log("Writing to Cosmos")
	// from https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-02.csv
	var s = "DDS,2009-02-03 08:25:00,2009-02-03 08:33:39,12,1.6000000000000001,-73.992767999999998,40.758324999999999,,,-73.994709999999998,40.739722999999998,CASH,6.9000000000000004,0,,0,0,6.9000000000000004"

	rec := &Record{'y', s}

	db := "xtoph-pilosa"
	pw := "UUEkv5WaMIytWXYiK6qZfnAPt4vwujN6f4PrsVZ08Dx4PQp0JYB1fcRjYZ4HWiLcDDcsPGzLj82laLFxXTEKng=="

	i := &mgo.DialInfo{
		Addrs:    []string{fmt.Sprintf("%s.documents.azure.com:10255", db)}, // Get HOST + PORT
		Timeout:  60 * time.Second,
		Database: db, // It can be anything
		Username: db, // Username
		Password: pw, // PASSWORD
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), &tls.Config{})
		},
	}

	session, err := mgo.DialWithInfo(i)
	if err != nil {
		t.Fatalf("Can't connect to mongo, go error %v\n", err)
	}
	session.SetMode(mgo.Strong, true)
	session.SetSafe(&mgo.Safe{})

	c := session.DB(db).C("ridesColl")

	defer func() {
		c.RemoveAll(nil)
		session.Close()
	}()

	// query ... should be empty
	n, err := c.Find(nil).Count()
	if err != nil {
		t.Fatalf("Problem getting rides")
	}
	if n != 0 {
		t.Fatalf("Collection not empty")
	}

	w, _ := NewCosmosWriter(db, pw)
	w.WriteToCosmos(rec)

	// Now query
	newRide := &Ride{}
	n, err = c.Find(nil).Count()
	if err != nil {
		t.Fatalf("Problem getting rides")
	}
	if n != 1 {
		t.Fatalf("Collection not empty, expected 1 found %v\n", n)
	} else {
		t.Log("Found inserted record")
	}

	err = c.Find(nil).One(newRide)
	compareYellowRide(newRide, t)

}
