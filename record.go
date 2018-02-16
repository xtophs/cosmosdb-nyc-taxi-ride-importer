package main

import (
	"log"
	"strconv"
	"strings"
	"time"
)

var greenFields = map[string]int{
	"vendor_id":          0,
	"pickup_datetime":    1,
	"dropoff_datetime":   2,
	"passenger_count":    9,
	"trip_distance":      10,
	"pickup_longitude":   5,
	"pickup_latitude":    6,
	"ratecode_id":        4,
	"store_and_fwd_flag": 3,
	"dropoff_longitude":  7,
	"dropoff_latitude":   8,
	"payment_type":       18,
	"fare_amount":        11,
	"extra":              12,
	"mta_tax":            13,
	"tip_amount":         14,
	"tolls_amount":       15,
	"total_amount":       17,
}

var yellowFields = map[string]int{
	"vendor_id":             0,
	"pickup_datetime":       1,
	"dropoff_datetime":      2,
	"passenger_count":       3,
	"trip_distance":         4,
	"pickup_longitude":      5,
	"pickup_latitude":       6,
	"ratecode_id":           7,
	"store_and_fwd_flag":    8,
	"dropoff_longitude":     9,
	"dropoff_latitude":      10,
	"payment_type":          11,
	"fare_amount":           12,
	"extra":                 13,
	"mta_tax":               14,
	"tip_amount":            15,
	"tolls_amount":          16,
	"total_amount":          17,
	"improvement_surcharge": 18,
}

// Record records
type Record struct {
	Type rune
	Val  string
}

type Ride2 struct {
	vendorID string `bson:"vendor"`
}

// Ride rides
type Ride struct {
	//ID bson.ObjectId `bson:"_id"`
	//	gridID          uint64    `bson:"grid_id,omitempty"`
	VendorID        string    `bson:"vendor_id"`
	SpeedMph        float64   `bson:"speed_mph"`
	TotalDollars    float64   `bson:"total_amount_dollars"`
	DurationMinutes float64   `bson:"duration_minutes"`
	PassengerCount  int       `bson:"passenger_count"`
	DistMiles       float64   `bson:"distance_miles"`
	PickupTime      time.Time `bson:"pickup_time"`
	PickupDay       int       `bson:"pickup_day"`
	PickupMDay      int       `bson:"pickup_mday"`
	PickupMonth     int       `bson:"pickup_month"`
	PickupYear      int       `bson:"pickup_year"`
	PickupLat       float64   `bson:"pickup_latitude"`
	PickupLon       float64   `bson:"pickup_longitude"`
	DropLat         float64   `bson:"drop_latitude"`
	DropLon         float64   `bson:"drop_longitude"`
	DropTime        time.Time `bson:"drop_time"`
	DropDay         int       `bson:"drop_day"`
	DropMDay        int       `bson:"drop_mday"`
	DropMonth       int       `bson:"drop_month"`
	DropYear        int       `bson"drop_year"`
	//pickupGridID    uint64    `bson:"pickup_grid_id, omitempty"`
	//dropGridID      uint64    `bson:"drop_grid_id, omitempty"`
	//pickupElevation float64   `bson:"pickup_elevation, omitempty"`
	//dropElevation   float64   `bson:"drop_elevation, omitempty"`
	CabType int `bson:"cab_type"`
}

func (r *Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

func (r *Record) toRide() *Ride {
	fields, _ := r.Clean()

	ride := &Ride{}

	//ride.ID = bson.NewObjectId()
	// TODO: Find more elegant way to do this
	if r.Type == 'g' {
		ride.CabType = 0

		// "vendor_id":          0,
		// "pickup_datetime":    1,
		// "dropoff_datetime":   2,
		// "passenger_count":    9,
		// "trip_distance":      10,
		// "pickup_longitude":   5,
		// "pickup_latitude":    6,
		// "dropoff_longitude":  7,
		// "dropoff_latitude":   8,
		// "total_amount":       17,

		// pilosa smaple does not include
		// store_and_fwd_flag
		// ratecode_id
		// payment_type
		// fare_amount
		// extra
		// mta_tax
		// tip_amount
		// tolls_amount
		// improvement_surcharge

		// 2013-08-01 08:14:37
		// TODO Errors
		ride.VendorID = fields[greenFields["vendor_id"]]
		ride.PickupTime, _ = time.Parse("2013-08-01 08:14:37", fields[greenFields["pickup_datetime"]])
		ride.PickupDay = ride.PickupTime.Day()
		ride.PickupMonth = int(ride.PickupTime.Month())
		ride.PickupYear = ride.PickupTime.Year()
		ride.DropTime, _ = time.Parse("2013-08-01 08:14:37", fields[greenFields["dropoff_datetime"]])
		ride.DropDay = ride.DropTime.Day()
		ride.DropMonth = int(ride.DropTime.Month())
		ride.DropYear = ride.DropTime.Year()
		ride.PassengerCount, _ = strconv.Atoi(fields[greenFields["passenger_count"]])
		ride.DistMiles, _ = strconv.ParseFloat(fields[greenFields["trip_distance"]], 64)
		ride.PickupLat, _ = strconv.ParseFloat(fields[greenFields["pickup_latitude"]], 64)
		ride.PickupLon, _ = strconv.ParseFloat(fields[greenFields["pickup_longitude"]], 64)
		ride.DropLat, _ = strconv.ParseFloat(fields[greenFields["dropoff_latitude"]], 64)
		ride.DropLon, _ = strconv.ParseFloat(fields[greenFields["dropoff_longitude"]], 64)

		ride.SpeedMph = ride.DistMiles / ride.DropTime.Sub(ride.PickupTime).Hours()

		ride.TotalDollars, _ = strconv.ParseFloat(fields[greenFields["total_amount"]], 64)
		ride.DurationMinutes = ride.DropTime.Sub(ride.PickupTime).Minutes()

	} else if r.Type == 'y' {
		ride.CabType = 1
		// TODO Errors
		ride.VendorID = fields[yellowFields["vendor_id"]]
		ride.PickupTime, _ = time.Parse("2013-08-01 08:14:37", fields[yellowFields["pickup_datetime"]])
		ride.PickupDay = ride.PickupTime.Day()
		ride.PickupMonth = int(ride.PickupTime.Month())
		ride.PickupYear = ride.PickupTime.Year()
		ride.DropTime, _ = time.Parse("2013-08-01 08:14:37", fields[yellowFields["dropoff_datetime"]])
		ride.DropDay = ride.DropTime.Day()
		ride.DropMonth = int(ride.DropTime.Month())
		ride.DropYear = ride.DropTime.Year()
		ride.PassengerCount, _ = strconv.Atoi(fields[yellowFields["passenger_count"]])
		ride.DistMiles, _ = strconv.ParseFloat(fields[yellowFields["trip_distance"]], 64)
		ride.PickupLat, _ = strconv.ParseFloat(fields[yellowFields["pickup_latitude"]], 64)
		ride.PickupLon, _ = strconv.ParseFloat(fields[yellowFields["pickup_longitude"]], 64)
		ride.DropLat, _ = strconv.ParseFloat(fields[yellowFields["dropoff_latitude"]], 64)
		ride.DropLon, _ = strconv.ParseFloat(fields[yellowFields["dropoff_longitude"]], 64)

		ride.SpeedMph = ride.DistMiles / ride.DropTime.Sub(ride.PickupTime).Hours()

		ride.TotalDollars, _ = strconv.ParseFloat(fields[yellowFields["total_amount"]], 64)
		ride.DurationMinutes = ride.DropTime.Sub(ride.PickupTime).Minutes()

	} else {
		log.Println("unknown record type")
		return nil
	}

	return ride
}
