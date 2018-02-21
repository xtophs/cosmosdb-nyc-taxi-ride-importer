package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
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
	"total_amount":          18,
	"improvement_surcharge": 17,
}

// Record records
type Record struct {
	Type rune
	Val  string
}

// Ride rides
type Ride struct {
	ID              bson.ObjectId `bson:"_id"`
	VendorID        string        `bson:"vendor_id"`
	SpeedMph        float64       `bson:"speed_mph"`
	TotalDollars    float64       `bson:"total_amount_dollars"`
	DurationMinutes float64       `bson:"duration_minutes"`
	PassengerCount  int           `bson:"passenger_count"`
	DistMiles       float64       `bson:"distance_miles"`
	PickupTime      *time.Time    `bson:"pickup_time"`
	PickupDay       int           `bson:"pickup_day"`
	PickupMDay      int           `bson:"pickup_mday"`
	PickupMonth     int           `bson:"pickup_month"`
	PickupYear      int           `bson:"pickup_year"`
	PickupLat       float64       `bson:"pickup_latitude"`
	PickupLon       float64       `bson:"pickup_longitude"`
	DropLat         float64       `bson:"drop_latitude"`
	DropLon         float64       `bson:"drop_longitude"`
	DropTime        *time.Time    `bson:"drop_time"`
	DropDay         int           `bson:"drop_day"`
	DropMDay        int           `bson:"drop_mday"`
	DropMonth       int           `bson:"drop_month"`
	DropYear        int           `bson:"drop_year"`
	CabType         int           `bson:"cab_type"`
	//pickupGridID    uint64    `bson:"pickup_grid_id, omitempty"`
	//dropGridID      uint64    `bson:"drop_grid_id, omitempty"`
	//pickupElevation float64   `bson:"pickup_elevation, omitempty"`
	//dropElevation   float64   `bson:"drop_elevation, omitempty"`
}

func (r *Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

func parseDate(datetime string) (*time.Time, error) {
	dateFormat := "2006-01-02 15:04:05"

	if len(datetime) == 0 {
		log.Printf("time was empty")
		return nil, fmt.Errorf("Can't parse empty date")
	}

	t, err := time.Parse(dateFormat, datetime)
	if err != nil {
		log.Printf("Error parsing date %s\n", err.Error())
		return nil, err
	}

	return &t, nil
}

func (r *Record) safeParseDateField(fieldName string, fields []string) (t *time.Time, err error) {

	var fieldNames map[string]int
	var tm time.Time
	dateFormat := "2006-01-02 15:04:05"

	if r.Type == 'g' {
		fieldNames = greenFields
	} else if r.Type == 'y' {
		fieldNames = yellowFields
	} else {
		return nil, fmt.Errorf("Bad Record Type %v", r.Type)
	}

	if fieldNames[fieldName] < len(fields) {
		if len(fields[fieldNames[fieldName]]) == 0 {
			return nil, fmt.Errorf("Empty record for %s", fieldName)
		}
		tm, err = time.Parse(dateFormat, fields[fieldNames[fieldName]])
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Error parsing %s", fieldName))
		}
	} else {
		return nil, fmt.Errorf("Bad index %d for field %s, max Index %d", fieldNames[fieldName], fieldName, len(fields))
	}

	return &tm, nil
}

func (r *Record) safeParseIntegerField(fieldName string, fields []string) (i int, err error) {

	var fieldNames map[string]int

	if r.Type == 'g' {
		fieldNames = greenFields
	} else if r.Type == 'y' {
		fieldNames = yellowFields
	} else {
		return -1, fmt.Errorf("Bad Record Type %v", r.Type)
	}

	if fieldNames[fieldName] < len(fields) {
		if len(fields[fieldNames[fieldName]]) == 0 {
			return -1, fmt.Errorf("Empty record for %s", fieldName)
		}
		i, err = strconv.Atoi(fields[fieldNames[fieldName]])
		if err != nil {
			return -1, errors.Wrap(err, fmt.Sprintf("Error parsing %s", fieldName))
		}
	} else {
		return -1, fmt.Errorf("Bad index %d for field %s, max Index %d", fieldNames[fieldName], fieldName, len(fields))
	}

	return i, nil
}

func (r *Record) safeParseFloatField(fieldName string, fields []string) (f float64, err error) {

	var fieldNames map[string]int

	if r.Type == 'g' {
		fieldNames = greenFields
	} else if r.Type == 'y' {
		fieldNames = yellowFields
	} else {
		return -1, fmt.Errorf("Bad Record Type %v", r.Type)
	}

	if fieldNames[fieldName] < len(fields) {
		if len(fields[fieldNames[fieldName]]) == 0 {
			return -1, fmt.Errorf("Empty record for %s, %d, %v", fieldName, fieldNames[fieldName], fields)
		}
		f, err = strconv.ParseFloat(fields[fieldNames[fieldName]], 64)
		if err != nil {
			return -1, errors.Wrap(err, fmt.Sprintf("Error parsing %s", fieldName))
		}
	} else {
		return -1, fmt.Errorf("Bad index %d for field %s, max Index %d", fieldNames[fieldName], fieldName, len(fields))
	}

	return f, nil
}

func (r *Record) toRide() (ride *Ride, err error) {
	fields, _ := r.Clean()

	ride = &Ride{}

	ride.ID = bson.NewObjectId()
	if r.Type == 'g' {
		ride.CabType = 0

	} else if r.Type == 'y' {
		ride.CabType = 1
	} else {
		log.Printf("unknown record type, %v\n", r)
		return nil, nil
	}

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
	// TODO Errors
	ride.VendorID = fields[greenFields["vendor_id"]]

	ride.PickupTime, err = r.safeParseDateField("pickup_datetime", fields)
	if err != nil {
		return nil, err
	}
	ride.PickupDay = ride.PickupTime.Day()
	ride.PickupMonth = int(ride.PickupTime.Month())
	ride.PickupYear = ride.PickupTime.Year()
	ride.DropTime, err = r.safeParseDateField("dropoff_datetime", fields)
	if err != nil {
		return nil, err
	}
	ride.DropDay = ride.DropTime.Day()
	ride.DropMonth = int(ride.DropTime.Month())
	ride.DropYear = ride.DropTime.Year()
	ride.PassengerCount, err = r.safeParseIntegerField("passenger_count", fields)
	if err != nil {
		return nil, err
	}

	ride.DistMiles, err = r.safeParseFloatField("trip_distance", fields)
	if err != nil {
		return nil, err
	}

	ride.PickupLat, err = r.safeParseFloatField("pickup_latitude", fields)
	if err != nil {
		return nil, err
	}

	ride.PickupLon, err = r.safeParseFloatField("pickup_longitude", fields)
	if err != nil {
		return nil, err
	}

	ride.DropLat, err = r.safeParseFloatField("dropoff_latitude", fields)
	if err != nil {
		return nil, err
	}

	ride.DropLon, err = r.safeParseFloatField("dropoff_longitude", fields)
	if err != nil {
		return nil, err
	}

	ride.SpeedMph = ride.DistMiles / ride.DropTime.Sub(*ride.PickupTime).Hours()
	ride.TotalDollars, err = r.safeParseFloatField("total_amount", fields)
	if err != nil {
		return nil, err
	}

	ride.DurationMinutes = ride.DropTime.Sub(*ride.PickupTime).Minutes()

	return ride, nil
}
