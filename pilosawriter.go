package main

import (
	"log"
	"strings"
	"time"

	"github.com/pilosa/pdk"
	//"github.com/pilosa/pilosa"
)

type PilosaWriter struct {
	greenBms  []pdk.BitMapper
	yellowBms []pdk.BitMapper
	ams       []pdk.AttrMapper
	importer  pdk.PilosaImporter
}

func NewPilosaWriter(host string, index string, bufferSize int) *PilosaWriter {

	frames := []string{"cab_type", "passenger_count", "total_amount_dollars", "pickup_time", "pickup_day", "pickup_mday", "pickup_month", "pickup_year", "drop_time", "drop_day", "drop_mday", "drop_month", "drop_year", "dist_miles", "duration_minutes", "speed_mph", "pickup_grid_id", "drop_grid_id", "pickup_elevation", "drop_elevation"}

	// pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	// if err != nil {
	// 	return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	// }
	//setupClient := pcli.NewClientWithURI(pilosaURI)

	return &PilosaWriter{
		greenBms:  getBitMappers(greenFields),
		yellowBms: getBitMappers(yellowFields),
		ams:       getAttrMappers(),
		importer:  pdk.NewImportClient(host, index, frames, bufferSize),
	}
}

func (w *PilosaWriter) write(record *Record, recordManager *RecordManager) {
	var bms []pdk.BitMapper
	var cabType uint64

	fields, ok := record.Clean()
	if !ok {
		recordManager.skippedRecs.Add(1)
		return
	}

	if record.Type == 'g' {
		bms = w.greenBms
		cabType = 0
	} else if record.Type == 'y' {
		bms = w.yellowBms
		cabType = 1
	} else {
		log.Println("unknown record type %v", record)
		return
	}

	bitsToSet := make([]BitFrame, 0)
	bitsToSet = append(bitsToSet, BitFrame{Bit: cabType, Frame: "cab_type"})
	for _, bm := range bms {
		if len(bm.Fields) != len(bm.Parsers) {
			// TODO if len(pm.Parsers) == 1, use that for all fields
			log.Fatalf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
		}

		// parse fields into a slice `parsed`
		parsed := make([]interface{}, 0, len(bm.Fields))
		for n, fieldnum := range bm.Fields {
			parser := bm.Parsers[n]
			if fieldnum >= len(fields) {
				log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
				recordManager.skippedRecs.Add(1)
				return
			}
			parsedField, err := parser.Parse(fields[fieldnum])
			if err != nil && fields[fieldnum] == "" {
				recordManager.skippedRecs.Add(1)
				return
			} else if err != nil {
				log.Printf("parsing: field: %v err: %v bm: %v rec: %v", fields[fieldnum], err, bm, record)
				recordManager.skippedRecs.Add(1)
				return
			}
			parsed = append(parsed, parsedField)
		}

		// map those fields to a slice of IDs
		ids, err := bm.Mapper.ID(parsed...)
		if err != nil {
			if err.Error() == "point (0, 0) out of range" {
				recordManager.nullLocs.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if strings.Contains(bm.Frame, "grid_id") && strings.Contains(err.Error(), "out of range") {
				recordManager.badLocs.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if bm.Frame == "speed_mph" && strings.Contains(err.Error(), "out of range") {
				recordManager.badSpeeds.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if bm.Frame == "total_amount_dollars" && strings.Contains(err.Error(), "out of range") {
				recordManager.badTotalAmnts.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if bm.Frame == "duration_minutes" && strings.Contains(err.Error(), "out of range") {
				recordManager.badDurations.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if bm.Frame == "passenger_count" && strings.Contains(err.Error(), "out of range") {
				recordManager.badPassCounts.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			if bm.Frame == "dist_miles" && strings.Contains(err.Error(), "out of range") {
				recordManager.badDist.Add(1)
				recordManager.skippedRecs.Add(1)
				return
			}
			log.Printf("mapping: bm: %v, err: %v rec: %v", bm, err, record)
			recordManager.skippedRecs.Add(1)
			recordManager.badUnknowns.Add(1)
			return
		}
		for _, id := range ids {
			bitsToSet = append(bitsToSet, BitFrame{Bit: uint64(id), Frame: bm.Frame})
		}
	}
	columnID := recordManager.nexter.Next()
	for _, bit := range bitsToSet {
		w.importer.SetBit(bit.Bit, columnID, bit.Frame)
	}

}

func getAttrMappers() []pdk.AttrMapper {
	ams := []pdk.AttrMapper{}

	return ams
}

func getBitMappers(fields map[string]int) []pdk.BitMapper {
	// map a pair of floats to a grid sector of a rectangular region
	gm := pdk.GridMapper{
		Xmin: -74.27,
		Xmax: -73.69,
		Xres: 100,
		Ymin: 40.48,
		Ymax: 40.93,
		Yres: 100,
	}

	elevFloatMapper := pdk.LinearFloatMapper{
		Min: -32,
		Max: 195,
		Res: 46,
	}

	gfm := pdk.NewGridToFloatMapper(gm, elevFloatMapper, elevations)

	// map a float according to a custom set of bins
	// seems like the LFM defined below is more sensible
	/*
		fm := pdk.FloatMapper{
			Buckets: []float64{0, 0.5, 1, 2, 5, 10, 25, 50, 100, 200},
		}
	*/

	// this set of bins is equivalent to rounding to nearest int (TODO verify)
	lfm := pdk.LinearFloatMapper{
		Min: -0.5,
		Max: 3600.5,
		Res: 3601,
	}

	// map the (pickupTime, dropTime) pair, according to the duration in minutes, binned using `fm`
	durm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			return end.Sub(start).Minutes()
		},
		Mapper: lfm,
	}

	// map the (pickupTime, dropTime, dist) triple, according to the speed in mph, binned using `fm`
	speedm := pdk.CustomMapper{
		Func: func(fields ...interface{}) interface{} {
			start := fields[0].(time.Time)
			end := fields[1].(time.Time)
			dist := fields[2].(float64)
			return dist / end.Sub(start).Hours()
		},
		Mapper: lfm,
	}

	tp := pdk.TimeParser{Layout: "2006-01-02 15:04:05"}

	bms := []pdk.BitMapper{
		pdk.BitMapper{
			Frame:   "passenger_count",
			Mapper:  pdk.IntMapper{Min: 0, Max: 9},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields["passenger_count"]},
		},
		pdk.BitMapper{
			Frame:   "total_amount_dollars",
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["total_amount"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_time",
			Mapper:  pdk.TimeOfDayMapper{Res: 48},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_mday",
			Mapper:  pdk.DayOfMonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_month",
			Mapper:  pdk.MonthMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "drop_year",
			Mapper:  pdk.YearMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "dist_miles", // note "_miles" is a unit annotation
			Mapper:  lfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}},
			Fields:  []int{fields["trip_distance"]},
		},
		pdk.BitMapper{
			Frame:   "duration_minutes",
			Mapper:  durm,
			Parsers: []pdk.Parser{tp, tp},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"]},
		},
		pdk.BitMapper{
			Frame:   "speed_mph",
			Mapper:  speedm,
			Parsers: []pdk.Parser{tp, tp, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_datetime"], fields["dropoff_datetime"], fields["trip_distance"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["pickup_longitude"], fields["pickup_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "drop_grid_id",
			Mapper:  gm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
		pdk.BitMapper{
			Frame:   "drop_elevation",
			Mapper:  gfm,
			Parsers: []pdk.Parser{pdk.FloatParser{}, pdk.FloatParser{}},
			Fields:  []int{fields["dropoff_longitude"], fields["dropoff_latitude"]},
		},
	}

	return bms
}
