package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

type CosmosWriter struct {
	info       *mgo.DialInfo
	session    *mgo.Session
	collection *mgo.Collection
}

func NewCosmosWriter(db string, pw string) (*CosmosWriter, error) {
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

	s, err := mgo.DialWithInfo(i)
	if err != nil {
		return nil, errors.Errorf("Can't connect to mongo, go error %v\n", err)
	}
	// SetSafe changes the session safety mode.
	// If the safe parameter is nil, the session is put in unsafe mode, and writes become fire-and-forget,
	// without error checking. The unsafe mode is faster since operations won't hold on waiting for a confirmation.
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode.
	s.SetMode(mgo.Eventual, false)
	s.SetSafe(&mgo.Safe{})

	c := s.DB(db).C("ridesColl")

	return &CosmosWriter{
		info:       i,
		session:    s,
		collection: c,
	}, nil
}

func (w *CosmosWriter) write(record *Record, recordManager *RecordManager) error {
	// Create a session which maintains a pool of socket connections
	// to our MongoDB.

	// Where do I do that?
	//defer session.Close()

	// fields, ok := record.Clean()
	// if !ok {
	// 	recordManager.skippedRecs.Add(1)
	// 	return nil
	// }

	cancel := make(chan error, 1)
	go func() {
		err := w.WriteToCosmos(record)
		if err != nil {
			select {
			case cancel <- err:
			}
		}
	}()
	return nil
}

// WriteToCosmos writes to Cosmos
func (w *CosmosWriter) WriteToCosmos(rec *Record) error {
	// insert documents into ToCosmoscollection

	ride := rec.toRide()
	err := w.collection.Insert(ride)
	if err != nil {
		return errors.Wrap(err, "inserting ride")
	}
	return nil
}
