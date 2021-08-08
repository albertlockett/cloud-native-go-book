package main

import (
	"albertlockett.ca/cloud-native-go/kv"
	"albertlockett.ca/cloud-native-go/txlog"
	"fmt"
)

var txlogger txlog.TransactionLogger

func initializeTransactionLog() error {
	var err error
	txlogger, err = txlog.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := txlogger.ReadEvents()
	e, ok := txlog.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case txlog.EventDelete:
				err = kv.Delete(e.Key)
			case txlog.EventPut:
				err = kv.Put(e.Key, e.Value)
			}
		}
	}

	txlogger.Run()
	return err
}
