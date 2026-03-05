package heartwood

import (
	"github.com/boltdb/bolt"
)

const BucketName string = "events"

type EventBlob struct {
	ID   string
	JSON string
}

func SetupBoltDB(boltdb *bolt.DB) error {
	return boltdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BucketName))
		return err
	})
}

func BatchCheckEventsExist(boltdb *bolt.DB, eventIDs []string) map[string]bool {
	existsMap := make(map[string]bool)

	_ = boltdb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketName))
		if bucket == nil {
			return nil
		}
		for _, id := range eventIDs {
			existsMap[id] = bucket.Get([]byte(id)) != nil
		}
		return nil
	})

	return existsMap
}

func BatchWriteEvents(boltdb *bolt.DB, events []EventBlob) error {
	return boltdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketName))
		for _, event := range events {
			if err := bucket.Put(
				[]byte(event.ID), []byte(event.JSON),
			); err != nil {
				return err
			}
		}
		return nil
	})
}
