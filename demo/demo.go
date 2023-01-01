package main

import (
	"btreedb"
	"bytes"
	"fmt"
	"log"
)

func main() {
	maxKeySize := 128
	dbFilePath := "./test.db"
	btreeDegree := 256
	db, err := btreedb.CreateDB(dbFilePath, maxKeySize, btreeDegree)
	if err != nil {
		log.Fatal(err)
	}
	key := []byte("key1")
	val := []byte("val1")
	err = db.Insert(key, val)
	if err != nil {
		log.Fatal(err)
	}
	db.Close()

	db, err = btreedb.OpenDB(dbFilePath)
	if err != nil {
		log.Fatal(err)
	}
	valRead, err := db.Find(key)
	if err != nil {
		log.Fatal(err)
	}
	if bytes.Compare(val, valRead) != 0 {
		fmt.Printf("the value read not equal to the value inserted, val: %s, valRead: %s", string(val), string(valRead))
	}
}