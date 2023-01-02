package btreedb

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"testing"
)

var dbFilePath string = "test.db"
var db *DB

func createDB(degree int) {
	maxKeySize := 128
	var err error
	db, err = CreateDB(dbFilePath, maxKeySize, degree)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDBInsert(t *testing.T) {
	createDB(5)
	key := []byte("key1")
	val := []byte("val1")
	if err := db.Insert(key, val); err != nil {
		t.Error(err)
	}
	valRead, err := db.Find(key)
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(val, valRead) != 0 {
		t.Error("val not equal")
	}
}

func TestDBUpdate(t *testing.T) {
	createDB(5)
	key := []byte("key1")
	val := []byte("val1")
	if err := db.Insert(key, val); err != nil {
		t.Error(err)
	}
	valRead, err := db.Find(key)
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(val, valRead) != 0 {
		t.Error("val not equal")
	}

	newVal := []byte("newval1")
	if err := db.Insert(key, newVal); err != nil {
		t.Error(err)
	}
	valRead, err = db.Find(key)
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(newVal, valRead) != 0 {
		t.Error("val not equal")
	}
}

func TestDBUpdateTriggeredCompact(t *testing.T) {
	createDB(250)
	key := []byte("key1")
	for i := 0; i < 1000; i++ {
		val := []byte("abcdefghijklmnopqrstuvwxyz1234567890")
		if err := db.Insert(key, val); err != nil {
			t.Error(err)
		}
		valRead, err := db.Find(key)
		if err != nil {
			t.Error(err)
		}
		if bytes.Compare(val, valRead) != 0 {
			t.Error("val not equal")
		}
	}

	newVal := []byte("newval1")
	if err := db.Insert(key, newVal); err != nil {
		t.Error(err)
	}
	valRead, err := db.Find(key)
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(newVal, valRead) != 0 {
		t.Error("val not equal")
	}
}

func TestDBInsert2(t *testing.T) {
	db, err := OpenDB(dbFilePath)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("ssskey")
	val := []byte("sssval")
	if err := db.Insert(key, val); err != nil {
		t.Error(err)
	}
}

func TestDBInsertSplit(t *testing.T) {
	createDB(5)
	m := map[string]string{
		"key1": "val1",
		"key2": "val2",
		"key3": "val3",
		"key4": "val4",
		"key5": "val5",
	}
	for k, v := range m {
		key := []byte(k)
		val := []byte(v)
		if err := db.Insert(key, val); err != nil {
			t.Errorf("key: %s, err: %v", k, err)
		}
	}
	if err := db.Insert([]byte("key0"), []byte("val0")); err != nil {
		t.Errorf("key: key0, err: %v", err)
	}
	for k, v := range m {
		key := []byte(k)
		val := []byte(v)
		valRead, err := db.Find(key)
		if err != nil {
			valRead, err = db.Find(key)
			t.Fatalf("key: %s, err: %v", k, err)
		}

		if bytes.Compare(val, valRead) != 0 {
			t.Fatal("val not equal")
		}
	}
	db.PrintDotGraph2("mydb.dot")
	/*
	   valRead, err := db.Find(key)

	   	if err != nil {
	   		t.Error(err)
	   	}

	   	if bytes.Compare(val, valRead) != 0 {
	   		t.Error("val not equal")
	   	}
	*/
}

func TestDBDelete(t *testing.T) {
	createDB(5)
	m := map[string]string{
		"key1": "val1",
		"key2": "val2",
		"key3": "val3",
		"key4": "val4",
		"key5": "val5",
	}
	for k, v := range m {
		key := []byte(k)
		val := []byte(v)
		if err := db.Insert(key, val); err != nil {
			t.Errorf("key: %s, err: %v", k, err)
		}
		fmt.Fprintf(os.Stderr, "after insert, k: %s, v: %s\n", k, v)
		db.PrintDebugInfo()
	}
	if err := db.Insert([]byte("key0"), []byte("val0")); err != nil {
		t.Errorf("key: key0, err: %v", err)
	}
	fmt.Fprintf(os.Stderr, "after insert, key0, val0\n")
	db.PrintDebugInfo()
	for k := range m {
		key := []byte(k)
		db.Delete(key)
		_, err := db.Find(key)
		if err != ERR_KEY_NOT_EXIST {
			t.Fatalf("key: %s, err: %v", k, err)
		}

		fmt.Fprintf(os.Stderr, "after delete, k: %s\n", k)
		db.PrintDebugInfo()
	}
	//db.PrintDotGraph2("mydb.dot")
}

func TestBTreeDelete2(t *testing.T) {
	createDB(5)
	intSlice := getUniqueInts(1500)
	for i, num := range intSlice {
		_ = i
		key := fmt.Sprintf("k%d", num)
		val := fmt.Sprintf("v%d", num)
		db.Insert([]byte(key), []byte(val))
	}
	rand.Shuffle(len(intSlice), func(i, j int) { intSlice[i], intSlice[j] = intSlice[j], intSlice[i] })
	//db.PrintDotGraph2("mydb.dot")
	checkMemRequired := func(node *Node) bool {
		actualMemRequred := *node.ActualMemRequired()
		expectActualMemRequred := uint16(0)
		for i := 0; i < node.nKeys(); i++ {
			expectActualMemRequred += node.getKeySize(i)
		}
		if node.isLeaf() {
			for i := 0; i < node.nKeys(); i++ {
				expectActualMemRequred += node.getValSize(i)
			}
		}
		if expectActualMemRequred != actualMemRequred {
			t.Fatalf("error not equal: actualMemRequred: %d, expectActualMemRequred: %d\n", actualMemRequred, expectActualMemRequred)
			t.FailNow()
			return true
		}
		return false
	}
	for i, ri := range intSlice {
		_ = i
		randKey := fmt.Sprintf("k%d", ri)
		db.Delete([]byte(randKey))
		//db.PrintDebugInfo()
		db.Traverse(checkMemRequired)
		_, err := db.Find([]byte(randKey))
		if err != ERR_KEY_NOT_EXIST {
			t.Fatalf("key: %s, err: %v", randKey, err)
		}
		db.Traverse(checkMemRequired)
	}
	for i := 0; i < len(intSlice); i++ {
		num := intSlice[i]
		key := fmt.Sprintf("k%d", num)
		key2 := fmt.Sprintf("kk%d", num)
		val := fmt.Sprintf("v%d", num)
		db.Insert([]byte(key), []byte(val))
		db.Insert([]byte(key2), []byte(val))
	}
	db.PrintDebugInfo()
}

func getUniqueInts(n int) []int {
	m := make(map[int]struct{})
	for {
		num := rand.Intn(100 * n)
		m[num] = struct{}{}
		if len(m) >= n {
			break
		}
	}
	res := make([]int, len(m))
	cnt := 0
	for num := range m {
		res[cnt] = num
		cnt++
	}
	sort.Ints(res)
	rand.Shuffle(len(res), func(i, j int) { res[i], res[j] = res[j], res[i] })
	return res
}

func TestBTreeFind(t *testing.T) {
	createDB(5)
	intSlice := getUniqueInts(360)
	for i, num := range intSlice {
		_ = i
		key := fmt.Sprintf("k%d", num)
		val := fmt.Sprintf("v%d", num)
		db.Insert([]byte(key), []byte(val))
	}
	//db.PrintDotGraph2("mydb.dot")
	for i := 0; i < 5; i++ {
		ri := rand.Intn(len(intSlice))
		randKey := fmt.Sprintf("k%d", intSlice[ri])
		expectVal := fmt.Sprintf("v%d", intSlice[ri])
		val, err := db.Find([]byte(randKey))
		if err != nil || bytes.Compare(val, []byte(expectVal)) != 0 {
			t.Errorf("failed, key: %s, val: %s, expectVal: %s\n", randKey, val, expectVal)
		}
	}
}

func TestDBFind(t *testing.T) {
	db, err := OpenDB(dbFilePath)
	if err != nil {
		t.Fatal(err)
	}

	m := map[string]string{
		"k23456": "v23456",
		"k34375": "v34375",
		"k25737": "v25737",
		"k6682":  "v6682",
		"k31871": "v31871",
	}
	for k, v := range m {
		key := []byte(k)
		expVal := []byte(v)
		valRead, err := db.Find(key)
		if err != nil || bytes.Compare(valRead, []byte(expVal)) != 0 {
			t.Errorf("failed, key: %s, val: %s, expectVal: %s\n", key, valRead, expVal)
		}
	}

}
