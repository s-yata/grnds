package grnds

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/groonga/grnci"
)

func TestMain(t *testing.T) {
	type Rec struct {
		Val grnci.Int
	}

	if err := os.RemoveAll("/tmp/grnds"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		dir := fmt.Sprintf("/tmp/grnds/%02d", i)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		name := filepath.Join(dir, "db")
		db, err := grnci.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		rec := Rec{Val: grnci.Int(i * 100)}
		if _, err := db.LoadEx("tbl", rec, nil); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	db := NewDB()
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("/tmp/grnds/%02d/db", i)
		if err := db.Open(name); err != nil {
			t.Fatal(err)
		}
	}
	var recs []Rec
	n, err := db.Select("tbl", &recs, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("n:", n)
	fmt.Println("recs:", recs)
}
