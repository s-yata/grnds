package grnds

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/groonga/grnci"
)

func TestSelectWithoutSortby(t *testing.T) {
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
		recs := make([]Rec, 5)
		for j, _ := range recs {
			recs[j].Val = grnci.Int(rand.Intn(100))
		}
		if _, err := db.LoadEx("tbl", recs, nil); err != nil {
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

	{
		var recs []Rec
		n, err := db.Select("tbl", &recs, nil)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("default: n = %v, recs = %v\n", n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Limit = -1
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("limit = %d: n = %v, recs = %v\n", options.Limit, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Offset = 10
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("offset = %d: n = %v, recs = %v\n", options.Offset, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Filter = "Val<50"
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("filter = \"%s\": n = %v, recs = %v\n", options.Filter, n, recs)
	}
}

func TestSelectWithSortby(t *testing.T) {
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
		recs := make([]Rec, 5)
		for j, _ := range recs {
			recs[j].Val = grnci.Int(rand.Intn(100))
		}
		if _, err := db.LoadEx("tbl", recs, nil); err != nil {
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

	{
		options := grnci.NewSelectOptions()
		options.Sortby = "Val"
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("sortby = \"%s\": n = %v, recs = %v\n", options.Sortby, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Sortby = "-Val"
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("sortby = \"%s\": n = %v, recs = %v\n", options.Sortby, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Sortby = "Val"
		options.Offset = 5
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("sortby = \"%s\", offset = %d: n = %v, recs = %v\n",
			options.Sortby, options.Offset, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Sortby = "Val"
		options.Offset = 3
		options.Limit = 5
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("sortby = \"%s\", offset = %d, limit = %d: n = %v, recs = %v\n",
			options.Sortby, options.Offset, options.Limit, n, recs)
	}

	{
		options := grnci.NewSelectOptions()
		options.Filter = "Val%2==0"
		options.Sortby = "Val"
		var recs []Rec
		n, err := db.Select("tbl", &recs, options)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("sortby = \"%s\", filter = \"%s\": n = %v, recs = %v\n",
			options.Sortby, options.Filter, n, recs)
	}
}
