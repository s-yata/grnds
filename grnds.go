package grnds

import (
//	"bytes"
	"fmt"
	"reflect"
	"strings"
//	"sync"
//	"time"

	"github.com/groonga/grnci"
)

type DB struct {
	dbs []*grnci.DB
}

func NewDB() *DB {
	return &DB{}
}

func (db *DB) Open(name string) error {
	newDB, err := grnci.Open(name)
	if err != nil {
		return err
	}
	db.dbs = append(db.dbs, newDB)
	return nil
}

func (db *DB) Connect(host string, port int) error {
	newDB, err := grnci.Connect(host, port)
	if err != nil {
		return err
	}
	db.dbs = append(db.dbs, newDB)
	return nil
}

func (db *DB) Close() error {
	var firstErr error
	for _, db := range db.dbs {
		if err := db.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func splitValues(s string, sep byte) []string {
	var vals []string
	for {
		idx := strings.IndexByte(s, sep)
		if idx == -1 {
			s = strings.TrimSpace(s)
			if (len(vals) != 0) || (len(s) != 0) {
				vals = append(vals, s)
			}
			return vals
		}
		vals = append(vals, strings.TrimSpace(s[:idx]))
		s = s[idx+1:]
	}
}

func (db *DB) makeEmptyResponses(vals interface{}) ([]reflect.Value, error) {
	typ := reflect.TypeOf(vals)
	if typ.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("unsupported data type")
	}
	typ = typ.Elem()
	if typ.Kind() != reflect.Slice {
		return nil, fmt.Errorf("unsupported data type")
	}
	resps := make([]reflect.Value, len(db.dbs))
	for i, _ := range resps {
		resps[i] = reflect.New(typ)
	}
	return resps, nil
}

func (db *DB) selectWithoutSortby(tbl string, vals interface{}, options *grnci.SelectOptions, sortby []string) (int, error) {
	// TODO: adjust options.

	// TODO: execute `select` in parallel.

	// TODO: merge the responses.

	return 0, nil
}

func (db *DB) selectWithSortby(tbl string, vals interface{}, options *grnci.SelectOptions, sortby []string) (int, error) {
	// TODO: adjust options.

	// TODO: execute `select` in parallel.

	// TODO: merge the responses.

	return 0, nil
}

func (db *DB) Select(tbl string, vals interface{}, options *grnci.SelectOptions) (int, error) {
	sortby := splitValues(options.Sortby, ',')
	if len(sortby) == 0 {
		return db.selectWithoutSortby(tbl, vals, options, sortby)
	} else {
		return db.selectWithSortby(tbl, vals, options, sortby)
	}
}

//type Resp struct {
//	N    int
//	Recs []Rec
//}

//func selectWithoutSortby(dbs []*grnci.DB, options grnci.SelectOptions) (outN int, outRecs []Rec) {
//	// Adjust --offset and --limit.
//	offset := options.Offset
//	limit := options.Limit
//	if offset < 0 {
//		log.Fatal("negative offset is not supported")
//	}
//	if limit > 0 {
//		options.Limit += offset
//	}
//	options.Offset = 0

//	// Execute `select` in parallel.
//	var wg sync.WaitGroup
//	wg.Add(len(dbs))
//	resps := make([]Resp, len(dbs))
//	for i, _ := range dbs {
//		go func(i int) {
//			var recs []Rec
//			n, err := dbs[i].Select("Page", &recs, &options)
//			if err != nil {
//				log.Fatal("grnci.DB.Select() failed: ", err)
//			}
//			resps[i] = Resp{n, recs}
//			wg.Done()
//		}(i)
//	}
//	wg.Wait()

//	// Merge the responses.
//	for _, resp := range resps {
//		outN += resp.N
//		outRecs = append(outRecs, resp.Recs...)
//	}
//	if offset < len(outRecs) {
//		outRecs = outRecs[offset:]
//	} else {
//		outRecs = outRecs[len(outRecs):]
//	}
//	if (limit >= 0) && (limit < len(outRecs)) {
//		outRecs = outRecs[:limit]
//	}
//	return
//}

//type Cond struct {
//	Field *grnci.FieldInfo
//	Neg   bool
//}

//func compareFields(lhsVal, rhsVal reflect.Value) int {
//	switch lhs := lhsVal.Interface().(type) {
//	case grnci.Bool:
//		rhs := rhsVal.Interface().(grnci.Bool)
//		if !lhs && rhs {
//			return -1
//		} else if lhs == rhs {
//			return 0
//		} else {
//			return 1
//		}
//	case grnci.Int:
//		rhs := rhsVal.Interface().(grnci.Int)
//		if lhs < rhs {
//			return -1
//		} else if lhs == rhs {
//			return 0
//		} else {
//			return 1
//		}
//	case grnci.Float:
//		rhs := rhsVal.Interface().(grnci.Float)
//		if lhs < rhs {
//			return -1
//		} else if lhs == rhs {
//			return 0
//		} else {
//			return 1
//		}
//	case grnci.Time:
//		rhs := rhsVal.Interface().(grnci.Time)
//		if lhs < rhs {
//			return -1
//		} else if lhs == rhs {
//			return 0
//		} else {
//			return 1
//		}
//	case grnci.Text:
//		rhs := rhsVal.Interface().(grnci.Text)
//		if lhs < rhs {
//			return -1
//		} else if lhs == rhs {
//			return 0
//		} else {
//			return 1
//		}
//	default:
//		log.Fatal("unsupported data type: ", lhsVal.Type())
//	}
//	return -1
//}

//func isPrior(conds []Cond, lhsResp, rhsResp Resp) bool {
//	lhsVal := reflect.ValueOf(lhsResp.Recs[0])
//	rhsVal := reflect.ValueOf(rhsResp.Recs[0])
//	for _, cond := range conds {
//		lhsField := lhsVal.Field(cond.Field.ID())
//		rhsField := rhsVal.Field(cond.Field.ID())
//		var res int
//		if !cond.Neg {
//			res = compareFields(lhsField, rhsField)
//		} else {
//			res = compareFields(rhsField, lhsField)
//		}
//		if res == 0 {
//			continue
//		}
//		return res < 0
//	}
//	return false
//}

//func chooseMostPrior(conds []Cond, resps []Resp) int {
//	priorID := 0
//	for i := 1; i < len(resps); i++ {
//		if isPrior(conds, resps[i], resps[priorID]) {
//			priorID = i
//		}
//	}
//	return priorID
//}

//func selectWithSortby(dbs []*grnci.DB, options grnci.SelectOptions, sortby []string) (outN int, outRecs []Rec) {
//	// Add --sortby to --output_columns if required.
//	output := splitValues(options.OutputColumns, ',')
//	if len(output) != 0 {
//		for _, sortby := range sortby {
//			if strings.HasPrefix(sortby, "-") {
//				sortby = sortby[1:]
//			}
//			included := false
//			for _, output := range output {
//				if sortby == output {
//					included = true
//					break
//				}
//			}
//			if !included {
//				output = append(output, sortby)
//			}
//		}
//	}

//	// Adjust --offset and --limit.
//	offset := options.Offset
//	limit := options.Limit
//	if offset < 0 {
//		log.Fatal("negative offset is not supported")
//	}
//	if limit > 0 {
//		options.Limit += offset
//	}
//	options.Offset = 0

//	// Execute `select` in parallel.
//	var wg sync.WaitGroup
//	wg.Add(len(dbs))
//	resps := make([]Resp, len(dbs))
//	for i, _ := range dbs {
//		go func(i int) {
//			var recs []Rec
//			n, err := dbs[i].Select("Page", &recs, &options)
//			if err != nil {
//				log.Fatal("grnci.DB.Select() failed: ", err)
//			}
//			resps[i] = Resp{n, recs}
//			wg.Done()
//		}(i)
//	}
//	wg.Wait()

//	// Find fields used in sorting.
//	info := grnci.GetStructInfo(outRecs)
//	if err := info.Error(); err != nil {
//		log.Fatal("grnci.GetStructInfo() failed: ", err)
//	}
//	conds := make([]Cond, len(sortby))
//	for i, sortby := range sortby {
//		if conds[i].Neg = strings.HasPrefix(sortby, "-"); conds[i].Neg {
//			sortby = sortby[1:]
//		}
//		if conds[i].Field = info.FieldByColumnName(sortby); conds[i].Field == nil {
//			log.Fatal("grnci.StructInfo.FieldByColumnName() failed: ", sortby)
//		}
//	}

//	// Merge the responses.
//	cnt := 0
//	for i := 0; i < len(resps); i++ {
//		outN += resps[i].N
//		if len(resps[i].Recs) == 0 {
//			continue
//		}
//		resps[cnt] = resps[i]
//		cnt++
//	}
//	resps = resps[:cnt]
//	for (len(resps) != 0) && (len(outRecs) < (offset + limit)) {
//		respID := chooseMostPrior(conds, resps)
//		outRecs = append(outRecs, resps[respID].Recs[0])
//		resps[respID].Recs = resps[respID].Recs[1:]
//		if len(resps[respID].Recs) == 0 {
//			for i := respID + 1; i < len(resps); i++ {
//				resps[i-1] = resps[i]
//			}
//			resps = resps[:len(resps)-1]
//		}
//	}
//	return
//}

//func runSelect(dbs []*grnci.DB, options *grnci.SelectOptions) (int, []Rec) {
//	sortby := splitValues(options.Sortby, ',')
//	if len(sortby) == 0 {
//		return selectWithoutSortby(dbs, *options)
//	} else {
//		return selectWithSortby(dbs, *options, sortby)
//	}
//}
