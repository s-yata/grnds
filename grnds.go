package grnds

import (
	//	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
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
	// Adjust --offset and --limit.
	if options.Offset < 0 {
		return 0, fmt.Errorf("invalid offset: offset = %d", options.Offset)
	}
	localOptions := *options
	if options.Limit > 0 {
		localOptions.Limit += options.Offset
	}
	localOptions.Offset = 0

	// Execute `select` in parallel.
	var wg sync.WaitGroup
	wg.Add(len(db.dbs))
	ns := make([]int, len(db.dbs))
	resps, err := db.makeEmptyResponses(vals)
	if err != nil {
		return 0, err
	}
	errs := make([]error, len(db.dbs))
	for i, _ := range db.dbs {
		go func(i int) {
			ns[i], errs[i] = db.dbs[i].Select(tbl, resps[i].Interface(), &localOptions)
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}

	// Merge the responses.
	outN := 0
	outRecs := reflect.ValueOf(vals).Elem()
	for i := 0; i < len(db.dbs); i++ {
		outN += ns[i]
		outRecs = reflect.AppendSlice(outRecs, resps[i].Elem())
	}
	if options.Offset < outRecs.Len() {
		outRecs = outRecs.Slice(options.Offset, outRecs.Len())
	} else {
		outRecs = outRecs.Slice(outRecs.Len(), outRecs.Len())
	}
	if (options.Limit >= 0) && (options.Limit < outRecs.Len()) {
		outRecs = outRecs.Slice(0, options.Limit)
	}
	reflect.ValueOf(vals).Elem().Set(outRecs)
	return outN, nil
}

func compareFields(lhsVal, rhsVal reflect.Value) int {
	switch lhs := lhsVal.Interface().(type) {
	case grnci.Bool:
		rhs := rhsVal.Interface().(grnci.Bool)
		if !lhs && rhs {
			return -1
		} else if lhs == rhs {
			return 0
		} else {
			return 1
		}
	case grnci.Int:
		rhs := rhsVal.Interface().(grnci.Int)
		if lhs < rhs {
			return -1
		} else if lhs == rhs {
			return 0
		} else {
			return 1
		}
	case grnci.Float:
		rhs := rhsVal.Interface().(grnci.Float)
		if lhs < rhs {
			return -1
		} else if lhs == rhs {
			return 0
		} else {
			return 1
		}
	case grnci.Time:
		rhs := rhsVal.Interface().(grnci.Time)
		if lhs < rhs {
			return -1
		} else if lhs == rhs {
			return 0
		} else {
			return 1
		}
	case grnci.Text:
		rhs := rhsVal.Interface().(grnci.Text)
		if lhs < rhs {
			return -1
		} else if lhs == rhs {
			return 0
		} else {
			return 1
		}
	default:
		return 0
	}
}

type Cond struct {
	Field *grnci.FieldInfo
	Neg   bool
}

//type Resp struct {
//	N    int
//	Recs []Rec
//}

func isPrior(conds []Cond, lhsResp, rhsResp reflect.Value) bool {
	lhsVal := lhsResp.Index(0)
	rhsVal := rhsResp.Index(0)
	for _, cond := range conds {
		lhsField := lhsVal.Field(cond.Field.ID())
		rhsField := rhsVal.Field(cond.Field.ID())
		var res int
		if !cond.Neg {
			res = compareFields(lhsField, rhsField)
		} else {
			res = compareFields(rhsField, lhsField)
		}
		if res == 0 {
			continue
		}
		return res < 0
	}
	return false
}

func chooseMostPrior(conds []Cond, resps []reflect.Value) int {
	priorID := 0
	for i := 1; i < len(resps); i++ {
		if isPrior(conds, resps[i], resps[priorID]) {
			priorID = i
		}
	}
	return priorID
}

func (db *DB) selectWithSortby(tbl string, vals interface{}, options *grnci.SelectOptions, sortby []string) (int, error) {
	// Add --sortby to --output_columns if required.
	outCols := splitValues(options.OutputColumns, ',')
	if len(outCols) != 0 {
		for _, sortby := range sortby {
			if strings.HasPrefix(sortby, "-") {
				sortby = sortby[1:]
			}
			included := false
			for _, outCol := range outCols {
				if sortby == outCol {
					included = true
					break
				}
			}
			if !included {
				outCols = append(outCols, sortby)
			}
		}
	}

	// Adjust --offset and --limit.
	if options.Offset < 0 {
		return 0, fmt.Errorf("invalid offset: offset = %d", options.Offset)
	}
	localOptions := *options
	if options.Limit > 0 {
		localOptions.Limit += options.Offset
	}
	localOptions.Offset = 0

	// Execute `select` in parallel.
	var wg sync.WaitGroup
	wg.Add(len(db.dbs))
	ns := make([]int, len(db.dbs))
	resps, err := db.makeEmptyResponses(vals)
	if err != nil {
		return 0, err
	}
	errs := make([]error, len(db.dbs))
	for i, _ := range db.dbs {
		go func(i int) {
			ns[i], errs[i] = db.dbs[i].Select(tbl, resps[i].Interface(), &localOptions)
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}

	// Find fields used in sorting.
	info := grnci.GetStructInfo(vals)
	if err := info.Error(); err != nil {
		return 0, err
	}
	conds := make([]Cond, len(sortby))
	for i, entry := range sortby {
		if conds[i].Neg = strings.HasPrefix(entry, "-"); conds[i].Neg {
			entry = entry[1:]
		}
		if conds[i].Field = info.FieldByColumnName(entry); conds[i].Field == nil {
			return 0, fmt.Errorf("no such field: entry = %s", entry)
		}
	}

	// Merge the responses.
	outN := 0
	outRecs := reflect.ValueOf(vals).Elem()
	nNonEmpties := 0
	for i := 0; i < len(db.dbs); i++ {
		outN += ns[i]
		if resps[i].IsNil() {
			continue
		}
		resps[i] = resps[i].Elem()
		if resps[i].Len() == 0 {
			continue
		}
		resps[nNonEmpties] = resps[i]
		nNonEmpties++
	}
	resps = resps[:nNonEmpties]
	for (len(resps) != 0) && (outRecs.Len() < (options.Offset + options.Limit)) {
		respID := chooseMostPrior(conds, resps)
		outRecs = reflect.Append(outRecs, resps[respID].Index(0))
		resps[respID] = resps[respID].Slice(1, resps[respID].Len())
		if resps[respID].Len() == 0 {
			for i := respID + 1; i < len(resps); i++ {
				resps[i-1] = resps[i]
			}
			resps = resps[:len(resps)-1]
		}
	}
	if options.Offset < outRecs.Len() {
		outRecs = outRecs.Slice(options.Offset, outRecs.Len())
	} else {
		outRecs = outRecs.Slice(outRecs.Len(), outRecs.Len())
	}
	if (options.Limit >= 0) && (options.Limit < outRecs.Len()) {
		outRecs = outRecs.Slice(0, options.Limit)
	}
	reflect.ValueOf(vals).Elem().Set(outRecs)
	return outN, nil
}

func (db *DB) Select(tbl string, vals interface{}, options *grnci.SelectOptions) (int, error) {
	if options == nil {
		options = grnci.NewSelectOptions()
	}
	sortby := splitValues(options.Sortby, ',')
	if len(sortby) == 0 {
		return db.selectWithoutSortby(tbl, vals, options, sortby)
	} else {
		return db.selectWithSortby(tbl, vals, options, sortby)
	}
}
