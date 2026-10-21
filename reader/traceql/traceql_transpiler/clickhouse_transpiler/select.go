package clickhouse_transpiler

import (
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// listedIntrinsics are the intrinsics every listed span or trace carries in
// search results, so select() reads nothing for them.
var listedIntrinsics = map[string]bool{
	"duration": true, "span:duration": true, "span:id": true,
	"traceDuration": true, "trace:duration": true, "trace:id": true,
	"rootName": true, "trace:rootName": true, "rootServiceName": true, "trace:rootService": true,
}

// nestedSetColumns maps the nested-set intrinsics to their columns in the
// side table. Selected, they keep their names as keys.
var nestedSetColumns = map[string]string{
	"nestedSetLeft": "left", "nestedSetRight": "right", "nestedSetParent": "parent",
}

// IsNestedSetKey reports whether a selected key holds a nested-set value.
func IsNestedSetKey(key string) bool {
	_, ok := nestedSetColumns[key]
	return ok
}

// spanValueKeys lists the keys search reads for each listed span, each once in
// order: those of the select() stages, then those the spanset filters read.
// An attribute named like a nested-set intrinsic shares its key.
func spanValueKeys(q *traceql_ir.Query) ([]string, error) {
	var keys []string
	seen := map[string]bool{}
	for _, f := range append(q.SelectedFields(), q.FilteredFields()...) {
		if listedIntrinsics[f.Name] {
			continue
		}
		key := f.Name
		if !IsNestedSetKey(key) {
			ref, err := fieldRefOf(f.Name)
			if err != nil {
				return nil, err
			}
			key = ref.key
		}
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// splitSelected separates the nested-set keys from the attribute keys.
func splitSelected(keys []string) (attrKeys, nestedSet []string) {
	for _, key := range keys {
		if IsNestedSetKey(key) {
			nestedSet = append(nestedSet, key)
		} else {
			attrKeys = append(attrKeys, key)
		}
	}
	return attrKeys, nestedSet
}
