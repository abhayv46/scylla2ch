// types.go — CQL → ClickHouse type mapping.
//
// Maps every CQL data type to its ClickHouse equivalent for auto-create DDL.
// This must stay in sync with ConvertValue() in consumer/consumer.go — they
// form a contract: types.go decides the column type, ConvertValue() produces
// Go values that match that type.
//
// Type alignment (verified by tests in types_test.go):
//   CQL timestamp → CH Int64     (ConvertValue sends UnixMilli int64)
//   CQL uuid      → CH String    (ConvertValue sends UUID.String())
//   CQL boolean   → CH UInt8     (ConvertValue sends 1/0 uint8)
//   CQL map<K,V>  → CH String    (ConvertValue sends JSON string)
//   CQL list<T>   → CH Array(T)  (ConvertValue sends Go slice)
//
// Non-PK columns are wrapped in Nullable() — CDC delta rows only contain
// columns that were actually written, so missing columns must accept NULL.
// Arrays are never nullable (ClickHouse limitation).
package converter

import "strings"

// CQLToClickHouseType maps ANY CQL type to its ClickHouse equivalent.
// Designed to be fully dynamic — works with any ScyllaDB table schema.
// PK columns are NOT nullable. Non-PK columns are Nullable.
// Array columns are never nullable (ClickHouse limitation).
func CQLToClickHouseType(cqlType string, isPK bool) string {
	chType := mapCQLType(cqlType)
	if isPK || strings.HasPrefix(chType, "Array(") {
		return chType // PK columns and arrays are never nullable
	}
	return "Nullable(" + chType + ")"
}

// mapCQLType converts a CQL type string to the corresponding ClickHouse type.
//
// Complete CQL type coverage:
//   Scalars:    text, varchar, ascii, bigint, int, smallint, tinyint, float, double,
//               boolean, counter, varint, decimal, duration
//   Time:       timestamp, date, time
//   Identity:   uuid, timeuuid
//   Binary:     blob, inet
//   Collections: list<T>, set<T>, map<K,V> (+ frozen variants)
//   Complex:    tuple<...>, UDT (user-defined types)
//
// All types map to what convertValue() in consumer.go actually sends:
//   - timestamp → Int64 (consumer sends UnixMilli)
//   - uuid/timeuuid → String (consumer sends v.String())
//   - inet → String (consumer sends net.IP.String())
//   - decimal/varint → String (consumer sends .String())
//   - boolean → UInt8 (consumer sends 1/0)
//   - map/tuple/UDT → String (consumer sends JSON)
func mapCQLType(cqlType string) string {
	// Normalize
	cqlType = strings.TrimSpace(strings.ToLower(cqlType))

	// === Scalar types ===
	switch cqlType {
	case "text", "varchar", "ascii":
		return "String"
	case "bigint", "counter":
		return "Int64"
	case "int":
		return "Int32"
	case "smallint":
		return "Int16"
	case "tinyint":
		return "Int8"
	case "float":
		return "Float32"
	case "double":
		return "Float64"
	case "boolean":
		return "UInt8"

	// === Time types ===
	case "timestamp":
		return "Int64" // consumer sends UnixMilli (int64)
	case "date":
		return "Int64" // consumer sends UnixMilli via time.Time (same as timestamp)
	case "time":
		return "Int64" // nanoseconds since midnight

	// === Identity types ===
	case "uuid", "timeuuid":
		return "String" // consumer sends UUID.String()

	// === Binary/network types ===
	case "blob":
		return "String" // consumer sends hex.EncodeToString
	case "inet":
		return "String" // consumer sends net.IP.String()

	// === Arbitrary precision types ===
	case "varint":
		return "String" // consumer sends big.Int.String()
	case "decimal":
		return "String" // consumer sends inf.Dec.String()

	// === Duration ===
	case "duration":
		return "String" // consumer sends fmt.Sprintf
	}

	// === Collection types ===

	// list<T> and frozen<list<T>>
	if strings.HasPrefix(cqlType, "frozen<list<") || strings.HasPrefix(cqlType, "list<") {
		inner := extractInner(cqlType)
		return "Array(" + mapCQLType(inner) + ")"
	}

	// set<T> and frozen<set<T>> — same as list in ClickHouse
	if strings.HasPrefix(cqlType, "frozen<set<") || strings.HasPrefix(cqlType, "set<") {
		inner := extractInner(cqlType)
		return "Array(" + mapCQLType(inner) + ")"
	}

	// map<K,V> and frozen<map<K,V>> → JSON string
	if strings.HasPrefix(cqlType, "frozen<map<") || strings.HasPrefix(cqlType, "map<") {
		return "String" // consumer serializes maps as JSON
	}

	// tuple<...> and frozen<tuple<...>> → JSON string
	if strings.HasPrefix(cqlType, "tuple<") || strings.HasPrefix(cqlType, "frozen<tuple<") {
		return "String" // consumer serializes tuples as JSON
	}

	// frozen<...> catch-all (UDTs, nested frozen) → JSON string
	if strings.HasPrefix(cqlType, "frozen<") {
		return "String" // consumer serializes complex frozen types as JSON
	}

	// Default fallback — unknown types become String (JSON-serialized by consumer)
	return "String"
}

// extractInner extracts the inner element type from collection types.
// Uses bracket-depth counting for correct nested type extraction.
//
// Examples:
//   "frozen<list<text>>"          → "text"
//   "set<int>"                    → "int"
//   "frozen<list<frozen<map<text, text>>>>" → "frozen<map<text, text>>"
//   "list<uuid>"                  → "uuid"
func extractInner(cqlType string) string {
	// Remove frozen< wrapper if present using bracket matching
	s := cqlType
	if strings.HasPrefix(s, "frozen<") {
		s = s[7:] // strip "frozen<"
		// Find matching closing > by counting bracket depth
		if end := matchingBracket(s); end >= 0 {
			s = s[:end]
		}
	}

	// Now extract inner from list<X> or set<X> using bracket matching
	if idx := strings.Index(s, "<"); idx != -1 {
		inner := s[idx+1:]
		// Find the matching closing > for the outer collection bracket
		if end := matchingBracket(inner); end >= 0 {
			inner = inner[:end]
		}
		return strings.TrimSpace(inner)
	}

	return "text" // fallback
}

// matchingBracket finds the position of the closing > that matches the outermost level.
// Returns -1 if no matching bracket found.
func matchingBracket(s string) int {
	depth := 0
	for i, ch := range s {
		switch ch {
		case '<':
			depth++
		case '>':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}