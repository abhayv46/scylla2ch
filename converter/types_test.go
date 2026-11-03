package converter

import (
	"testing"
)

// ---------- Scalar type mapping ----------

func TestCQLToClickHouse_Text(t *testing.T) {
	for _, cql := range []string{"text", "varchar", "ascii"} {
		got := CQLToClickHouseType(cql, false)
		if got != "Nullable(String)" {
			t.Errorf("%s: expected Nullable(String), got %s", cql, got)
		}
	}
}

func TestCQLToClickHouse_TextPK(t *testing.T) {
	got := CQLToClickHouseType("text", true)
	if got != "String" {
		t.Errorf("PK text: expected String, got %s", got)
	}
}

func TestCQLToClickHouse_Bigint(t *testing.T) {
	for _, cql := range []string{"bigint", "counter"} {
		got := CQLToClickHouseType(cql, false)
		if got != "Nullable(Int64)" {
			t.Errorf("%s: expected Nullable(Int64), got %s", cql, got)
		}
	}
}

func TestCQLToClickHouse_Int(t *testing.T) {
	got := CQLToClickHouseType("int", false)
	if got != "Nullable(Int32)" {
		t.Errorf("expected Nullable(Int32), got %s", got)
	}
}

func TestCQLToClickHouse_Smallint(t *testing.T) {
	got := CQLToClickHouseType("smallint", false)
	if got != "Nullable(Int16)" {
		t.Errorf("expected Nullable(Int16), got %s", got)
	}
}

func TestCQLToClickHouse_Tinyint(t *testing.T) {
	got := CQLToClickHouseType("tinyint", false)
	if got != "Nullable(Int8)" {
		t.Errorf("expected Nullable(Int8), got %s", got)
	}
}

func TestCQLToClickHouse_Float(t *testing.T) {
	got := CQLToClickHouseType("float", false)
	if got != "Nullable(Float32)" {
		t.Errorf("expected Nullable(Float32), got %s", got)
	}
}

func TestCQLToClickHouse_Double(t *testing.T) {
	got := CQLToClickHouseType("double", false)
	if got != "Nullable(Float64)" {
		t.Errorf("expected Nullable(Float64), got %s", got)
	}
}

func TestCQLToClickHouse_Boolean(t *testing.T) {
	got := CQLToClickHouseType("boolean", false)
	if got != "Nullable(UInt8)" {
		t.Errorf("expected Nullable(UInt8), got %s", got)
	}
}

// ---------- Time types ----------

func TestCQLToClickHouse_Timestamp(t *testing.T) {
	got := CQLToClickHouseType("timestamp", false)
	if got != "Nullable(Int64)" {
		t.Errorf("expected Nullable(Int64), got %s", got)
	}
}

func TestCQLToClickHouse_Date(t *testing.T) {
	got := CQLToClickHouseType("date", false)
	if got != "Nullable(Int64)" {
		t.Errorf("expected Nullable(Int64), got %s", got)
	}
}

func TestCQLToClickHouse_Time(t *testing.T) {
	got := CQLToClickHouseType("time", false)
	if got != "Nullable(Int64)" {
		t.Errorf("expected Nullable(Int64), got %s", got)
	}
}

// ---------- Identity types ----------

func TestCQLToClickHouse_UUID(t *testing.T) {
	for _, cql := range []string{"uuid", "timeuuid"} {
		got := CQLToClickHouseType(cql, false)
		if got != "Nullable(String)" {
			t.Errorf("%s: expected Nullable(String), got %s", cql, got)
		}
	}
}

func TestCQLToClickHouse_UUID_PK(t *testing.T) {
	got := CQLToClickHouseType("uuid", true)
	if got != "String" {
		t.Errorf("expected String (PK), got %s", got)
	}
}

// ---------- Binary/network ----------

func TestCQLToClickHouse_Blob(t *testing.T) {
	got := CQLToClickHouseType("blob", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_Inet(t *testing.T) {
	got := CQLToClickHouseType("inet", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

// ---------- Arbitrary precision ----------

func TestCQLToClickHouse_Varint(t *testing.T) {
	got := CQLToClickHouseType("varint", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_Decimal(t *testing.T) {
	got := CQLToClickHouseType("decimal", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_Duration(t *testing.T) {
	got := CQLToClickHouseType("duration", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

// ---------- Collection types ----------

func TestCQLToClickHouse_ListText(t *testing.T) {
	got := CQLToClickHouseType("list<text>", false)
	if got != "Array(String)" {
		t.Errorf("expected Array(String), got %s", got)
	}
}

func TestCQLToClickHouse_ListInt(t *testing.T) {
	got := CQLToClickHouseType("list<int>", false)
	if got != "Array(Int32)" {
		t.Errorf("expected Array(Int32), got %s", got)
	}
}

func TestCQLToClickHouse_SetUUID(t *testing.T) {
	got := CQLToClickHouseType("set<uuid>", false)
	if got != "Array(String)" {
		t.Errorf("expected Array(String), got %s", got)
	}
}

func TestCQLToClickHouse_FrozenListText(t *testing.T) {
	got := CQLToClickHouseType("frozen<list<text>>", false)
	if got != "Array(String)" {
		t.Errorf("expected Array(String), got %s", got)
	}
}

func TestCQLToClickHouse_FrozenSetInt(t *testing.T) {
	got := CQLToClickHouseType("frozen<set<int>>", false)
	if got != "Array(Int32)" {
		t.Errorf("expected Array(Int32), got %s", got)
	}
}

func TestCQLToClickHouse_MapTextText(t *testing.T) {
	got := CQLToClickHouseType("map<text, text>", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_FrozenMapTextInt(t *testing.T) {
	got := CQLToClickHouseType("frozen<map<text, int>>", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_TupleTextInt(t *testing.T) {
	got := CQLToClickHouseType("tuple<text, int>", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_FrozenTuple(t *testing.T) {
	got := CQLToClickHouseType("frozen<tuple<text, int>>", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_FrozenUDT(t *testing.T) {
	got := CQLToClickHouseType("frozen<my_udt>", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_UnknownType(t *testing.T) {
	got := CQLToClickHouseType("custom_unknown", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

// ---------- Arrays are never Nullable ----------

func TestCQLToClickHouse_ListNotNullable(t *testing.T) {
	got := CQLToClickHouseType("list<text>", false)
	// Array types are never wrapped in Nullable
	if got != "Array(String)" {
		t.Errorf("expected Array(String) without Nullable, got %s", got)
	}
}

// ---------- Case insensitivity ----------

func TestCQLToClickHouse_CaseInsensitive(t *testing.T) {
	got := CQLToClickHouseType("TEXT", false)
	if got != "Nullable(String)" {
		t.Errorf("expected Nullable(String), got %s", got)
	}
}

func TestCQLToClickHouse_LeadingSpaces(t *testing.T) {
	got := CQLToClickHouseType("  int  ", false)
	if got != "Nullable(Int32)" {
		t.Errorf("expected Nullable(Int32), got %s", got)
	}
}

// ---------- Nested collections ----------

func TestCQLToClickHouse_ListOfFrozenMap(t *testing.T) {
	got := CQLToClickHouseType("list<frozen<map<text, text>>>", false)
	// Inner frozen<map<...>> → String, so Array(String)
	if got != "Array(String)" {
		t.Errorf("expected Array(String), got %s", got)
	}
}

// ---------- extractInner ----------

func TestExtractInner_Simple(t *testing.T) {
	got := extractInner("list<text>")
	if got != "text" {
		t.Errorf("expected text, got %s", got)
	}
}

func TestExtractInner_Frozen(t *testing.T) {
	got := extractInner("frozen<list<int>>")
	if got != "int" {
		t.Errorf("expected int, got %s", got)
	}
}

func TestExtractInner_NestedFrozen(t *testing.T) {
	got := extractInner("frozen<list<frozen<map<text, text>>>>")
	if got != "frozen<map<text, text>>" {
		t.Errorf("expected frozen<map<text, text>>, got %s", got)
	}
}

func TestExtractInner_Set(t *testing.T) {
	got := extractInner("set<uuid>")
	if got != "uuid" {
		t.Errorf("expected uuid, got %s", got)
	}
}

func TestExtractInner_NoAngle(t *testing.T) {
	got := extractInner("text")
	if got != "text" {
		t.Errorf("expected text fallback, got %s", got)
	}
}