package consumer

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

// ---------- Pointer scalar types (main CDC path) ----------

func TestConvertValue_StringPtr(t *testing.T) {
	s := "hello"
	got := ConvertValue("col", &s)
	if got != "hello" {
		t.Fatalf("expected hello, got %v", got)
	}
}

func TestConvertValue_StringPtrNil(t *testing.T) {
	var s *string
	got := ConvertValue("col", s)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_Int64Ptr(t *testing.T) {
	v := int64(42)
	got := ConvertValue("col", &v)
	if got != int64(42) {
		t.Fatalf("expected 42, got %v", got)
	}
}

func TestConvertValue_IntPtr_ConvertsToInt32(t *testing.T) {
	// CQL int → Go *int → must become int32 for ClickHouse Int32
	v := 999
	got := ConvertValue("col", &v)
	if got != int32(999) {
		t.Fatalf("expected int32(999), got %T(%v)", got, got)
	}
}

func TestConvertValue_Int32Ptr(t *testing.T) {
	v := int32(100)
	got := ConvertValue("col", &v)
	if got != int32(100) {
		t.Fatalf("expected int32(100), got %v", got)
	}
}

func TestConvertValue_Int16Ptr(t *testing.T) {
	v := int16(10)
	got := ConvertValue("col", &v)
	if got != int16(10) {
		t.Fatalf("expected int16(10), got %v", got)
	}
}

func TestConvertValue_Int8Ptr(t *testing.T) {
	v := int8(5)
	got := ConvertValue("col", &v)
	if got != int8(5) {
		t.Fatalf("expected int8(5), got %v", got)
	}
}

func TestConvertValue_Float32Ptr(t *testing.T) {
	v := float32(3.14)
	got := ConvertValue("col", &v)
	if got != float32(3.14) {
		t.Fatalf("expected 3.14, got %v", got)
	}
}

func TestConvertValue_Float64Ptr(t *testing.T) {
	v := 2.718
	got := ConvertValue("col", &v)
	if got != 2.718 {
		t.Fatalf("expected 2.718, got %v", got)
	}
}

func TestConvertValue_BoolPtrTrue(t *testing.T) {
	v := true
	got := ConvertValue("col", &v)
	if got != uint8(1) {
		t.Fatalf("expected uint8(1), got %T(%v)", got, got)
	}
}

func TestConvertValue_BoolPtrFalse(t *testing.T) {
	v := false
	got := ConvertValue("col", &v)
	if got != uint8(0) {
		t.Fatalf("expected uint8(0), got %T(%v)", got, got)
	}
}

// ---------- Time types ----------

func TestConvertValue_TimePtr(t *testing.T) {
	ts := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	got := ConvertValue("col", &ts)
	expected := ts.UnixMilli()
	if got != expected {
		t.Fatalf("expected %d, got %v", expected, got)
	}
}

func TestConvertValue_TimeValue(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := ConvertValue("col", ts)
	expected := ts.UnixMilli()
	if got != expected {
		t.Fatalf("expected %d, got %v", expected, got)
	}
}

func TestConvertValue_UUIDPtr(t *testing.T) {
	u := gocql.TimeUUID()
	got := ConvertValue("col", &u)
	if got != u.String() {
		t.Fatalf("expected %s, got %v", u.String(), got)
	}
}

func TestConvertValue_UUIDValue(t *testing.T) {
	u := gocql.TimeUUID()
	got := ConvertValue("col", u)
	if got != u.String() {
		t.Fatalf("expected %s, got %v", u.String(), got)
	}
}

// ---------- Binary/network types ----------

func TestConvertValue_Blob(t *testing.T) {
	b := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	got := ConvertValue("col", b)
	expected := hex.EncodeToString(b)
	if got != expected {
		t.Fatalf("expected %s, got %v", expected, got)
	}
}

func TestConvertValue_NetIPPtr(t *testing.T) {
	ip := net.ParseIP("10.0.0.1")
	got := ConvertValue("col", &ip)
	if got != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %v", got)
	}
}

func TestConvertValue_NetIPValue(t *testing.T) {
	ip := net.ParseIP("::1")
	got := ConvertValue("col", ip)
	if got != "::1" {
		t.Fatalf("expected ::1, got %v", got)
	}
}

// ---------- Arbitrary precision ----------

func TestConvertValue_BigInt(t *testing.T) {
	v := big.NewInt(123456789012345)
	got := ConvertValue("col", v)
	if got != "123456789012345" {
		t.Fatalf("expected 123456789012345, got %v", got)
	}
}

func TestConvertValue_InfDec(t *testing.T) {
	v := inf.NewDec(12345, 2) // 123.45
	got := ConvertValue("col", v)
	if got != v.String() {
		t.Fatalf("expected %s, got %v", v.String(), got)
	}
}

// ---------- Duration ----------

func TestConvertValue_DurationPtr(t *testing.T) {
	d := gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3000000000}
	got := ConvertValue("col", &d)
	if got == nil {
		t.Fatal("expected non-nil")
	}
}

func TestConvertValue_DurationValue(t *testing.T) {
	d := gocql.Duration{Months: 0, Days: 5, Nanoseconds: 0}
	got := ConvertValue("col", d)
	if got == nil {
		t.Fatal("expected non-nil")
	}
}

// ---------- Collection types ----------

func TestConvertValue_StringSlice(t *testing.T) {
	v := []string{"a", "b", "c"}
	got := ConvertValue("col", v).([]string)
	if len(got) != 3 || got[0] != "a" {
		t.Fatalf("expected [a b c], got %v", got)
	}
}

func TestConvertValue_IntSlice_ConvertsToInt32(t *testing.T) {
	v := []int{1, 2, 3}
	got := ConvertValue("col", v).([]int32)
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("expected [1 2 3] int32, got %v", got)
	}
}

func TestConvertValue_Int64Slice(t *testing.T) {
	v := []int64{100, 200}
	got := ConvertValue("col", v).([]int64)
	if len(got) != 2 {
		t.Fatalf("expected len 2, got %d", len(got))
	}
}

func TestConvertValue_BoolSlice_ConvertsToUInt8(t *testing.T) {
	v := []bool{true, false, true}
	got := ConvertValue("col", v).([]uint8)
	if got[0] != 1 || got[1] != 0 || got[2] != 1 {
		t.Fatalf("expected [1 0 1], got %v", got)
	}
}

func TestConvertValue_UUIDSlice(t *testing.T) {
	u1, u2 := gocql.TimeUUID(), gocql.TimeUUID()
	v := []gocql.UUID{u1, u2}
	got := ConvertValue("col", v).([]string)
	if got[0] != u1.String() || got[1] != u2.String() {
		t.Fatalf("expected UUID strings, got %v", got)
	}
}

func TestConvertValue_TimeSlice(t *testing.T) {
	t1 := time.Now()
	t2 := t1.Add(time.Hour)
	v := []time.Time{t1, t2}
	got := ConvertValue("col", v).([]int64)
	if got[0] != t1.UnixMilli() {
		t.Fatalf("expected UnixMilli, got %v", got)
	}
}

func TestConvertValue_IPSlice(t *testing.T) {
	v := []net.IP{net.ParseIP("1.2.3.4"), net.ParseIP("5.6.7.8")}
	got := ConvertValue("col", v).([]string)
	if got[0] != "1.2.3.4" {
		t.Fatalf("expected 1.2.3.4, got %v", got)
	}
}

func TestConvertValue_BlobSlice(t *testing.T) {
	v := [][]byte{{0x01, 0x02}, {0x03}}
	got := ConvertValue("col", v).([]string)
	if got[0] != "0102" || got[1] != "03" {
		t.Fatalf("expected hex strings, got %v", got)
	}
}

// ---------- Map types → JSON ----------

func TestConvertValue_MapStringString(t *testing.T) {
	v := map[string]string{"key": "val"}
	got := ConvertValue("col", v).(string)
	if got != `{"key":"val"}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_MapStringInterface(t *testing.T) {
	v := map[string]interface{}{"count": 42}
	got := ConvertValue("col", v).(string)
	if got == "" {
		t.Fatal("expected JSON string")
	}
}

func TestConvertValue_MapStringInt64(t *testing.T) {
	v := map[string]int64{"a": 1}
	got := ConvertValue("col", v).(string)
	if got != `{"a":1}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_MapIntString(t *testing.T) {
	v := map[int]string{1: "one"}
	got := ConvertValue("col", v).(string)
	if got == "" {
		t.Fatal("expected JSON string")
	}
}

// ---------- Tuple → JSON ----------

func TestConvertValue_Tuple(t *testing.T) {
	v := []interface{}{"hello", 42, true}
	got := ConvertValue("col", v).(string)
	if got != `["hello",42,true]` {
		t.Fatalf("expected JSON array, got %v", got)
	}
}

// ---------- Direct value types (non-pointer) ----------

func TestConvertValue_DirectString(t *testing.T) {
	got := ConvertValue("col", "direct")
	if got != "direct" {
		t.Fatalf("expected direct, got %v", got)
	}
}

func TestConvertValue_DirectInt(t *testing.T) {
	got := ConvertValue("col", 42)
	if got != int32(42) {
		t.Fatalf("expected int32(42), got %T(%v)", got, got)
	}
}

func TestConvertValue_DirectBool(t *testing.T) {
	got := ConvertValue("col", true)
	if got != uint8(1) {
		t.Fatalf("expected uint8(1), got %T(%v)", got, got)
	}
}

// ---------- Reflect fallback ----------

func TestConvertValue_UnknownStruct_FallsBackToJSON(t *testing.T) {
	type Custom struct {
		X int `json:"x"`
	}
	got := ConvertValue("col", Custom{X: 99}).(string)
	if got != `{"x":99}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_UnknownPtrDeref(t *testing.T) {
	type Custom struct {
		Y string `json:"y"`
	}
	c := Custom{Y: "test"}
	got := ConvertValue("col", &c).(string)
	if got != `{"y":"test"}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_NilPtrFallback(t *testing.T) {
	var p *int64
	got := ConvertValue("col", p)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

// ---------- Edge cases ----------

func TestConvertValue_EmptyByteSlice(t *testing.T) {
	got := ConvertValue("col", []byte{})
	if got != "" {
		t.Fatalf("expected empty string, got %v", got)
	}
}

func TestConvertValue_EmptyStringSlice(t *testing.T) {
	got := ConvertValue("col", []string{}).([]string)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestConvertValue_Float32Slice(t *testing.T) {
	v := []float32{1.1, 2.2}
	got := ConvertValue("col", v).([]float32)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConvertValue_Float64Slice(t *testing.T) {
	v := []float64{1.1, 2.2}
	got := ConvertValue("col", v).([]float64)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConvertValue_Int16Slice(t *testing.T) {
	v := []int16{1, 2}
	got := ConvertValue("col", v).([]int16)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConvertValue_Int8Slice(t *testing.T) {
	// []int8 should pass through
	v := []int8{1, 2}
	got := ConvertValue("col", v).([]int8)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConvertValue_Int32Slice(t *testing.T) {
	v := []int32{10, 20}
	got := ConvertValue("col", v).([]int32)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestConvertValue_MapStringFloat64(t *testing.T) {
	v := map[string]float64{"pi": 3.14}
	got := ConvertValue("col", v).(string)
	if got != `{"pi":3.14}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_MapStringBool(t *testing.T) {
	v := map[string]bool{"active": true}
	got := ConvertValue("col", v).(string)
	if got != `{"active":true}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_MapStringInt(t *testing.T) {
	v := map[string]int{"count": 5}
	got := ConvertValue("col", v).(string)
	if got != `{"count":5}` {
		t.Fatalf("expected JSON, got %v", got)
	}
}

func TestConvertValue_MapInt64String(t *testing.T) {
	v := map[int64]string{1: "one"}
	got := ConvertValue("col", v).(string)
	if got == "" {
		t.Fatal("expected JSON")
	}
}

// ---------- shortTableName ----------

func TestShortTableName_WithKeyspace(t *testing.T) {
	c := &Consumer{tableName: "chat.rooms", shortName: "rooms"}
	if c.shortTableName() != "rooms" {
		t.Fatalf("expected rooms, got %s", c.shortTableName())
	}
}

func TestShortTableName_NoKeyspace(t *testing.T) {
	c := &Consumer{tableName: "rooms", shortName: "rooms"}
	if c.shortTableName() != "rooms" {
		t.Fatalf("expected rooms, got %s", c.shortTableName())
	}
}

func TestShortTableName_MultipleDots(t *testing.T) {
	c := &Consumer{tableName: "a.b.c", shortName: "b.c"}
	if c.shortTableName() != "b.c" {
		t.Fatalf("expected b.c, got %s", c.shortTableName())
	}
}

// ---------- toJSON helper ----------

func TestToJSON_ValidMap(t *testing.T) {
	got := toJSON(map[string]int{"a": 1})
	if got != `{"a":1}` {
		t.Fatalf("expected JSON, got %s", got)
	}
}

func TestToJSON_EmptyMap(t *testing.T) {
	got := toJSON(map[string]string{})
	if got != `{}` {
		t.Fatalf("expected {}, got %s", got)
	}
}

func TestToJSON_NilSlice(t *testing.T) {
	var s []string
	got := toJSON(s)
	if got != "null" {
		t.Fatalf("expected null, got %s", got)
	}
}

// ---------- Edge cases: overflow / boundary values ----------

func TestConvertValue_IntMaxValue(t *testing.T) {
	v := int(2147483647) // max int32
	got := ConvertValue("col", &v)
	if got != int32(2147483647) {
		t.Fatalf("expected max int32, got %v", got)
	}
}

func TestConvertValue_IntMinValue(t *testing.T) {
	v := int(-2147483648) // min int32
	got := ConvertValue("col", &v)
	if got != int32(-2147483648) {
		t.Fatalf("expected min int32, got %v", got)
	}
}

func TestConvertValue_Int64MaxValue(t *testing.T) {
	v := int64(9223372036854775807)
	got := ConvertValue("col", &v)
	if got != int64(9223372036854775807) {
		t.Fatalf("expected max int64, got %v", got)
	}
}

func TestConvertValue_ZeroTime(t *testing.T) {
	v := time.Time{} // zero time
	got := ConvertValue("col", &v)
	expected := v.UnixMilli()
	if got != expected {
		t.Fatalf("expected %d, got %v", expected, got)
	}
}

func TestConvertValue_BlobNil(t *testing.T) {
	var b []byte // nil
	got := ConvertValue("col", b)
	// nil []byte → guarded by `if v != nil` → returns nil (skip column)
	if got != nil {
		t.Fatalf("expected nil for nil blob, got %v", got)
	}
}

func TestConvertValue_EmptyString(t *testing.T) {
	s := ""
	got := ConvertValue("col", &s)
	if got != "" {
		t.Fatalf("expected empty string, got %v", got)
	}
}

func TestConvertValue_LargeMap(t *testing.T) {
	m := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		m[fmt.Sprintf("key_%d", i)] = i
	}
	got := ConvertValue("col", m).(string)
	if got == "" || got == "{}" {
		t.Fatal("expected non-empty JSON for large map")
	}
}

func TestConvertValue_NestedMap(t *testing.T) {
	// UDT with nested map
	m := map[string]interface{}{
		"outer": map[string]interface{}{
			"inner": "value",
		},
	}
	got := ConvertValue("col", m).(string)
	if got == "" {
		t.Fatal("expected JSON for nested map")
	}
}

func TestConvertValue_IPV6(t *testing.T) {
	ip := net.ParseIP("2001:db8::1")
	got := ConvertValue("col", ip)
	if got != "2001:db8::1" {
		t.Fatalf("expected IPv6 string, got %v", got)
	}
}

func TestConvertValue_BigIntZero(t *testing.T) {
	v := big.NewInt(0)
	got := ConvertValue("col", v)
	if got != "0" {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestConvertValue_BigIntNegative(t *testing.T) {
	v := big.NewInt(-99999999999)
	got := ConvertValue("col", v)
	if got != "-99999999999" {
		t.Fatalf("expected -99999999999, got %v", got)
	}
}

// ---------- Reflect fallback edge cases ----------

func TestConvertValue_UnknownSliceType(t *testing.T) {
	type Custom struct{ V int }
	v := []Custom{{1}, {2}}
	got := ConvertValue("col", v).(string)
	if got == "" {
		t.Fatal("expected JSON for unknown slice type")
	}
}

func TestConvertValue_UnknownMapType(t *testing.T) {
	v := map[float64]string{1.5: "a"}
	got := ConvertValue("col", v).(string)
	if got == "" {
		t.Fatal("expected JSON for unknown map type")
	}
}

func TestConvertValue_PrimitiveIntFallback(t *testing.T) {
	// uint32 is not handled directly — should go to reflect/fmt fallback
	v := uint32(42)
	got := ConvertValue("col", v)
	if got == nil {
		t.Fatal("expected non-nil for uint32")
	}
}

func TestConvertValue_ChannelType_FallsBackToFmtSprintf(t *testing.T) {
	// Channels can't be JSON-marshaled — falls back to fmt.Sprintf
	ch := make(chan int)
	got := ConvertValue("col", ch)
	if got == nil {
		t.Fatal("expected non-nil for channel type")
	}
}

// ---------- Nil interface ----------

func TestConvertValue_NilInterface(t *testing.T) {
	got := ConvertValue("col", nil)
	// nil interface hits reflect fallback → fmt.Sprintf("%v", nil) = "<nil>"
	// In practice, extractValues guards rawVal == nil before calling ConvertValue
	if got != "<nil>" {
		t.Fatalf("expected \"<nil>\" string for nil interface, got %v", got)
	}
}

func TestConvertValue_NilBoolPtr(t *testing.T) {
	var b *bool
	got := ConvertValue("col", b)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilFloat64Ptr(t *testing.T) {
	var f *float64
	got := ConvertValue("col", f)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilTimePtrr(t *testing.T) {
	var t2 *time.Time
	got := ConvertValue("col", t2)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilNetIPPtr(t *testing.T) {
	var ip *net.IP
	got := ConvertValue("col", ip)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilBigIntPtr(t *testing.T) {
	var v *big.Int
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilUUIDPtr(t *testing.T) {
	var u *gocql.UUID
	got := ConvertValue("col", u)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilDurationPtr(t *testing.T) {
	var d *gocql.Duration
	got := ConvertValue("col", d)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilInt64Ptr(t *testing.T) {
	var v *int64
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilIntPtr(t *testing.T) {
	var v *int
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilInt32Ptr(t *testing.T) {
	var v *int32
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilInt16Ptr(t *testing.T) {
	var v *int16
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilInt8Ptr(t *testing.T) {
	var v *int8
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConvertValue_NilFloat32Ptr(t *testing.T) {
	var v *float32
	got := ConvertValue("col", v)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}
