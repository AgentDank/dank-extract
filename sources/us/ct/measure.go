// Copyright 2025 Neomantra Corp

package ct

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

///////////////////////////////////////////////////////////////////////////////

// IsTraceMeasurement returns true if the string is considered a trace measure
// Examples are: "TRC" "<LOQ" and "<0.1"
func IsTraceMeasurement(str string) bool {
	if str == "TRC" || strings.Contains(str, "LOQ") ||
		strings.HasPrefix(str, "<") {
		return true
	}
	return false
}

// IsEmptyMeasurement returns true if the string is considered an empty measure
// Examples are: "" and "."
func IsEmptyMeasurement(str string) bool {
	if str == "" || str == "." || str == "-" ||
		strings.HasPrefix(str, "--") {
		return true
	}
	return false
}

// IsErrorMeasurement returns true if the string is considered to be erroneous
func IsErrorMeasurement(str string) bool {
	// One entry had two decimal points (1.1.)... We skip those
	if strings.Count(str, ".") > 1 {
		return true
	}

	// Another error is commas and weird quotes
	if strings.ContainsAny(str, ",`/()") {
		return true
	}

	// Other specific ones have letters in the beginning
	if len(str) > 0 && isLetter(str[0]) {
		return true
	}
	if str == "0<0.10" || strings.HasPrefix(str, "terpinolene: 1.22") || strings.HasPrefix(str, "a-Ocimene: 1.08") {
		return true
	}

	// All good
	return false
}

// isLetter returns true if c is a letter (a-z, A-Z)
func isLetter(c byte) bool {
	r := rune(c)
	return ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')
}

///////////////////////////////////////////////////////////////////////////////

var (
	measureEmptySentinel = 0.0          // Sentinel value for Empty, which is nil-value
	measureZeroSentinel  = math.NaN()   // Sentinel value for Zero, must use IsNan because NaN != NaN
	measureTraceSentinel = math.Inf(-1) // Sentinel value for Trace
)

func measureSentinelize(amount float64) float64 {
	if amount == 0 {
		return measureZeroSentinel
	} else if amount < 0 {
		return measureTraceSentinel
	}
	return amount
}

///////////////////////////////////////////////////////////////////////////////

// Measure tracks a measurement, with special flags for no-measurement and trace measurement
type Measure struct {
	amount float64 // amount is the amount of the measure, or sentinel values
}

// NewMeasure creates a new measure with the given amount.
// Any amount < 0, will be treated as a trace measurement.
// To create an "empty" Measure object, use nil-initialization Measure{} or NewEmptyMeasure
func NewMeasure(amount float64) Measure {
	return Measure{amount: measureSentinelize(amount)}
}

// NewEmptyMeasure creates a new "empty" measure.
// This may also be created through nil-initialization Measure{}
func NewEmptyMeasure() Measure {
	return Measure{amount: measureEmptySentinel}
}

// NewTraceMeasure creates a new "trace amount" measure.
func NewTraceMeasure() Measure {
	return Measure{amount: measureTraceSentinel}
}

// IsEmpty returns true if the measure is empty (no measurement)
func (m Measure) IsEmpty() bool {
	return m.amount == measureEmptySentinel
}

// IsTrace returns true if the measure is a trace amount
func (m Measure) IsTrace() bool {
	return math.IsInf(m.amount, -1)
}

// IsZero returns true if the measure has an initialized, but zero value
func (m Measure) IsZero() bool {
	return math.IsNaN(m.amount)
}

// Amount returns the value, isTrace, isEmpty
func (m Measure) Amount() (result float64, trace bool, empty bool) {
	if m.IsEmpty() {
		return 0, false, true
	} else if m.IsZero() {
		return 0, false, false
	} else if m.IsTrace() {
		return 0, true, false
	} else {
		return m.amount, false, false
	}
}

// IsValidPercent returns true if the measure is a valid percentage (0-100)
func (m Measure) IsValidPercent() bool {
	if m.IsZero() || m.IsEmpty() || m.IsTrace() {
		return true // these are valid values
	}
	return m.amount >= 0 && m.amount <= 100
}

///////////////////////////////////////////////////////////////////////////////

// FromString modifies the given measure based on the passed string
func (m *Measure) FromString(str string) error {
	if IsEmptyMeasurement(str) {
		m.amount = measureEmptySentinel
		return nil
	}
	if IsErrorMeasurement(str) {
		m.amount = measureEmptySentinel
		return nil
	}
	if IsTraceMeasurement(str) {
		m.amount = measureTraceSentinel
		return nil
	}

	// Strip leading comma
	str = strings.TrimPrefix(str, ",")
	// Strip leading >
	str = strings.TrimPrefix(str, ">")
	// Remove percentages
	str = strings.TrimSuffix(str, "%")

	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return err
	}

	m.amount = measureSentinelize(val)
	return nil
}

// AsSQL converts the measure to "NULL" or "<amount>".
func (m Measure) AsSQL() string {
	if m.IsEmpty() || m.IsTrace() {
		return "NULL"
	}
	if m.IsZero() {
		return "0"
	}
	return fmt.Sprintf("%f", m.amount)
}

// AsCSV converts the measure to "" or "<amount>".
func (m Measure) AsCSV() string {
	if m.IsEmpty() || m.IsTrace() {
		return ""
	}
	if m.IsZero() {
		return "0"
	}
	return fmt.Sprintf("%f", m.amount)
}

///////////////////////////////////////////////////////////////////////////////
// Marshalling

// MarshalJSON converts the measure to JSON
func (m *Measure) MarshalJSON() ([]byte, error) {
	if m.IsEmpty() {
		return []byte("null"), nil
	} else if m.IsZero() {
		return []byte("0"), nil
	} else if m.IsTrace() {
		return []byte(`"<0.01"`), nil
	} else {
		return []byte(fmt.Sprintf("%f", m.amount)), nil
	}
}

// UnmarshalJSON converts the measure from JSON
func (m *Measure) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		m.amount = measureEmptySentinel
		return nil
	}

	var str string
	var err error
	if err = json.Unmarshal(b, &str); err == nil {
		return m.FromString(str)
	}

	var val float64
	if err := json.Unmarshal(b, &val); err == nil {
		m.amount = measureSentinelize(val)
		return nil
	}
	return fmt.Errorf("failed to unmarshal measure: %w", err)
}

// Value implements the driver.Valuer interface for inserting into SQL
func (m Measure) Value() (driver.Value, error) {
	if m.IsTrace() || m.IsEmpty() {
		return nil, nil
	}
	if m.IsZero() {
		return 0, nil
	}
	return m.amount, nil
}

// UnmarshalCSV unmarshals the measure from a CSV string
func (m *Measure) UnmarshalCSV(value string) error {
	if value == "" {
		m.amount = measureEmptySentinel
		return nil
	}
	return m.FromString(value)
}

// MarshalCSV marshals the measure to a CSV string
func (m Measure) MarshalCSV() (string, error) {
	return m.AsCSV(), nil
}
