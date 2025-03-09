package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type JSONText json.RawMessage

var emptyJSON = JSONText("{}")
var emptyArrayJSON = JSONText("[]")

func (js JSONText) Value() (driver.Value, error) {
	return js.String(), nil
}

func (js *JSONText) Scan(value interface{}) error {
	// if value is nil, false
	if value == nil {
		// set the value of the pointer yne to JSONText(false)
		*js = emptyJSON
		return nil
	}
	var source []byte

	switch t := value.(type) {
	case string:
		source = []byte(t)
	case []byte:
		if len(t) == 0 {
			source = emptyJSON
		} else {
			source = t
		}
	case nil:
		*js = emptyJSON
	default:
		return errors.New("Incompatible type for JSONText")
	}

	*js = JSONText(append((*js)[0:0], source...))
	return nil

}

// MarshalJSON returns the *j as the JSON encoding of j.
func (j JSONText) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return emptyJSON, nil
	}
	return j, nil
}

// UnmarshalJSON sets *j to a copy of data
func (j *JSONText) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("JSONText: UnmarshalJSON on nil pointer")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

// Unmarshal unmarshal's the json in j to v, as in json.Unmarshal.
func (j *JSONText) Unmarshal(v interface{}) error {
	if len(*j) == 0 {
		*j = emptyJSON
	}
	return json.Unmarshal([]byte(*j), v)
}

// String supports pretty printing for JSONText types.
func (j JSONText) String() string {
	return string(j)
}
