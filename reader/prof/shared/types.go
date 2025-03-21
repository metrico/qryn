package shared

import (
	"errors"
	"strings"
)

var (
	ErrInvalidTypeId = errors.New("invalid type ID format")
)

type TypeId struct {
	Tp         string
	SampleType string
	SampleUnit string
	PeriodType string
	PeriodUnit string
}

func ParseTypeId(strTypeId string) (TypeId, error) {
	parts := strings.SplitN(strTypeId, ":", 5)
	if len(parts) != 5 {
		return TypeId{}, ErrInvalidTypeId
	}
	return TypeId{
		Tp:         parts[0],
		SampleType: parts[1],
		SampleUnit: parts[2],
		PeriodType: parts[3],
		PeriodUnit: parts[4],
	}, nil
}

func ParseShortTypeId(strTypeId string) (TypeId, error) {
	parts := strings.SplitN(strTypeId, ":", 3)
	if len(parts) != 3 {
		return TypeId{}, ErrInvalidTypeId
	}
	return TypeId{
		Tp:         parts[0],
		PeriodType: parts[1],
		PeriodUnit: parts[2],
	}, nil
}
