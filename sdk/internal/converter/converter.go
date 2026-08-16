// Package converter defines the pluggable DataConverter interface and ships
// a default JSON implementation. Made pluggable from day one (rather than
// bolted on later) because production workflow histories are serialized
// with whatever converter was active at the time — changing the wire
// format after the fact is far more expensive than designing for it now.
package converter

import (
	"encoding/json"
	"fmt"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
)

// DataConverter marshals Go values to/from the Payload envelope used on the
// wire and in history. metadata's "encoding" key records which converter
// produced a given Payload.
type DataConverter interface {
	ToPayload(v any) (*flowv1.Payload, error)
	FromPayload(p *flowv1.Payload, valuePtr any) error
}

type jsonConverter struct{}

// Default is the JSON DataConverter used unless a worker/client is
// configured with another implementation.
var Default DataConverter = jsonConverter{}

func (jsonConverter) ToPayload(v any) (*flowv1.Payload, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("converter: marshal: %w", err)
	}
	return &flowv1.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     data,
	}, nil
}

func (jsonConverter) FromPayload(p *flowv1.Payload, valuePtr any) error {
	if p == nil || len(p.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(p.Data, valuePtr); err != nil {
		return fmt.Errorf("converter: unmarshal: %w", err)
	}
	return nil
}
