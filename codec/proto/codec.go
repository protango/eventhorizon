// Copyright (c) 2021 - The Event Horizon authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"context"
	"encoding/json"
	"fmt"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func marshalMap(source map[string]interface{}) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for key, element := range source {
		bytes, err := json.Marshal(element)
		if err != nil {
			return nil, fmt.Errorf("could not marshal interface as JSON: %w", err)
		}
		result[key] = bytes
	}

	return result, nil
}

func unmarshalMap(source map[string][]byte) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, element := range source {
		err := json.Unmarshal(element, result[key])
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal bytes as JSON: %w", err)
		}
	}

	return result, nil
}

// EventCodec is a codec for marshaling and unmarshaling events
// to and from bytes in JSON format.
type EventCodec struct{}

// MarshalEvent marshals an event into bytes in JSON format.
func (c *EventCodec) MarshalEvent(ctx context.Context, event eh.Event) ([]byte, error) {
	marshalledMetadata, err := marshalMap(event.Metadata())
	if err != nil {
		return nil, err
	}

	marshaledContext, err := marshalMap(eh.MarshalContext(ctx))
	if err != nil {
		return nil, err
	}

	e := Event{
		EventType:     event.EventType().String(),
		Timestamp:     timestamppb.New(event.Timestamp()),
		AggregateType: event.AggregateType().String(),
		AggregateID:   event.AggregateID().String(),
		Version:       int32(event.Version()),
		Context:       marshaledContext,
		Metadata:      marshalledMetadata,
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error

		eventDataProto, ok := event.Data().(proto.Message)
		if !ok {
			return nil, fmt.Errorf("event data is not a protobuf message")
		}

		e.RawData, err = proto.Marshal(eventDataProto)
		if err != nil {
			return nil, fmt.Errorf("could not marshal event data: %w", err)
		}
	}

	b, err := proto.Marshal(&e)
	if err != nil {
		return nil, fmt.Errorf("could not marshal event: %w", err)
	}

	return b, nil
}

// UnmarshalEvent unmarshals an event from bytes in JSON format.
func (c *EventCodec) UnmarshalEvent(ctx context.Context, b []byte) (eh.Event, context.Context, error) {
	// Decode the raw JSON event data.
	var e Event = Event{}
	if err := proto.Unmarshal(b, &e); err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal event: %w", err)
	}

	// Create an event of the correct type and decode from proto.
	var protoEventData proto.Message = nil
	if e.RawData != nil {
		ehEventData, err := eh.CreateEventData(eh.EventType(e.EventType))
		if err != nil {
			return nil, nil, fmt.Errorf("could not create event data: %w", err)
		}

		var ok bool
		if protoEventData, ok = ehEventData.(proto.Message); !ok {
			return nil, nil, fmt.Errorf("registered event data factory did not return a proto message")
		}

		if err := proto.Unmarshal(e.RawData, protoEventData); err != nil {
			return nil, nil, fmt.Errorf("could not unmarshal event data: %w", err)
		}
		e.RawData = nil
	}

	// Unmarshal metadata
	unmarshaledMetadata, err := unmarshalMap(e.Metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal event metadata: %w", err)
	}

	// Unmarshal context
	unmarshaledCtx, err := unmarshalMap(e.Context)
	if err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal event context: %w", err)
	}

	// Build the event.
	aggregateID, err := uuid.Parse(e.AggregateID)
	if err != nil {
		aggregateID = uuid.Nil
	}
	event := eh.NewEvent(
		eh.EventType(e.EventType),
		protoEventData,
		e.Timestamp.AsTime(),
		eh.ForAggregate(
			eh.AggregateType(e.AggregateType),
			aggregateID,
			int(e.Version),
		),
		eh.WithMetadata(unmarshaledMetadata),
	)

	// Unmarshal the context.
	ctx = eh.UnmarshalContext(ctx, unmarshaledCtx)

	return event, ctx, nil
}
