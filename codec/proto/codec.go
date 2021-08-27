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

// EventCodec is a codec for marshaling and unmarshaling events
// to and from bytes in JSON format.
type EventCodec struct{}

// MarshalEvent marshals an event into bytes in JSON format.
func (c *EventCodec) MarshalEvent(ctx context.Context, event eh.Event) ([]byte, error) {
	marshaledMetadata, err := json.Marshal(event.Metadata())
	if err != nil {
		return nil, fmt.Errorf("could not marshal event metadata as JSON: %w", err)
	}

	marshaledContext, err := json.Marshal(eh.MarshalContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("could not marshal event context as JSON: %w", err)
	}

	e := Event{
		EventType:     event.EventType().String(),
		Timestamp:     timestamppb.New(event.Timestamp()),
		AggregateType: event.AggregateType().String(),
		AggregateID:   event.AggregateID().String(),
		Version:       int32(event.Version()),
		Context:       marshaledContext,
		Metadata:      marshaledMetadata,
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
	var unmarshaledMetadata, unmarshaledCtx map[string]interface{}
	if err := json.Unmarshal(e.Metadata, &unmarshaledMetadata); err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal event metadata: %w", err)
	}

	// Unmarshal context
	if err := json.Unmarshal(e.Context, &unmarshaledCtx); err != nil {
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
