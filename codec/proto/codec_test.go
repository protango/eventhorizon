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
	"bytes"
	"context"
	"testing"
	"time"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/uuid"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventCodec(t *testing.T) {
	// Setup test parameters
	eventType := eh.EventType("CodecEvent")
	ctx := mocks.WithContextOne(context.Background(), "testval")
	id := uuid.MustParse("10a7ec0f-7f2b-46f5-bca1-877b6e33c9fd")
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	metadata := map[string]interface{}{"num": 42.0}
	eventData := TestData{
		Value: "Test data",
	}

	// Register with eh
	eh.RegisterEventData(eventType, func() eh.EventData { return &TestData{} })

	// Init codec
	c := &EventCodec{}

	// find expected bytes
	eventDataBytes, err := proto.Marshal(&eventData)
	if err != nil {
		t.Error("Failed to set up expected bytes:", err)
	}

	ehEvent := Event{
		EventType:     string(eventType),
		Timestamp:     timestamppb.New(timestamp),
		AggregateType: string(mocks.AggregateType),
		AggregateID:   id.String(),
		Version:       1,
		Context:       []byte(`{"context_one":"testval"}`),
		Metadata:      []byte(`{"num":42}`),
		RawData:       eventDataBytes,
	}

	expectedBytes, err := proto.Marshal(&ehEvent)
	if err != nil {
		t.Error("Failed to set up expected bytes:", err)
	}

	// Marshaling.
	event := eh.NewEvent(eventType, &eventData, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 1),
		eh.WithMetadata(metadata), // NOTE: Just one key to avoid comparisson issues.
	)
	b, err := c.MarshalEvent(ctx, event)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !bytes.Equal(b, expectedBytes) {
		t.Error("the encoded bytes should be correct:", b)
	}

	// Unmarshaling.
	decodedEvent, decodedContext, err := c.UnmarshalEvent(context.Background(), b)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if err := eh.CompareEvents(decodedEvent, event, eh.CompareDataAsProto()); err != nil {
		t.Error("the decoded event was incorrect:", err)
	}
	if val, ok := mocks.ContextOne(decodedContext); !ok || val != "testval" {
		t.Error("the decoded context was incorrect:", decodedContext)
	}
}
