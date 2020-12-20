package scalers

import (
	"testing"
)

type mqttMetadataTestData struct {
	metadata    map[string]string
	raisesError bool
}

var testMqttMetadata = []mqttMetadataTestData{
	// No metadata
	{metadata: map[string]string{}, raisesError: true},
	// OK
	{metadata: map[string]string{"host": "tcp://localhost:1883", "topic": "test/topic", "present": "true", "desiredReplicas": "5"}, raisesError: false},
	// Desired replicas not an int
	{metadata: map[string]string{"host": "tcp://localhost:1883", "topic": "test/topic", "present": "true", "desiredReplicas": "five"}, raisesError: true},
	// Present not a boolean
	{metadata: map[string]string{"host": "tcp://localhost:1883", "topic": "test/topic", "present": "i think so", "desiredReplicas": "5"}, raisesError: true},
	// Missing host
	{metadata: map[string]string{"topic": "test/topic", "present": "true", "desiredReplicas": "5"}, raisesError: true},
	// Missing topic
	{metadata: map[string]string{"host": "tcp://localhost:1883", "present": "true", "desiredReplicas": "5"}, raisesError: true},
	// Missing desired replicas
	{metadata: map[string]string{"host": "tcp://localhost:1883", "topic": "test/topic", "present": "true"}, raisesError: true},
	// Missing present
	{metadata: map[string]string{"host": "tcp://localhost:1883", "topic": "test/topic", "desiredReplicas": "5"}, raisesError: false},
}

func TestParseMqttMetadata(t *testing.T) {
	for _, testData := range testMqttMetadata {
		_, err := parseMqttMetadata(&ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: map[string]string{}})
		if err != nil && !testData.raisesError {
			t.Error("Expected success but got error", err)
		}
		if err == nil && testData.raisesError {
			t.Error("Expected error but got success")
		}
	}
}
