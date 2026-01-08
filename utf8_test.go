package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{
			name:  "valid utf8",
			input: `{"name":"Alice","age":30}`,
			valid: true,
		},
		{
			name:  "valid utf8 with chinese",
			input: `{"name":"张三","city":"北京"}`,
			valid: true,
		},
		{
			name:  "invalid utf8 - single bad byte",
			input: "hello\xffworld",
			valid: false,
		},
		{
			name:  "invalid utf8 - multiple bad bytes",
			input: "test\xc3\x28data",
			valid: false,
		},
		{
			name:  "invalid utf8 in json",
			input: "{\"name\":\"test\xffvalue\"}",
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeUTF8(tt.input)

			// Result must be valid UTF-8
			if !utf8.ValidString(result) {
				t.Errorf("sanitizeUTF8() returned invalid UTF-8 string")
			}

			// If input was valid, output should be same
			if tt.valid && result != tt.input {
				t.Errorf("sanitizeUTF8() changed valid UTF-8 string: got %q, want %q", result, tt.input)
			}

			// If input was invalid, output should be different
			if !tt.valid && result == tt.input {
				t.Errorf("sanitizeUTF8() did not change invalid UTF-8 string: %q", tt.input)
			}
		})
	}
}

func TestReWriteTheJsonDataWithInvalidUTF8(t *testing.T) {
	// Simulate the real-world scenario from the error message
	// The error occurred at "pos 1328 of size 2036"
	// This suggests a relatively large JSON with invalid UTF-8 somewhere in the middle

	// Create a JSON string with invalid UTF-8 in the middle
	invalidJSON := `{"appid":"test123","data":[{"name":"user` + "\xff\xfe" + `","value":"test"}]}`

	// Verify the input has invalid UTF-8
	if utf8.ValidString(invalidJSON) {
		t.Fatal("Test input should contain invalid UTF-8")
	}

	// Sanitize it
	sanitized := sanitizeUTF8(invalidJSON)

	// Verify output is valid UTF-8
	if !utf8.ValidString(sanitized) {
		t.Error("Sanitized output is not valid UTF-8")
	}

	t.Logf("Original length: %d, Sanitized length: %d", len(invalidJSON), len(sanitized))
	t.Logf("Sanitized output: %s", sanitized)
}

func TestJSONMarshalWithSanitizedUTF8(t *testing.T) {
	// Test that sanitized data can be successfully marshaled to JSON
	// This simulates what happens in reWriteTheJsonData

	type testData struct {
		UUID    string          `json:"uuid"`
		RawData json.RawMessage `json:"raw_data"`
	}

	// Create data with invalid UTF-8
	invalidData := `{"key":"value` + "\xff" + `"}`

	// Sanitize it
	raw, err := prepareRawData(invalidData, 10)
	if err != nil {
		t.Fatalf("prepareRawData failed: %v", err)
	}

	// Try to marshal into a struct
	record := testData{
		UUID:    "test-uuid",
		RawData: raw,
	}

	jsonBytes, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("Failed to marshal sanitized data: %v", err)
	}

	// Verify the result is valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	t.Logf("Successfully marshaled: %s", string(jsonBytes))
}

func TestSanitizeUTF8Performance(t *testing.T) {
	// Test with a large string similar to the production size (2036 bytes)
	var sb strings.Builder
	sb.WriteString(`{"large":"`)

	// Add valid data
	for i := 0; i < 1800; i++ {
		sb.WriteByte('a')
	}

	// Add some invalid bytes in the middle (similar to pos 1328 in the error)
	sb.WriteString("\xff\xfe\xfd")

	// Add more valid data
	for i := 0; i < 200; i++ {
		sb.WriteByte('b')
	}
	sb.WriteString(`"}`)

	largeInvalidString := sb.String()

	// Verify input has invalid UTF-8
	if utf8.ValidString(largeInvalidString) {
		t.Fatal("Test input should contain invalid UTF-8")
	}

	// Sanitize
	result := sanitizeUTF8(largeInvalidString)

	// Verify result is valid UTF-8
	if !utf8.ValidString(result) {
		t.Error("Sanitized large string is not valid UTF-8")
	}

	t.Logf("Input size: %d, Output size: %d", len(largeInvalidString), len(result))
}

func TestRealWorldKafkaMessage(t *testing.T) {
	// Test with a message similar to the one that caused the original error
	// Based on the Kafka message structure provided
	kafkaMsg := `{"appid":"bfdc640a329d4ca999649adf834ee842","client_ip":"208.107.120.175","data_object":{"#app_id":"bfdc640a329d4ca999649adf834ee842","data":[{"#event_name":"ta_app_start","properties":{"#lib":"iOS","device_name":"iPhone` + "\xff" + `"}}]}}`

	// Verify it has invalid UTF-8
	if utf8.ValidString(kafkaMsg) {
		t.Skip("Test message is valid UTF-8, skipping")
	}

	// Sanitize
	sanitized := sanitizeUTF8(kafkaMsg)

	// Should be valid UTF-8
	if !utf8.ValidString(sanitized) {
		t.Error("Sanitized Kafka message is not valid UTF-8")
	}

	// Should still be parseable as JSON (structure preserved, just invalid bytes removed)
	// Note: The JSON might not be valid anymore if the invalid bytes were in critical positions,
	// but the UTF-8 should be valid
	t.Logf("Sanitized Kafka message: %s", sanitized)
}

func TestPrepareRawDataInvalidJSON(t *testing.T) {
	_, err := prepareRawData("not-json", 5)
	if err == nil {
		t.Fatal("expected error for invalid JSON payload")
	}
}

func TestSanitizeKafkaKey(t *testing.T) {
	rawKey := string([]byte{0xff, 0xfe, 0xfd})
	sanitized, encoding := sanitizeKafkaKey(rawKey)
	if encoding != "base64" {
		t.Fatalf("expected base64 encoding, got %s", encoding)
	}
	expected := base64.StdEncoding.EncodeToString([]byte(rawKey))
	if sanitized != expected {
		t.Fatalf("base64 value mismatch: got %s, want %s", sanitized, expected)
	}

	plain, encoding := sanitizeKafkaKey("valid-key")
	if encoding != "" || plain != "valid-key" {
		t.Fatalf("unexpected sanitization result: %s (%s)", plain, encoding)
	}
}

func TestBuildNDJSONRecord(t *testing.T) {
	cfg := &config.Config{KafkaTopic: "test_topic"}
	ig := &databendIngester{databendIngesterCfg: cfg}

	msg := message.MessageData{
		Data:       `{"foo":"bar"}`,
		DataOffset: 42,
		Partition:  1,
		Key:        string([]byte{0xff, 0x01}),
		CreateTime: time.Unix(0, 0),
	}

	record, err := ig.buildNDJSONRecord(msg)
	if err != nil {
		t.Fatalf("buildNDJSONRecord failed: %v", err)
	}

	if record.RecordMetadata.KeyEncoding != "base64" {
		t.Fatalf("expected key encoding metadata")
	}
	if string(record.RawData) != `{"foo":"bar"}` {
		t.Fatalf("unexpected raw data: %s", string(record.RawData))
	}
}
