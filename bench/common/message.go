package common

import (
	"crypto/rand"
	"fmt"
)

// PreparedMessage holds a pre-generated message ready for publishing.
type PreparedMessage struct {
	Key   []byte
	Value []byte
}

// PreGenerateMessages creates all message batches upfront.
// This allows benchmarks to measure pure ingestion performance
// without including data generation time.
func PreGenerateMessages(numMessages, messageSize, batchSize int) [][]PreparedMessage {
	if batchSize <= 0 {
		batchSize = numMessages
	}

	numBatches := numMessages / batchSize
	remainder := numMessages % batchSize

	// Allocate batches
	totalBatches := numBatches
	if remainder > 0 {
		totalBatches++
	}

	batches := make([][]PreparedMessage, totalBatches)

	// Pre-generate the payload template once
	payload := make([]byte, messageSize)
	rand.Read(payload)

	msgIndex := 0
	for i := 0; i < numBatches; i++ {
		batches[i] = make([]PreparedMessage, batchSize)
		for j := 0; j < batchSize; j++ {
			batches[i][j] = PreparedMessage{
				Key:   []byte(fmt.Sprintf("key-%d", msgIndex)),
				Value: generatePayload(messageSize, payload),
			}
			msgIndex++
		}
	}

	// Handle remainder
	if remainder > 0 {
		batches[numBatches] = make([]PreparedMessage, remainder)
		for j := 0; j < remainder; j++ {
			batches[numBatches][j] = PreparedMessage{
				Key:   []byte(fmt.Sprintf("key-%d", msgIndex)),
				Value: generatePayload(messageSize, payload),
			}
			msgIndex++
		}
	}

	return batches
}

// PreGenerateMessagesFlat creates all messages as a flat slice.
// Useful when batch structure isn't needed.
func PreGenerateMessagesFlat(numMessages, messageSize int) []PreparedMessage {
	messages := make([]PreparedMessage, numMessages)

	// Pre-generate the payload template once
	payload := make([]byte, messageSize)
	rand.Read(payload)

	for i := 0; i < numMessages; i++ {
		messages[i] = PreparedMessage{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: generatePayload(messageSize, payload),
		}
	}

	return messages
}

// generatePayload creates a new payload by copying and slightly modifying the template.
// This is more efficient than calling crypto/rand for every message.
func generatePayload(size int, template []byte) []byte {
	payload := make([]byte, size)
	copy(payload, template)
	// Add some variation by modifying a few bytes
	if size > 8 {
		rand.Read(payload[:8])
	}
	return payload
}

// TotalMessageCount returns the total number of messages across all batches.
func TotalMessageCount(batches [][]PreparedMessage) int {
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	return total
}

// TotalByteSize returns the total bytes across all messages.
func TotalByteSize(batches [][]PreparedMessage) int64 {
	var total int64
	for _, batch := range batches {
		for _, msg := range batch {
			total += int64(len(msg.Value))
		}
	}
	return total
}
