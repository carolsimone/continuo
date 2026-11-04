package deadletter

import "strings"

// Dedup keys identify a dead letter across redeliveries: one key per source
// message, so storing the same dead letter twice keeps the first row.

// ConsumerKey identifies a message a consumer group dead-lettered. messageID is
// the message's dedup identity: its entry ID, qualified by the Redis key it was
// read from for any key but the stream's legacy one.
func ConsumerKey(stream, group, messageID string) string {
	return strings.Join([]string{"consumer", stream, group, messageID}, "|")
}

// OutboxKey identifies an outbox row a relay dead-lettered.
func OutboxKey(table, failedOutboxID string) string {
	return strings.Join([]string{"outbox", table, failedOutboxID}, "|")
}

// QuarantineKey identifies a stream entry the retention cap quarantined for one group.
func QuarantineKey(stream, group, messageID string) string {
	return strings.Join([]string{"quarantine", stream, group, messageID}, "|")
}

// UnreadableKey identifies a dead-letter stream entry that could not be decoded.
// messageID is the entry's dedup identity, as for ConsumerKey.
func UnreadableKey(stream, messageID string) string {
	return strings.Join([]string{"unreadable", stream, messageID}, "|")
}
