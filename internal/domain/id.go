package domain

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// NewID returns a UUIDv7 (time ordered) in lower-case canonical form.
func NewID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	if _, err := rand.Read(b[6:]); err != nil {
		// Not a business rejection: without entropy the process cannot continue safely.
		panic("crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// ParseID validates a UUID and returns its canonical lower-case form.
func ParseID(s string) (string, error) {
	if !uuidRe.MatchString(s) {
		return "", fmt.Errorf("%w: %q is not a UUID", ErrInvalidEntity, s)
	}
	return strings.ToLower(s), nil
}
