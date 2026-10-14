package ids

import (
	"crypto/rand"
	"encoding/hex"
)

// traceID 用 16 字节,spanID 用 8 字节,和业界的惯例一致。
func NewTraceID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func NewSpanID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Short 截前 8 位,界面上显示用,完整的 id 太长了。
func Short(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
