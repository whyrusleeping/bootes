package ingest

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// Backlink represents a reference found in a record
type Backlink struct {
	Ref     string // DID or AT-URI
	RefType string // "did" or "uri"
	Path    string // JSON path (e.g., "subject", "reply.parent.uri")
}

// CBORResult holds all extracted data from a single CBOR scan
type CBORResult struct {
	JSON      string
	CreatedAt time.Time
	Backlinks []Backlink
}

// Buffer pools for reuse
var (
	scratchPool = sync.Pool{
		New: func() any {
			b := make([]byte, 9)
			return &b
		},
	}
	builderPool = sync.Pool{
		New: func() any {
			b := &bytes.Buffer{}
			b.Grow(4096)
			return b
		},
	}
)

// FastCBORToJSON converts CBOR to JSON and extracts metadata in a single pass.
// Returns JSON string, createdAt timestamp, and any backlinks (DIDs/AT-URIs) found.
// On error, returns empty JSON object with timestamp from rkey.
func FastCBORToJSON(data []byte, rkey string) CBORResult {
	scratchPtr := scratchPool.Get().(*[]byte) //nolint:errcheck // type guaranteed by pool's New func
	scratch := *scratchPtr
	defer scratchPool.Put(scratchPtr)

	sb := builderPool.Get().(*bytes.Buffer) //nolint:errcheck // type guaranteed by pool's New func
	defer func() {
		sb.Reset() // keeps underlying []byte for reuse
		builderPool.Put(sb)
	}()

	if sb.Cap() < len(data)*2 {
		sb.Grow(len(data) * 2)
	}

	r := bytes.NewReader(data)
	cr := cbg.NewCborReader(r)

	var createdAt string
	var backlinks []Backlink

	var strBuf []byte
	err := writeValueAsJSON(cr, scratch, sb, "", &createdAt, &backlinks, strBuf)
	if err != nil {
		// On error, return empty result with timestamp from rkey
		return CBORResult{
			JSON:      "{}",
			CreatedAt: parseCreatedAt("", rkey),
		}
	}

	// Parse createdAt or fall back to TID
	ts := parseCreatedAt(createdAt, rkey)

	// Must copy since we're returning the builder to the pool
	return CBORResult{
		JSON:      sb.String(),
		CreatedAt: ts,
		Backlinks: backlinks,
	}
}

// parseCreatedAt parses createdAt string or falls back to TID-based timestamp
func parseCreatedAt(createdAt, rkey string) time.Time {
	if createdAt != "" {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			return t
		}
		// Try parsing with nanoseconds
		if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			return t
		}
	}

	// Fall back to TID parsing
	if tid, err := syntax.ParseTID(rkey); err == nil {
		return tid.Time()
	}

	return time.Now()
}

// writeValueAsJSON reads one CBOR value and writes it as JSON
func writeValueAsJSON(cr *cbg.CborReader, scratch []byte, sb *bytes.Buffer, path string, createdAt *string, backlinks *[]Backlink, strBuf []byte) error {
	maj, extra, err := cbg.CborReadHeaderBuf(cr, scratch)
	if err != nil {
		return err
	}

	switch maj {
	case cbg.MajUnsignedInt:
		sb.WriteString(strconv.FormatUint(extra, 10))

	case cbg.MajNegativeInt:
		if extra > uint64(math.MaxInt64) {
			// Value is -1 - extra, but that overflows int64.
			// Write as string: -(extra+1), handling the case where extra+1 itself overflows.
			if extra == math.MaxUint64 {
				sb.WriteString("-18446744073709551616")
			} else {
				sb.WriteByte('-')
				sb.WriteString(strconv.FormatUint(extra+1, 10))
			}
		} else {
			sb.WriteString(strconv.FormatInt(-1-int64(extra), 10))
		}

	case cbg.MajTextString:
		var err error
		strBuf, err = readStringToBuffer(cr, int(extra), strBuf)
		if err != nil {
			return err
		}
		writeJSONStringBytes(sb, strBuf)

		// Only allocate a string when we need to keep it
		if path == "createdAt" {
			*createdAt = string(strBuf)
		}
		if isDIDBytes(strBuf) || isATURIBytes(strBuf) {
			str := string(strBuf)
			*backlinks = append(*backlinks, Backlink{
				Ref:     str,
				RefType: classifyRef(str),
				Path:    path,
			})
		}

	case cbg.MajByteString:
		// Emit as {"$bytes": "base64..."}
		byteData, err := readBytes(cr, int(extra))
		if err != nil {
			return err
		}
		sb.WriteString(`{"$bytes":"`)
		sb.WriteString(base64.StdEncoding.EncodeToString(byteData))
		sb.WriteString(`"}`)

	case cbg.MajArray:
		if extra > maxCBORCollectionLength {
			return fmt.Errorf("CBOR array length %d exceeds max %d", extra, maxCBORCollectionLength)
		}
		sb.WriteByte('[')
		for i := uint64(0); i < extra; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			elemPath := path + "[]"
			if err := writeValueAsJSON(cr, scratch, sb, elemPath, createdAt, backlinks, strBuf); err != nil {
				return err
			}
		}
		sb.WriteByte(']')

	case cbg.MajMap:
		if extra > maxCBORCollectionLength {
			return fmt.Errorf("CBOR map length %d exceeds max %d", extra, maxCBORCollectionLength)
		}
		sb.WriteByte('{')
		for i := uint64(0); i < extra; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			// Read key
			key, strBuf, err := readMapKey(cr, scratch, strBuf)
			if err != nil {
				return err
			}
			writeJSONString(sb, key)
			sb.WriteByte(':')
			// Build path for value
			valuePath := key
			if path != "" {
				valuePath = path + "." + key
			}
			// Read value
			if err := writeValueAsJSON(cr, scratch, sb, valuePath, createdAt, backlinks, strBuf); err != nil {
				return err
			}
		}
		sb.WriteByte('}')

	case cbg.MajTag:
		if extra == 42 {
			// CID tag - emit as {"$link": "cid"}
			if err := writeCIDAsJSON(cr, scratch, sb); err != nil {
				return err
			}
		} else {
			// Other tag - just write the content
			if err := writeValueAsJSON(cr, scratch, sb, path, createdAt, backlinks, strBuf); err != nil {
				return err
			}
		}

	case cbg.MajOther:
		if err := writeOtherAsJSON(extra, cr, sb); err != nil {
			return err
		}
	}
	return nil
}

// readStringToBuffer reads a CBOR string directly into a reusable byte slice,
// growing it as needed. Returns the sub-slice containing the string data.
// The returned slice is only valid until the next call.
// maxCBORStringLength is a sanity limit for CBOR string lengths to catch corrupted data
const maxCBORStringLength = 10 * 1024 * 1024 // 10MB
const maxCBORCollectionLength = 1_000_000    // max array/map entries

// Pre-allocated prefixes to avoid allocations in hot path
var (
	prefixDIDPLC = []byte("did:plc:")
	prefixDIDWeb = []byte("did:web:")
	prefixATURI  = []byte("at://")
)

func readStringToBuffer(cr *cbg.CborReader, length int, buf []byte) ([]byte, error) {
	if length == 0 {
		return buf[:0], nil
	}
	if length < 0 || length > maxCBORStringLength {
		return nil, fmt.Errorf("invalid CBOR string length: %d", length)
	}
	if cap(buf) < length {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}
	_, err := io.ReadFull(cr, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// readBytes reads bytes of known length from the CBOR reader
func readBytes(cr *cbg.CborReader, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if length < 0 || length > maxCBORStringLength {
		return nil, fmt.Errorf("invalid CBOR bytes length: %d", length)
	}
	buf := make([]byte, length)
	_, err := io.ReadFull(cr, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// readMapKey reads a map key (must be a string). Uses strBuf to avoid allocation.
func readMapKey(cr *cbg.CborReader, scratch []byte, strBuf []byte) (string, []byte, error) {
	maj, extra, err := cbg.CborReadHeaderBuf(cr, scratch)
	if err != nil {
		return "", strBuf, err
	}
	if maj != cbg.MajTextString {
		return "", strBuf, fmt.Errorf("map key must be string, got major type %d", maj)
	}
	strBuf, err = readStringToBuffer(cr, int(extra), strBuf)
	if err != nil {
		return "", strBuf, err
	}
	return string(strBuf), strBuf, nil
}

// writeJSONString writes a properly escaped JSON string
func writeJSONString(sb *bytes.Buffer, s string) {
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(sb, `\u%04x`, r)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
}

// writeJSONStringBytes writes a properly escaped JSON string from a byte slice
func writeJSONStringBytes(sb *bytes.Buffer, s []byte) {
	sb.WriteByte('"')
	for _, b := range s {
		switch b {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			if b < 0x20 {
				fmt.Fprintf(sb, `\u%04x`, b)
			} else {
				sb.WriteByte(b)
			}
		}
	}
	sb.WriteByte('"')
}

// writeCIDAsJSON reads a CID from CBOR tag 42 and writes it as {"$link": "cid"}
func writeCIDAsJSON(cr *cbg.CborReader, scratch []byte, sb *bytes.Buffer) error {
	// Read bytes
	maj, extra, err := cbg.CborReadHeaderBuf(cr, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("CID tag content must be bytes, got major type %d", maj)
	}

	cidBytes, err := readBytes(cr, int(extra))
	if err != nil {
		return err
	}

	// ATProto CID links have a leading 0x00 byte that must be stripped
	if len(cidBytes) > 1 && cidBytes[0] == 0x00 {
		cidBytes = cidBytes[1:]
	}

	c, err := cid.Cast(cidBytes)
	if err != nil {
		return fmt.Errorf("casting CID bytes: %w", err)
	}

	sb.WriteString(`{"$link":"`)
	sb.WriteString(c.String())
	sb.WriteString(`"}`)
	return nil
}

// writeOtherAsJSON handles CBOR major type 7 (simple values and floats)
func writeOtherAsJSON(extra uint64, cr *cbg.CborReader, sb *bytes.Buffer) error {
	switch extra {
	case 20: // false
		sb.WriteString("false")
	case 21: // true
		sb.WriteString("true")
	case 22, 23: // null, undefined
		sb.WriteString("null")
	case 25: // float16
		buf := make([]byte, 2)
		if _, err := io.ReadFull(cr, buf); err != nil {
			return err
		}
		f := float16ToFloat64(buf)
		writeJSONFloat(sb, f)
	case 26: // float32
		buf := make([]byte, 4)
		if _, err := io.ReadFull(cr, buf); err != nil {
			return err
		}
		bits := uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
		f := float64(math.Float32frombits(bits))
		writeJSONFloat(sb, f)
	case 27: // float64
		buf := make([]byte, 8)
		if _, err := io.ReadFull(cr, buf); err != nil {
			return err
		}
		bits := uint64(buf[0])<<56 | uint64(buf[1])<<48 | uint64(buf[2])<<40 | uint64(buf[3])<<32 |
			uint64(buf[4])<<24 | uint64(buf[5])<<16 | uint64(buf[6])<<8 | uint64(buf[7])
		f := math.Float64frombits(bits)
		writeJSONFloat(sb, f)
	default:
		// Other simple values - treat as integer
		sb.WriteString(strconv.FormatUint(extra, 10))
	}
	return nil
}

// writeJSONFloat writes a float as valid JSON, converting NaN/Inf to null
func writeJSONFloat(sb *bytes.Buffer, f float64) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		sb.WriteString("null")
	} else {
		sb.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	}
}

// float16ToFloat64 converts IEEE 754 half-precision to float64
func float16ToFloat64(b []byte) float64 {
	bits := uint16(b[0])<<8 | uint16(b[1])
	sign := (bits >> 15) & 1
	exp := (bits >> 10) & 0x1F
	frac := bits & 0x3FF

	var f float64
	switch exp {
	case 0:
		if frac == 0 {
			f = 0
		} else {
			f = float64(frac) / 1024.0 * (1.0 / 16384.0) // subnormal
		}
	case 31:
		if frac == 0 {
			if sign == 1 {
				return math.Inf(-1)
			}
			return math.Inf(1)
		}
		return math.NaN()
	default:
		f = (1.0 + float64(frac)/1024.0)
		for i := 0; i < int(exp)-15; i++ {
			f *= 2
		}
		for i := 0; i < 15-int(exp); i++ {
			f /= 2
		}
	}
	if sign == 1 {
		f = -f
	}
	return f
}

// isDIDBytes checks if a byte slice is a DID
func isDIDBytes(b []byte) bool {
	return bytes.HasPrefix(b, prefixDIDPLC) || bytes.HasPrefix(b, prefixDIDWeb)
}

// isATURIBytes checks if a byte slice is an AT-URI
func isATURIBytes(b []byte) bool {
	return bytes.HasPrefix(b, prefixATURI)
}

// classifyRef returns "uri" for AT-URIs or "did" for DIDs
func classifyRef(s string) string {
	if strings.HasPrefix(s, "at://") {
		return "uri"
	}
	return "did"
}
