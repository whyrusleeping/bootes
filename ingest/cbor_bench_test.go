package ingest

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

// createTestPost creates a realistic ATProto post with embeds and facets
func createTestPost() *bsky.FeedPost {
	return &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "Collecting Canadian software alternatives:\n\ncdox.ca - Document writing and sharing, Canadian-hosted\nfloatfinancial.com - Corporate cards, expense management\nvenn.ca - Multi-currency accounts, low FX rates",
		CreatedAt:     "2026-01-27T01:30:09.265920Z",
		Langs:         []string{"en"},
		Facets: []*bsky.RichtextFacet{
			{
				Index: &bsky.RichtextFacet_ByteSlice{
					ByteStart: 50,
					ByteEnd:   57,
				},
				Features: []*bsky.RichtextFacet_Features_Elem{
					{
						RichtextFacet_Link: &bsky.RichtextFacet_Link{
							LexiconTypeID: "app.bsky.richtext.facet#link",
							Uri:           "https://cdox.ca",
						},
					},
				},
			},
			{
				Index: &bsky.RichtextFacet_ByteSlice{
					ByteStart: 114,
					ByteEnd:   132,
				},
				Features: []*bsky.RichtextFacet_Features_Elem{
					{
						RichtextFacet_Link: &bsky.RichtextFacet_Link{
							LexiconTypeID: "app.bsky.richtext.facet#link",
							Uri:           "https://floatfinancial.com",
						},
					},
				},
			},
			{
				Index: &bsky.RichtextFacet_ByteSlice{
					ByteStart: 178,
					ByteEnd:   185,
				},
				Features: []*bsky.RichtextFacet_Features_Elem{
					{
						RichtextFacet_Link: &bsky.RichtextFacet_Link{
							LexiconTypeID: "app.bsky.richtext.facet#link",
							Uri:           "https://venn.ca",
						},
					},
				},
			},
		},
	}
}

var testCBORData []byte

func init() {
	post := createTestPost()
	var buf bytes.Buffer
	if err := post.MarshalCBOR(&buf); err != nil {
		panic(err)
	}
	testCBORData = buf.Bytes()
}

// createTestPostWithCID creates a post with an embedded image containing a CID reference
func createTestPostWithCID() (*bsky.FeedPost, cid.Cid) {
	c, _ := cid.Decode("bafkreiewtju6rkfj426g6lnemjzxdcyxm55g5hmlz4x5llb43j6rapkvti")

	return &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "Test post with image",
		CreatedAt:     "2026-01-27T01:30:09.265920Z",
		Embed: &bsky.FeedPost_Embed{
			EmbedImages: &bsky.EmbedImages{
				LexiconTypeID: "app.bsky.embed.images",
				Images: []*bsky.EmbedImages_Image{
					{
						Alt: "test image",
						Image: &lexutil.LexBlob{
							Ref:      lexutil.LexLink(c),
							MimeType: "image/jpeg",
							Size:     12345,
						},
					},
				},
			},
		},
	}, c
}

// createTestFollow creates a follow record that references a DID
func createTestFollow() *bsky.GraphFollow {
	return &bsky.GraphFollow{
		LexiconTypeID: "app.bsky.graph.follow",
		Subject:       "did:plc:z72i7hdynmk6r22z27h6tvur",
		CreatedAt:     "2026-01-27T01:30:09.265920Z",
	}
}

// ============== Correctness Tests ==============

func TestCIDHandling(t *testing.T) {
	t.Parallel()
	post, expectedCID := createTestPostWithCID()

	var buf bytes.Buffer
	err := post.MarshalCBOR(&buf)
	require.NoError(t, err, "marshal cbor")
	cborData := buf.Bytes()
	t.Logf("CBOR size: %d bytes", len(cborData))

	// Test fast path
	result := FastCBORToJSON(cborData, "testrkey")
	t.Logf("Fast JSON:\n%s", result.JSON)

	// Verify CID is present in fast output
	var fastMap map[string]any
	err = json.Unmarshal([]byte(result.JSON), &fastMap)
	require.NoError(t, err, "parse fast json")
	fastEmbed := fastMap["embed"].(map[string]any)
	fastImages := fastEmbed["images"].([]any)
	fastImg := fastImages[0].(map[string]any)
	fastImage := fastImg["image"].(map[string]any)
	fastRef := fastImage["ref"].(map[string]any)
	fastLink, ok := fastRef["$link"].(string)
	require.True(t, ok, "no $link in fast result, got: %v", fastRef)
	require.Equal(t, expectedCID.String(), fastLink, "fast CID mismatch")
}

func TestCorrectnessCreatedAtExtraction(t *testing.T) {
	t.Parallel()
	expected := time.Date(2026, 1, 27, 1, 30, 9, 265920000, time.UTC)

	// Test fast extraction
	result := FastCBORToJSON(testCBORData, "3mdemcarbmb26")
	require.True(t, result.CreatedAt.Equal(expected), "createdAt: got %v, want %v", result.CreatedAt, expected)
}

func TestCorrectnessJSONOutput(t *testing.T) {
	t.Parallel()
	result := FastCBORToJSON(testCBORData, "3mdemcarbmb26")

	// Parse JSON to verify structure
	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "parse json")

	// Check key fields exist
	require.Equal(t, "app.bsky.feed.post", m["$type"], "$type mismatch")
	require.Equal(t, "2026-01-27T01:30:09.265920Z", m["createdAt"], "createdAt mismatch")
	facets, ok := m["facets"].([]any)
	require.True(t, ok && len(facets) == 3, "facets: expected 3, got %v", m["facets"])

	t.Logf("JSON length: %d", len(result.JSON))
}

func TestFastCBORToJSON(t *testing.T) {
	t.Parallel()
	result := FastCBORToJSON(testCBORData, "3mdemcarbmb26")

	require.NotEmpty(t, result.JSON, "empty JSON result")
	require.NotEqual(t, "{}", result.JSON, "empty JSON result")

	expected := time.Date(2026, 1, 27, 1, 30, 9, 265920000, time.UTC)
	require.True(t, result.CreatedAt.Equal(expected), "createdAt: got %v, want %v", result.CreatedAt, expected)

	// Verify JSON is valid
	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
}

func TestUntaggedBytesHandling(t *testing.T) {
	t.Parallel()
	// Create CBOR with untagged bytes (not CID-tagged)
	// This is a map with a "data" field containing raw bytes
	// CBOR: A1 64 64617461 44 DEADBEEF
	//       map(1) "data"(4) bytes(4) 0xDEADBEEF
	cborData := []byte{
		0xA1,                   // map(1)
		0x64,                   // text(4)
		0x64, 0x61, 0x74, 0x61, // "data"
		0x44,                   // bytes(4)
		0xDE, 0xAD, 0xBE, 0xEF, // raw bytes
	}

	// Test fast path with untagged bytes
	result := FastCBORToJSON(cborData, "testrkey")
	t.Logf("Fast JSON: %s", result.JSON)

	// Verify bytes are present as $bytes with base64 encoding
	var resultMap map[string]any
	err := json.Unmarshal([]byte(result.JSON), &resultMap)
	require.NoError(t, err, "parse json")

	data, ok := resultMap["data"].(map[string]any)
	require.True(t, ok, "expected data to be map with $bytes, got: %v", resultMap["data"])

	bytesVal, ok := data["$bytes"].(string)
	require.True(t, ok, "expected $bytes field, got: %v", data)

	// 0xDEADBEEF in base64 is "3q2+7w=="
	require.Equal(t, "3q2+7w==", bytesVal, "base64 mismatch")
}

func TestBacklinkExtraction(t *testing.T) {
	t.Parallel()
	// Test DID extraction from a follow record
	follow := createTestFollow()
	var buf bytes.Buffer
	err := follow.MarshalCBOR(&buf)
	require.NoError(t, err, "marshal follow")

	result := FastCBORToJSON(buf.Bytes(), "testrkey")
	t.Logf("Follow JSON: %s", result.JSON)
	t.Logf("Backlinks: %+v", result.Backlinks)

	// Should find the subject DID
	foundSubject := false
	for _, bl := range result.Backlinks {
		if bl.Ref == "did:plc:z72i7hdynmk6r22z27h6tvur" && bl.Path == "subject" && bl.RefType == "did" {
			foundSubject = true
			break
		}
	}
	require.True(t, foundSubject, "did not find expected subject DID backlink")
}

func TestBacklinkExtractionATURI(t *testing.T) {
	t.Parallel()
	// Create a post with a reply that references an AT-URI
	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "This is a reply",
		CreatedAt:     "2026-01-27T01:30:09.265920Z",
		Reply: &bsky.FeedPost_ReplyRef{
			Parent: &comatproto.RepoStrongRef{
				Uri: "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/parent123",
				Cid: "bafkreiewtju6rkfj426g6lnemjzxdcyxm55g5hmlz4x5llb43j6rapkvti",
			},
			Root: &comatproto.RepoStrongRef{
				Uri: "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/root456",
				Cid: "bafkreiewtju6rkfj426g6lnemjzxdcyxm55g5hmlz4x5llb43j6rapkvti",
			},
		},
	}

	var buf bytes.Buffer
	err := post.MarshalCBOR(&buf)
	require.NoError(t, err, "marshal post")

	result := FastCBORToJSON(buf.Bytes(), "testrkey")
	t.Logf("Reply Post JSON: %s", result.JSON)
	t.Logf("Backlinks: %+v", result.Backlinks)

	// Should find the parent and root AT-URIs
	foundParent := false
	foundRoot := false
	for _, bl := range result.Backlinks {
		if bl.Ref == "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/parent123" && bl.RefType == "uri" {
			foundParent = true
		}
		if bl.Ref == "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/root456" && bl.RefType == "uri" {
			foundRoot = true
		}
	}
	require.True(t, foundParent, "did not find expected parent AT-URI backlink")
	require.True(t, foundRoot, "did not find expected root AT-URI backlink")
}

func TestBacklinkExtractionLike(t *testing.T) {
	t.Parallel()
	// Create a like record with nested subject.uri
	like := &bsky.FeedLike{
		LexiconTypeID: "app.bsky.feed.like",
		Subject: &comatproto.RepoStrongRef{
			Uri: "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/liked123",
			Cid: "bafkreiewtju6rkfj426g6lnemjzxdcyxm55g5hmlz4x5llb43j6rapkvti",
		},
		CreatedAt: "2026-01-27T01:30:09.265920Z",
	}

	var buf bytes.Buffer
	err := like.MarshalCBOR(&buf)
	require.NoError(t, err, "marshal like")

	result := FastCBORToJSON(buf.Bytes(), "testrkey")
	t.Logf("Like JSON: %s", result.JSON)
	t.Logf("Backlinks: %+v", result.Backlinks)

	// Should find the subject.uri AT-URI
	foundSubjectURI := false
	for _, bl := range result.Backlinks {
		if bl.Ref == "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/liked123" && bl.Path == "subject.uri" && bl.RefType == "uri" {
			foundSubjectURI = true
			break
		}
	}
	require.True(t, foundSubjectURI, "did not find expected subject.uri AT-URI backlink")
}

func TestErrorHandling(t *testing.T) {
	t.Parallel()
	// Test with invalid CBOR data
	invalidCBOR := []byte{0xFF, 0xFF, 0xFF}
	result := FastCBORToJSON(invalidCBOR, "3mdemcarbmb26")

	// Should return empty JSON, not panic
	require.Equal(t, "{}", result.JSON, "expected empty JSON on error")

	// Should still have a valid timestamp from rkey
	require.False(t, result.CreatedAt.IsZero(), "expected non-zero createdAt from rkey parsing")
}

func TestUnicodeAndEscaping(t *testing.T) {
	t.Parallel()
	// Create a post with unicode text and characters that need JSON escaping
	post := &bsky.FeedPost{
		LexiconTypeID: "app.bsky.feed.post",
		Text:          "Hello 世界! 🌍\nNew line\ttab \"quoted\" back\\slash",
		CreatedAt:     "2026-01-27T01:30:09.265920Z",
	}

	var buf bytes.Buffer
	err := post.MarshalCBOR(&buf)
	require.NoError(t, err, "marshal")

	result := FastCBORToJSON(buf.Bytes(), "testrkey")
	t.Logf("JSON: %s", result.JSON)

	// Verify it's valid JSON
	var m map[string]any
	err = json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON\nraw: %s", result.JSON)

	// Verify the text round-trips correctly
	got := m["text"].(string)
	want := "Hello 世界! 🌍\nNew line\ttab \"quoted\" back\\slash"
	require.Equal(t, want, got, "text mismatch")
}

func TestCorruptedCBOR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "truncated string",
			// Map with 1 key, text key "a", then text value claiming 255 bytes but only 2 present
			data: []byte{0xA1, 0x61, 0x61, 0x78, 0xFF, 0x41, 0x42},
		},
		{
			name: "huge string length",
			// text string with length 0xFFFFFFFFFFFFFFFF
			data: []byte{0x7B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "huge array length",
			// array with length 0xFFFFFFFFFFFFFFFF
			data: []byte{0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "huge map length",
			// map with length 0xFFFFFFFFFFFFFFFF
			data: []byte{0xBB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "truncated mid-value",
			data: []byte{0xA1, 0x61, 0x61, 0x62}, // map(1), key "a", then incomplete text
		},
		{
			name: "empty input",
			data: []byte{},
		},
		{
			name: "single byte garbage",
			data: []byte{0xFF},
		},
		{
			name: "negative int overflow",
			// negative int with max uint64 value
			data: []byte{0x3B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			result := FastCBORToJSON(tt.data, "3mdemcarbmb26")
			t.Logf("result: JSON=%q createdAt=%v", result.JSON, result.CreatedAt)

			// Should return valid JSON (either parsed content or fallback "{}")
			var m any
			err := json.Unmarshal([]byte(result.JSON), &m)
			require.NoError(t, err, "invalid JSON output (json=%q)", result.JSON)

			// Should have a non-zero timestamp from rkey fallback
			require.False(t, result.CreatedAt.IsZero(), "expected non-zero createdAt from rkey parsing")
		})
	}
}

func TestNegativeIntegers(t *testing.T) {
	t.Parallel()
	// CBOR: map(1), key "val", negative int -100
	// A1 63 76616C 38 63
	// map(1) text(3)"val" negint(99) => -1-99 = -100
	data := []byte{0xA1, 0x63, 0x76, 0x61, 0x6C, 0x38, 0x63}

	result := FastCBORToJSON(data, "testrkey")
	t.Logf("JSON: %s", result.JSON)

	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
	val, ok := m["val"].(float64)
	require.True(t, ok, "expected number, got %T: %v", m["val"], m["val"])
	require.Equal(t, float64(-100), val, "expected -100")
}

func TestBoolAndNull(t *testing.T) {
	t.Parallel()
	// CBOR: map(3), "a":true, "b":false, "c":null
	// A3 61 61 F5 61 62 F4 61 63 F6
	data := []byte{
		0xA3,
		0x61, 0x61, 0xF5, // "a": true
		0x61, 0x62, 0xF4, // "b": false
		0x61, 0x63, 0xF6, // "c": null
	}

	result := FastCBORToJSON(data, "testrkey")
	t.Logf("JSON: %s", result.JSON)

	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
	require.Equal(t, true, m["a"], "expected true")
	require.Equal(t, false, m["b"], "expected false")
	require.Nil(t, m["c"], "expected nil")
}

func TestNestedArrays(t *testing.T) {
	t.Parallel()
	// CBOR: map(1), "arr": [1, [2, 3], "hello"]
	// A1 63 617272 83 01 82 02 03 65 68656C6C6F
	data := []byte{
		0xA1,                   // map(1)
		0x63, 0x61, 0x72, 0x72, // "arr"
		0x83,             // array(3)
		0x01,             // 1
		0x82, 0x02, 0x03, // [2, 3]
		0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F, // "hello"
	}

	result := FastCBORToJSON(data, "testrkey")
	t.Logf("JSON: %s", result.JSON)

	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
	arr, ok := m["arr"].([]any)
	require.True(t, ok && len(arr) == 3, "expected array of 3, got %v", m["arr"])
	require.Equal(t, float64(1), arr[0].(float64), "arr[0]: expected 1")
	inner, ok := arr[1].([]any)
	require.True(t, ok && len(inner) == 2, "arr[1]: expected [2,3], got %v", arr[1])
	require.Equal(t, "hello", arr[2].(string), "arr[2]: expected hello")
}

func TestEmptyMap(t *testing.T) {
	t.Parallel()
	// CBOR: empty map
	data := []byte{0xA0}

	result := FastCBORToJSON(data, "testrkey")
	require.Equal(t, "{}", result.JSON, "expected {}")
}

func TestEmptyArray(t *testing.T) {
	t.Parallel()
	// CBOR: map(1), "arr": []
	data := []byte{0xA1, 0x63, 0x61, 0x72, 0x72, 0x80} // map(1), "arr", array(0)

	result := FastCBORToJSON(data, "testrkey")
	t.Logf("JSON: %s", result.JSON)

	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
	arr, ok := m["arr"].([]any)
	require.True(t, ok && len(arr) == 0, "expected empty array, got %v", m["arr"])
}

func TestEmptyString(t *testing.T) {
	t.Parallel()
	// CBOR: map(1), "s": ""
	data := []byte{0xA1, 0x61, 0x73, 0x60} // map(1), "s", text(0)

	result := FastCBORToJSON(data, "testrkey")
	t.Logf("JSON: %s", result.JSON)

	var m map[string]any
	err := json.Unmarshal([]byte(result.JSON), &m)
	require.NoError(t, err, "invalid JSON")
	require.Equal(t, "", m["s"], "expected empty string")
}

// ============== Benchmarks ==============

// BenchmarkFastCBORToJSON benchmarks the fast path
func BenchmarkFastCBORToJSON(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(testCBORData)))

	for i := 0; i < b.N; i++ {
		result := FastCBORToJSON(testCBORData, "3mdemcarbmb26")
		if result.JSON == "" {
			b.Fatal("empty json")
		}
		if result.CreatedAt.IsZero() {
			b.Fatal("zero createdAt")
		}
	}
}

// BenchmarkFastCBORToJSONWithCID benchmarks fast path with CID-containing data
func BenchmarkFastCBORToJSONWithCID(b *testing.B) {
	post, _ := createTestPostWithCID()
	var buf bytes.Buffer
	if err := post.MarshalCBOR(&buf); err != nil {
		b.Fatal(err)
	}
	cidCBOR := buf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(len(cidCBOR)))

	for i := 0; i < b.N; i++ {
		result := FastCBORToJSON(cidCBOR, "testrkey")
		if result.JSON == "" {
			b.Fatal("empty json")
		}
	}
}

// BenchmarkFastCBORWithBacklinks benchmarks fast path including backlink extraction
func BenchmarkFastCBORWithBacklinks(b *testing.B) {
	follow := createTestFollow()
	var buf bytes.Buffer
	if err := follow.MarshalCBOR(&buf); err != nil {
		b.Fatal(err)
	}
	followCBOR := buf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(len(followCBOR)))

	for i := 0; i < b.N; i++ {
		result := FastCBORToJSON(followCBOR, "testrkey")
		if result.JSON == "" {
			b.Fatal("empty json")
		}
		if len(result.Backlinks) == 0 {
			b.Fatal("no backlinks")
		}
	}
}
