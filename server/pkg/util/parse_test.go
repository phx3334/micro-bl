package util

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{in: "30s", want: 30 * time.Second},
		{in: "15m", want: 15 * time.Minute},
		{in: "1h30m", want: 90 * time.Minute},
		{in: "2d", want: 48 * time.Hour},
		{in: "1d2h3m4s", want: 26*time.Hour + 3*time.Minute + 4*time.Second},
		{in: " 10m ", want: 10 * time.Minute},
	}
	for _, tc := range cases {
		got, err := ParseDuration(tc.in)
		if err != nil {
			t.Fatalf("ParseDuration(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseDuration(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	for _, bad := range []string{"", "abc", "1x", "1h30"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Fatalf("ParseDuration(%q) should fail", bad)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	cursor := EncodeCursor(3.5, 42)
	score, id := DecodeCursor(cursor)
	if score != 3.5 || id != 42 {
		t.Fatalf("DecodeCursor() = (%v, %d), want (3.5, 42)", score, id)
	}

	if got := EncodeCursor(0, 0); got != "" {
		t.Fatalf("zero cursor = %q, want empty", got)
	}
	if score, id := DecodeCursor("not-base64"); score != 0 || id != 0 {
		t.Fatalf("invalid cursor = (%v, %d), want (0, 0)", score, id)
	}
}

func TestCacheMissIDs(t *testing.T) {
	hit := map[uint]bool{1: true, 2: true}
	got := CacheMissIDs(hit, []uint{1, 2, 3, 4})
	if len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("CacheMissIDs() = %v, want [3 4]", got)
	}
	if got := CacheMissIDs(nil, nil); got != nil {
		t.Fatalf("empty input = %v, want nil", got)
	}
}
