package stat

import (
	"encoding/hex"
	"encoding/json"
	"hash"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"strings"

	"github.com/jbsmith7741/go-tools/uri"
	"github.com/pcelvng/task-tools/file/util"
)

func New() Stats {
	return Stats{}
}

// NewFromBytes creates Stats from
// json bytes.
func NewFromBytes(b []byte) Stats {
	sts := New()
	json.Unmarshal(b, &sts)
	return sts
}

// NewFromInfo creates Stats from a
// uri formatted info string.
func NewFromInfo(info string) Stats {
	sts := &Stats{}
	uri.Unmarshal(sts, info)

	return *sts
}

type Stats struct {
	// LineCnt returns the file line count.
	LineCnt int64 `json:"linecnt" uri:"linecnt"`

	// ByteCount returns uncompressed raw file byte count.
	ByteCnt int64 `json:"bytecnt" uri:"bytecnt"`

	// Size holds the actual file size.
	Size int64 `json:"size" uri:"size"`

	// Checksum returns the base64 encoded string of the file md5 hash.
	Checksum string `json:"checksum" uri:"checksum"`

	// Path returns the full absolute path of the file.
	Path string `json:"path" uri:"origin"`

	// Created returns the date the file was created or last updated;
	// whichever is more recent.
	Created string `json:"created" uri:"created"`

	mu sync.Mutex
}

// AddLine will atomically and safely increment
// LineCnt by one.
func (s *Stats) AddLine() {
	atomic.AddInt64(&s.LineCnt, 1)
}

// AddBytes will atomically and safely increment
// ByteCnt by 'cnt'.
func (s *Stats) AddBytes(cnt int64) {
	atomic.AddInt64(&s.ByteCnt, cnt)
}

// SetChecksum will correctly calculate and set the
// base64 encoded checksum.
func (s *Stats) SetChecksum(hsh hash.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Checksum = hex.EncodeToString(hsh.Sum(nil))
}

func (s *Stats) SetSize(size int64) {
	curSize := atomic.LoadInt64(&s.Size)
	atomic.CompareAndSwapInt64(&s.Size, curSize, size)
}

func (s *Stats) SetPath(pth string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Path = pth
}

// SetCreated will set the Created field in the
// format time.RFC3339 in UTC.
func (s *Stats) SetCreated(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Created = t.In(time.UTC).Format(time.RFC3339)
}

// ParseCreated will attempt to parse the Created
// field to a time.Time object.
// ParseCreated expects the Created time string is in
// time.RFC3339. If there is a parse error
// then the time.Time zero value is returned.
//
// The returned time will always be in UTC.
func (s *Stats) ParseCreated() time.Time {
	t, _ := time.Parse(time.RFC3339, s.Created)
	return t.In(time.UTC)
}

func (s Stats) ParsePath() (scheme, host, fPth string) {
	return util.ParsePath(s.Path)
}

func (s Stats) JSONBytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s Stats) JSONString() string {
	return string(s.JSONBytes())
}

// InfoString creates a uri-style info string from
// Stats.
func (s Stats) InfoString() string {
	u := &url.URL{}
	u.Scheme, u.Host, u.Path = s.ParsePath()
	qVal := u.Query()
	qVal.Set("linecnt", strconv.FormatInt(s.LineCnt, 10))
	qVal.Set("bytecnt", strconv.FormatInt(s.ByteCnt, 10))
	qVal.Set("size", strconv.FormatInt(s.Size, 10))
	qVal.Set("checksum", s.Checksum)
	qVal.Set("created", s.Created)
	u.RawQuery = qVal.Encode()

	info := u.String()
	if u.Scheme == "" && !strings.HasPrefix(info, "/") {
		info = "/" + info
	}
	return info
}

// Clone will create a copy of stat that won't trigger
// race conditions. Use Clone if you are updating and
// reading from stats at the same time. Read from the
// clone.
func (s Stats) Clone() Stats {
	clone := New()

	s.mu.Lock()
	clone.Checksum = s.Checksum
	clone.Path = s.Path
	clone.Created = s.Created
	s.mu.Unlock()

	clone.LineCnt = atomic.LoadInt64(&s.LineCnt)
	clone.ByteCnt = atomic.LoadInt64(&s.ByteCnt)
	clone.Size = atomic.LoadInt64(&s.Size)
	return clone
}
