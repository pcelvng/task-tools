package stat

import (
	"crypto/md5"
	"testing"
)

func BenchmarkAddLine(b *testing.B) {
	sts := Safe{}

	for i := 0; i < b.N; i++ {
		sts.AddLine()
	}
}

func BenchmarkAddBytes(b *testing.B) {
	sts := Safe{}

	for i := 0; i < b.N; i++ {
		sts.AddBytes(200)
	}
}

func BenchmarkTemplateParallel(b *testing.B) {
	sts := Safe{}
	hsh := md5.New()
	hsh.Write([]byte("test message"))

	// run test with '-race' flag to find race conditions
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sts.AddLine()
			sts.AddBytes(100)
			sts.SetSize(50)
			sts.SetChecksum(hsh)
			sts.SetPath("./test/path.txt")
			sts.SetPath("./tests/path.txt")
			_ = sts.Stats()
		}
	})
}
