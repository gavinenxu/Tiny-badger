package config

type Options struct {
	Dir string

	SyncWrites bool
	InMemory   bool
	ReadOnly   bool

	MemtableSize int64
}

func DefaultOptions(path string) Options {
	return Options{
		Dir: path,

		SyncWrites: false,
		InMemory:   false,
		ReadOnly:   false,

		MemtableSize: 32 << 20, // 32MB
	}
}
