package ledis

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/golang/snappy"
	"github.com/ledisdb/ledisdb/store"
)

// DumpHead is the head of a dump.
type DumpHead struct {
	CommitID uint64
}

// Read reads meta from the Reader.
func (h *DumpHead) Read(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &h.CommitID)
}

// Write writes meta to the Writer
func (h *DumpHead) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, h.CommitID)
}

// DumpFile dumps data to the file
func (l *Ledis) DumpFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return l.Dump(f)
}

// Dump dumps data to the Writer.
func (l *Ledis) Dump(w io.Writer) error {
	var err error

	var commitID uint64
	var snap *store.Snapshot

	l.wLock.Lock()

	if l.r != nil {
		if commitID, err = l.r.LastCommitID(); err != nil {
			l.wLock.Unlock()
			return err
		}
	}

	if snap, err = l.ldb.NewSnapshot(); err != nil {
		l.wLock.Unlock()
		return err
	}
	defer snap.Close()

	l.wLock.Unlock()

	wb := bufio.NewWriterSize(w, 4096)

	h := &DumpHead{commitID}

	if err = h.Write(wb); err != nil {
		return err
	}

	it := snap.NewIterator()
	defer it.Close()
	it.SeekToFirst()

	compressBuf := make([]byte, 4096)

	var key []byte
	var value []byte
	for ; it.Valid(); it.Next() {
		key = it.RawKey()
		value = it.RawValue()

		key = snappy.Encode(compressBuf, key)

		if err = binary.Write(wb, binary.BigEndian, uint16(len(key))); err != nil {
			return err
		}

		if _, err = wb.Write(key); err != nil {
			return err
		}

		value = snappy.Encode(compressBuf, value)

		if err = binary.Write(wb, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}

		if _, err = wb.Write(value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// LoadDumpFile clears all data and loads dump file to db
func (l *Ledis) LoadDumpFile(path string) (*DumpHead, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return l.LoadDump(f)
}

// LoadDump clears all data and loads dump file to db
func (l *Ledis) LoadDump(r io.Reader) (*DumpHead, error) {
	l.wLock.Lock()
	defer l.wLock.Unlock()

	var err error
	if err = l.flushAll(); err != nil {
		return nil, err
	}

	rb := bufio.NewReaderSize(r, 4096)

	h := new(DumpHead)

	if err = h.Read(rb); err != nil {
		return nil, err
	}

	var keyLen uint16
	var valueLen uint32

	deKeyBuf := make([]byte, 4096)
	deValueBuf := make([]byte, 4096)

	var key, value []byte

	var wb *store.WriteBatch

	n := 0

	var keyBuf bytes.Buffer
	var valueBuf bytes.Buffer

	for {
		if wb == nil {
			wb = l.ldb.NewWriteBatch()
		}
		if err = binary.Read(rb, binary.BigEndian, &keyLen); err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		if _, err = io.CopyN(&keyBuf, rb, int64(keyLen)); err != nil {
			return nil, err
		}

		if key, err = snappy.Decode(deKeyBuf, keyBuf.Bytes()); err != nil {
			return nil, err
		}

		if err = binary.Read(rb, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}

		if _, err = io.CopyN(&valueBuf, rb, int64(valueLen)); err != nil {
			return nil, err
		}

		if value, err = snappy.Decode(deValueBuf, valueBuf.Bytes()); err != nil {
			return nil, err
		}

		n++
		wb.Put(key, value)

		count := 1024 * 4
		if n%count == 0 {
			fmt.Println("n =", n)

			if err = wb.Commit(); err != nil {
				return nil, err
			}
			wb.Close()
			wb = nil
			runtime.GC()
		}

		// if err = l.ldb.Put(key, value); err != nil {
		// 	return nil, err
		// }

		keyBuf.Reset()
		valueBuf.Reset()
	}

	if wb != nil {
		fmt.Println("final write n =", n)

		if err = wb.Commit(); err != nil {
			return nil, err
		}
		wb.Close()

		if l.r != nil {
			if err := l.r.UpdateCommitID(h.CommitID); err != nil {
				return nil, err
			}
		}
	}

	l.CompactStore()

	return h, nil
}
