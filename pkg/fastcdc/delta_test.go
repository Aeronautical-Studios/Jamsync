// Modified by from https://github.com/jbreiding/rsync-go/blob/master/gen_test.go by Zach Geier (zach@jamhub.dev)

package fastcdc

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/zdgeier/jam/gen/jampb"
)

type RandReader struct {
	rand.Source
}

func (rr RandReader) Read(sink []byte) (int, error) {
	var tail, head int
	buf := make([]byte, 8)
	var r uint64
	for {
		head = min(tail+8, len(sink))
		if tail == head {
			return head, nil
		}

		r = (uint64)(rr.Int63())
		buf[0] = (byte)(r)
		buf[1] = (byte)(r >> 8)
		buf[2] = (byte)(r >> 16)
		buf[3] = (byte)(r >> 24)
		buf[4] = (byte)(r >> 32)
		buf[5] = (byte)(r >> 40)
		buf[6] = (byte)(r >> 48)
		buf[7] = (byte)(r >> 56)

		tail += copy(sink[tail:head], buf)
	}
}

type pair struct {
	Source, Target content
	Description    string
}
type content struct {
	Len   int
	Seed  int64
	Alter int
	Data  []byte
}

func (c *content) Fill() {
	c.Data = make([]byte, c.Len)
	src := rand.NewSource(c.Seed)
	RandReader{src}.Read(c.Data)

	if c.Alter > 0 {
		r := rand.New(src)
		for i := 0; i < c.Alter; i++ {
			at := r.Intn(len(c.Data))
			c.Data[at] += byte(r.Int())
		}
	}
}

func Test_GenData(t *testing.T) {
	var pairs = []pair{
		{
			Source:      content{Len: 512 * 1024, Seed: 42, Alter: 0},
			Target:      content{Len: 512 * 1024, Seed: 42, Alter: 1},
			Description: "Same length, slightly different content.",
		},
		{
			Source:      content{Len: 512*1024 + 89, Seed: 9824, Alter: 0},
			Target:      content{Len: 512*1024 + 89, Seed: 2345, Alter: 0},
			Description: "Same length, very different content.",
		},
		{
			Source:      content{Len: 512 * 1024, Seed: 42, Alter: 0},
			Target:      content{Len: 512 * 1024, Seed: 42, Alter: 0},
			Description: "Target shorter then source, same content.",
		},
		{
			Source:      content{Len: 512*1024 + 89, Seed: 42, Alter: 0},
			Target:      content{Len: 256*1024 + 19, Seed: 42, Alter: 5},
			Description: "Target shorter then source, slightly different content.",
		},
		{
			Source:      content{Len: 256*1024 + 19, Seed: 42, Alter: 0},
			Target:      content{Len: 512*1024 + 89, Seed: 42, Alter: 0},
			Description: "Source shorter then target, same content.",
		},
		{
			Source:      content{Len: 512*1024 + 89, Seed: 42, Alter: 5},
			Target:      content{Len: 256*1024 + 19, Seed: 42, Alter: 0},
			Description: "Source shorter then target, slightly different content.",
		},
		{
			Source:      content{Len: 512*1024 + 89, Seed: 42, Alter: 0},
			Target:      content{Len: 0, Seed: 42, Alter: 0},
			Description: "Target empty and source has content.",
		},
		{
			Source:      content{Len: 0, Seed: 42, Alter: 0},
			Target:      content{Len: 512*1024 + 89, Seed: 42, Alter: 0},
			Description: "Source empty and target has content.",
		},
		{
			Source:      content{Len: 872, Seed: 9824, Alter: 0},
			Target:      content{Len: 235, Seed: 2345, Alter: 0},
			Description: "Source and target both smaller then a block size.",
		},
	}

	for _, p := range pairs {
		(&p.Source).Fill()
		(&p.Target).Fill()

		sourceBuffer := bytes.NewReader(p.Source.Data)
		targetBuffer := bytes.NewReader(p.Target.Data)

		sig := make(map[uint64][]byte, 10)
		sourceChunker, err := NewJamChunker(Options{
			AverageSize: 1024 * 64,
			Seed:        84372,
		})
		assertNoError(t, err)
		sourceChunker.SetChunkerReader(sourceBuffer)
		targetChunker, err := NewJamChunker(Options{
			AverageSize: 1024 * 64,
			Seed:        84372,
		})
		assertNoError(t, err)
		targetChunker.SetChunkerReader(targetBuffer)

		err = targetChunker.CreateSignature(func(ch *jampb.ChunkHash) error {
			sig[ch.GetHash()] = nil
			return nil
		})
		if err != nil {
			t.Errorf("Failed to create signature: %s", err)
		}
		chunksOut := make(chan *jampb.Chunk)
		tot := 0
		go func() {
			var blockCt, dataCt, bytes int
			defer close(chunksOut)
			err := sourceChunker.CreateDelta(sig, func(chunk *jampb.Chunk) error {
				tot += int(chunk.Length)
				if chunk.Data == nil {
					blockCt++
				} else {
					b := make([]byte, len(chunk.Data))
					copy(b, chunk.Data)
					chunk.Data = b
					dataCt++
					bytes += len(chunk.Data)
				}
				chunksOut <- chunk
				return nil
			})
			t.Logf("Block Ops:%5d, Data Ops: %5d, Data Len: %5dKiB, For %s.", blockCt, dataCt, bytes/1024, p.Description)
			if err != nil {
				t.Errorf("Failed to create delta: %s", err)
			}
		}()

		result := new(bytes.Buffer)

		readBuffer := bytes.NewReader(p.Target.Data)
		err = targetChunker.ApplyDelta(result, readBuffer, chunksOut)
		if err != nil {
			t.Errorf("Failed to apply delta: %s", err)
		}
		if result.Len() != len(p.Source.Data) {
			t.Errorf("Result not same size as source: %s", p.Description)
		} else if bytes.Equal(result.Bytes(), p.Source.Data) == false {
			t.Errorf("Result is different from the source: %s", p.Description)
		}

		p.Source.Data = nil
		p.Target.Data = nil
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("expected no error but received: %v", err)
	}
}
