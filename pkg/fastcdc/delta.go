// Author: Zach Geier (zach@jamhub.dev)

package fastcdc

import (
	"io"

	"github.com/zdgeier/jam/gen/jampb"
)

type OpType byte

type ChunkHashWriter func(chunkHash *jampb.ChunkHash) error
type ChunkWriter func(chunk *jampb.Chunk) error

func (c *Chunker) CreateHashSignature() (map[uint64][]byte, error) {
	hashes := make(map[uint64][]byte)
	for {
		chunk, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		hashes[chunk.Hash] = nil
	}
	return hashes, nil
}

func (c *Chunker) CreateSignature(sw ChunkHashWriter) error {
	for {
		chunk, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		sw(&jampb.ChunkHash{
			Offset: chunk.Offset,
			Length: chunk.Length,
			Hash:   chunk.Hash,
		})
	}
	return nil
}

func (c *Chunker) ApplyDelta(alignedTarget io.Writer, target io.ReadSeeker, chunks chan *jampb.Chunk) error {
	var err error
	var n int
	var block []byte

	writeBlock := func(chunk *jampb.Chunk) error {
		_, err := target.Seek(int64(chunk.Offset), 0)
		if err != nil {
			return err
		}
		buffer := make([]byte, int(chunk.Length)) // TODO: reuse this buffer
		n, err = target.Read(buffer)
		if err != nil {
			if err != io.ErrUnexpectedEOF {
				return err
			}
		}
		block = buffer[:n]
		_, err = alignedTarget.Write(block)
		if err != nil {
			return err
		}
		return nil
	}

	for chunk := range chunks {
		if chunk.Data == nil {
			err = writeBlock(chunk)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
		} else {
			_, err = alignedTarget.Write(chunk.Data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Writes delta ops that are diffs from chunkhashes
func (c *Chunker) CreateDelta(hashes map[uint64][]byte, chunks ChunkWriter) error {
	for {
		chunk, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, ok := hashes[chunk.Hash]; ok {
			err := chunks(&jampb.Chunk{
				Hash:   chunk.Hash,
				Offset: chunk.Offset,
				Length: chunk.Length,
			})
			if err != nil {
				return err
			}
		} else {
			err := chunks(chunk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
