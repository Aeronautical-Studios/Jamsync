// Author: Zach Geier (zach@jamhub.dev)

package fastcdc

import (
	"io"

	"github.com/zdgeier/jam/gen/jampb"
)

type OpType byte

type ChunkHashWriter func(ch *jampb.ChunkHash) error
type OperationWriter func(op *jampb.Operation) error

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

func (c *Chunker) ApplyDelta(alignedTarget io.Writer, target io.ReadSeeker, ops chan *jampb.Operation) error {
	var err error
	var n int
	var block []byte

	writeBlock := func(op *jampb.Operation) error {
		_, err := target.Seek(int64(op.ChunkHash.Offset), 0)
		if err != nil {
			return err
		}
		buffer := make([]byte, int(op.ChunkHash.Length)) // TODO: reuse this buffer
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

	for op := range ops {
		switch op.Type {
		case jampb.Operation_OpBlock:
			err = writeBlock(op)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
		case jampb.Operation_OpData:
			_, err = alignedTarget.Write(op.Chunk.Data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Writes delta ops that are diffs from chunkhashes
func (c *Chunker) CreateDelta(chunkHashes []*jampb.ChunkHash, ops OperationWriter) error {
	for i := 0; ; i++ {
		chunk, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Has valid chunk hash to compare against
		if i < len(chunkHashes) {
			chunkHash := chunkHashes[i]
			if chunkHash.Hash == chunk.Hash && chunkHash.Length == chunk.Length && chunkHash.Offset == chunk.Offset {
				ops(&jampb.Operation{
					Type:      jampb.Operation_OpBlock,
					ChunkHash: chunkHash,
				})
			} else {
				ops(&jampb.Operation{
					Type:  jampb.Operation_OpData,
					Chunk: chunk,
				})
			}
		} else {
			ops(&jampb.Operation{
				Type:  jampb.Operation_OpData,
				Chunk: chunk,
			})

		}
	}
	return nil
}
