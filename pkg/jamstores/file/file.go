package file

import (
	"context"
	"io"
	"log"

	"github.com/zdgeier/jam/gen/jampb"
	"github.com/zdgeier/jam/pkg/fastcdc"
	"github.com/zeebo/xxh3"
)

func DownloadCommittedFile(client jampb.JamHubClient, ownerUsername string, projectId uint64, commitId uint64, filePath string, localReader io.ReadSeeker, localWriter io.Writer) error {
	localChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
	if err != nil {
		return err
	}
	localChunker.SetChunkerReader(localReader)

	localChunkHashes, err := localChunker.CreateHashSignature()
	if err != nil {
		return err
	}

	stream, err := client.ReadCommittedFile(context.Background(), &jampb.ReadCommittedFileRequest{
		ProjectId:        projectId,
		OwnerUsername:    ownerUsername,
		CommitId:         commitId,
		PathHash:         PathToHash(filePath),
		LocalChunkHashes: localChunkHashes,
	})
	if err != nil {
		return err
	}

	chunks := make(chan *jampb.Chunk)
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println(err)
				return
			}
			chunks <- in.Chunk
		}
		close(chunks)
	}()

	localReader.Seek(0, 0)
	localChunker.SetChunkerReader(localReader)
	err = localChunker.ApplyDelta(localWriter, localReader, chunks)
	if err != nil {
		return err
	}

	return err
}

func DownloadWorkspaceFile(client jampb.JamHubClient, ownerUsername string, projectId uint64, workspaceId uint64, changeId uint64, filePath string, localReader io.ReadSeeker, localWriter io.Writer) error {
	localChunker, err := fastcdc.NewJamChunker(fastcdc.DefaultOpts)
	if err != nil {
		return err
	}
	localChunker.SetChunkerReader(localReader)

	localChunkHashes := make(map[uint64][]byte)
	err = localChunker.CreateSignature(func(localChunk *jampb.ChunkHash) error {
		localChunkHashes[localChunk.Hash] = nil
		return nil
	})
	if err != nil {
		return err
	}

	stream, err := client.ReadWorkspaceFile(context.Background(), &jampb.ReadWorkspaceFileRequest{
		ProjectId:        projectId,
		OwnerUsername:    ownerUsername,
		WorkspaceId:      workspaceId,
		ChangeId:         changeId,
		PathHash:         PathToHash(filePath),
		LocalChunkHashes: localChunkHashes,
	})
	if err != nil {
		return err
	}

	chunks := make(chan *jampb.Chunk)
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			chunks <- in.Chunk
		}
		close(chunks)
	}()

	localReader.Seek(0, 0)
	localChunker.SetChunkerReader(localReader)
	err = localChunker.ApplyDelta(localWriter, localReader, chunks)
	if err != nil {
		return err
	}

	return nil
}

func PathToHash(path string) []byte {
	h := xxh3.Hash128([]byte(path)).Bytes()
	return h[:]
}
