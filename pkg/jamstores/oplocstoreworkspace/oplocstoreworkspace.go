package oplocstoreworkspace

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/zdgeier/jam/gen/jampb"
	"google.golang.org/protobuf/proto"
)

type LocalOpLocStore struct {
	cache *lru.Cache[string, *os.File]
	mu    sync.Mutex
}

func (s *LocalOpLocStore) filePath(ownerId string, projectId, workspaceId, changeId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d/%d/%02X/%02X.locs", ownerId, projectId, workspaceId, changeId, pathHash[:1], pathHash)
}

func (s *LocalOpLocStore) fileDir(ownerId string, projectId, workspaceId, changeId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d/%d/%02X", ownerId, projectId, workspaceId, changeId, pathHash[:1])
}

func NewOpLocStoreWorkspace() *LocalOpLocStore {
	cache, err := lru.NewWithEvict(2048, func(path string, file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
			return
		}
	})
	if err != nil {
		panic(err)
	}
	return &LocalOpLocStore{
		cache: cache,
	}
}

func (s *LocalOpLocStore) InsertOperationLocations(opLocs *jampb.WorkspaceOperationLocations) error {
	var (
		currFile *os.File
		err      error
	)
	err = os.MkdirAll(s.fileDir(opLocs.GetOwnerUsername(), opLocs.GetProjectId(), opLocs.GetWorkspaceId(), opLocs.GetChangeId(), opLocs.GetPathHash()), os.ModePerm)
	if err != nil {
		return err
	}
	filePath := s.filePath(opLocs.GetOwnerUsername(), opLocs.GetProjectId(), opLocs.GetWorkspaceId(), opLocs.GetChangeId(), opLocs.GetPathHash())
	if s.cache.Contains(filePath) {
		currFile, _ = s.cache.Get(filePath)
	} else {
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		s.cache.Add(filePath, currFile)
	}
	bytes, err := proto.Marshal(opLocs)
	if err != nil {
		return err
	}
	s.mu.Lock()
	_, err = currFile.Write(bytes)
	s.mu.Unlock()
	return err
}

func (s *LocalOpLocStore) ListOperationLocations(ownerId string, projectId, workspaceId, changeId uint64, pathHash []byte) (opLocs *jampb.WorkspaceOperationLocations, err error) {
	filePath := s.filePath(ownerId, projectId, workspaceId, changeId, pathHash)
	_, err = os.Stat(filePath)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	var currFile *os.File
	if s.cache.Contains(filePath) {
		currFile, _ = s.cache.Get(filePath)
	} else {
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		s.cache.Add(filePath, currFile)
	}

	s.mu.Lock()
	buf := bytes.NewBuffer(nil)
	_, err = currFile.Seek(0, 0)
	if err != nil {
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		s.cache.Add(filePath, currFile)
		_, err = currFile.Seek(0, 0)
		if err != nil {
			panic(err)
		}
	}
	_, err = io.Copy(buf, currFile)
	if err != nil {
		panic(err)
	}
	s.mu.Unlock()

	opLocs = &jampb.WorkspaceOperationLocations{}
	err = proto.Unmarshal(buf.Bytes(), opLocs)
	if err != nil {
		panic(err)
	}
	return opLocs, err
}

func (s *LocalOpLocStore) MaxChangeId(ownerId string, projectId, workspaceId uint64) (uint64, error) {
	workspaceDir := fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d", ownerId, projectId, workspaceId)
	_, err := os.Stat(workspaceDir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}

	files, err := os.ReadDir(fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d", ownerId, projectId, workspaceId))
	if err != nil {
		log.Panic(err)
	}

	maxChangeId := 0
	for _, file := range files {
		changeId, err := strconv.Atoi(file.Name())
		if err != nil {
			return 0, err
		}
		if changeId > maxChangeId {
			maxChangeId = changeId
		}
	}
	return uint64(maxChangeId), nil
}

func (s *LocalOpLocStore) DeleteProject(ownerId string, projectId uint64) error {
	return os.RemoveAll(fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace", ownerId, projectId))
}

func (s *LocalOpLocStore) DeleteWorkspace(ownerId string, projectId uint64, workspaceId uint64) error {
	dirs, err := os.ReadDir(fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d", ownerId, projectId, workspaceId))
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		err := os.RemoveAll(fmt.Sprintf("jamhubdata/%s/%d/oplocstoreworkspace/%d/%s", ownerId, projectId, workspaceId, dir.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}
