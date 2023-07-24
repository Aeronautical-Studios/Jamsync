package opdatastoreworkspace

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/zdgeier/jamhub/gen/pb"
	"google.golang.org/protobuf/proto"
)

type LocalStore struct {
	cache *lru.Cache[string, *os.File]
	mu    sync.Mutex
}

func NewOpDataStoreWorkspace() *LocalStore {
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
	return &LocalStore{
		cache: cache,
	}
}

func (s *LocalStore) filePath(ownerId string, projectId, workspaceId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace/%d/%02X/%02X.locs", ownerId, projectId, workspaceId, pathHash[:1], pathHash)
}

func (s *LocalStore) fileDir(ownerId string, projectId, workspaceId uint64, pathHash []byte) string {
	return fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace/%d/%02X", ownerId, projectId, workspaceId, pathHash[:1])
}

func (s *LocalStore) Read(ownerId string, projectId, workspaceId uint64, pathHash []byte, offset uint64, length uint64) (*pb.Operation, error) {
	filePath := s.filePath(ownerId, projectId, workspaceId, pathHash)
	var (
		currFile *os.File
		err      error
	)
	if s.cache.Contains(filePath) {
		currFile, _ = s.cache.Get(filePath)
	} else {
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Panic(err)
		}
		s.cache.Add(filePath, currFile)
	}
	s.mu.Lock()
	b := make([]byte, length)
	_, err = currFile.ReadAt(b, int64(offset))
	if err != nil {
		// Sometimes we get errors here for some reason so we just try again
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Panic(err)
		}
		s.cache.Add(filePath, currFile)
		_, err = currFile.ReadAt(b, int64(offset))
		if err != nil {
			log.Panic(err)
		}
	}
	s.mu.Unlock()

	op := new(pb.Operation)
	err = proto.Unmarshal(b, op)
	if err != nil {
		log.Panic(err)
	}
	return op, nil
}

func (s *LocalStore) Write(ownerId string, projectId, workspaceId uint64, pathHash []byte, op *pb.Operation) (offset uint64, length uint64, err error) {
	err = os.MkdirAll(s.fileDir(ownerId, projectId, workspaceId, pathHash), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	filePath := s.filePath(ownerId, projectId, workspaceId, pathHash)
	var currFile *os.File
	if s.cache.Contains(filePath) {
		currFile, _ = s.cache.Get(filePath)
	} else {
		currFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Panic(err)
		}
		s.cache.Add(filePath, currFile)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := currFile.Stat()
	if err != nil {
		log.Panic(err)
	}
	data, err := proto.Marshal(op)
	if err != nil {
		log.Panic(err)
	}
	writtenBytes, err := currFile.Write(data)
	if err != nil {
		log.Panic(err)
	}
	return uint64(info.Size()), uint64(writtenBytes), nil
}

func (s *LocalStore) GetChangedPathHashes(ownerId string, projectId uint64, workspaceId uint64) (map[string]interface{}, error) {
	projectDataDir := fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace/%d", ownerId, projectId, workspaceId)
	dirs, err := ioutil.ReadDir(projectDataDir)
	if err != nil {
		return nil, err
	}

	pathHashes := make(map[string]interface{}, 0)
	for _, dir := range dirs {
		files, err := ioutil.ReadDir(filepath.Join(projectDataDir, dir.Name()))
		if err != nil {
			return nil, err
		}

		for _, file := range files {
			data, err := hex.DecodeString(strings.TrimSuffix(file.Name(), ".locs"))
			if err != nil {
				return nil, err
			}
			pathHashes[string(data)] = nil
		}
	}

	return pathHashes, nil
}

func (s *LocalStore) DeleteProject(ownerId string, projectId uint64) error {
	return os.RemoveAll(fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace", ownerId, projectId))
}

func (s *LocalStore) DeleteWorkspace(ownerId string, projectId uint64, workspaceId uint64) error {
	dirs, err := ioutil.ReadDir(fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace/%d", ownerId, projectId, workspaceId))
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		err := os.RemoveAll(fmt.Sprintf("jamhubdata/%s/%d/opdataworkspace/%d/%s", ownerId, projectId, workspaceId, dir.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}
