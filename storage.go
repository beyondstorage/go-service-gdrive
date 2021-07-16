package gdrive

import (
	"context"
	"io"
	"strings"

	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.ID = s.getAbsPath(path)
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	fileId := s.pathToId(path)
	err = s.service.Files.Delete(fileId).Do()
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	panic("not implemented")
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta
}

// We just get the fileId of a file or directory in the root folder
func (s *Storage) nameToId(fileName string) string {
	searchArgs := "name='" + fileName + "' and parents = 'root'"
	fileList, _ := s.service.Files.List().Q(searchArgs).Do()
	//Assume that there is only one file matches
	return fileList.Files[0].Id
}

func (s *Storage) pathToId(path string) string {
	fileName := s.getFileName(path)
	// This means path is just a file or directory, like `foo.txt` or `dir`
	if fileName == path {
		return s.nameToId(fileName)
	} else {
		absPath := s.getAbsPath(path)
		tmp := strings.Split(absPath, "/")
		// First we need the FileId of the root directory, or we can not do a search
		superId := s.nameToId(tmp[0])
		for i := 0; i < len(tmp)-1; i++ {
			subId, _ := s.searchContentInDir(superId, tmp[i+1])
			superId = subId
		}
		return superId
	}
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	panic("not implemented")
}

// We pass the fileId of the directory, and it returns the fileId of the content in the directory
func (s *Storage) searchContentInDir(dirId string, contentName string) (fileId string, err error) {
	searchArgs := "parents='" + dirId + "'"
	fileList, _ := s.service.Files.List().Q(searchArgs).Do()
	for _, file := range fileList.Files {
		if file.Name == contentName {
			fileId = file.Id
		}
	}
	return fileId, err
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	panic("not implemented")
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	panic("not implemented")
}
