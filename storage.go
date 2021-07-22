package gdrive

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"

	. "github.com/beyondstorage/go-storage/v4/types"
	"google.golang.org/api/drive/v3"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.ID = s.getAbsPath(path)
	o.Path = path
	return o
}

func (s *Storage) createDir(ctx context.Context, name string, parents string) (fileId string, err error) {
	dir := &drive.File{
		Name:     name,
		Parents:  []string{parents},
		MimeType: "application/vnd.google-apps.folder",
	}
	f, err := s.service.Files.Create(dir).Context(ctx).Do()
	if err != nil {
		return "", err
	}
	return f.Id, nil
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	var fileId string
	fileId, err = s.pathToId(ctx, path)
	err = s.service.Files.Delete(fileId).Do()
	if err != nil {
		return err
	}
	return nil
}

// Get FileID via it's name and parents' fileID
func (s *Storage) getFile(ctx context.Context, name string, parents string) ([]*drive.File, error) {
	searchArg := fmt.Sprintf("name='%s' and parents='%s'", name, parents)
	return s.rawListFiles(ctx, searchArg)
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	nextFn := func(ctx context.Context, page *ObjectPage) error {
		fs, err := s.listFiles(ctx, path)
		if err != nil {
			return err
		}
		for _, f := range fs {
			o := NewObject(s, true)
			o.Path = f.Name

			switch f.MimeType {
			case "application/vnd.google-apps.folder":
				o.Mode |= ModeDir
			default:
				o.Mode |= ModeRead
			}
			o.SetContentLength(f.Size)
			page.Data = append(page.Data, o)
		}
		return IterateDone
	}
	oi = NewObjectIterator(ctx, nextFn, nil)
	return
}

func (s *Storage) listFiles(ctx context.Context, path string) (fs []*drive.File, err error) {
	var pathId string
	pathId, err = s.pathToId(ctx, path)
	if err != nil {
		return fs, err
	}
	return s.listFilesInDir(ctx, pathId)
}

// List files in a folder by passing the folder's FileID
func (s *Storage) listFilesInDir(ctx context.Context, fileId string) ([]*drive.File, error) {
	searchArg := fmt.Sprintf("parents='%s'", fileId)
	return s.rawListFiles(ctx, searchArg)
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) pathToId(ctx context.Context, path string) (fileId string, err error) {
	absPath := s.getAbsPath(path)
	pathUnits := strings.Split(absPath, "/")

	for i := 0; i < len(pathUnits); i++ {
		if i == 0 {
			fileId, err = s.searchContentInDir(ctx, "root", pathUnits[i])
		}
		fileId, err = s.searchContentInDir(ctx, fileId, pathUnits[i])
	}

	if err != nil {
		return "", err
	}
	return fileId, nil
}

func (s *Storage) rawListFiles(ctx context.Context, searchArg string) ([]*drive.File, error) {
	var fs []*drive.File
	pageToken := ""
	for {
		q := s.service.Files.List().Context(ctx).Q(searchArg).Fields("id", "name", "mimeType")
		// If we have a pageToken set, apply it to the query
		if pageToken != "" {
			q = q.PageToken(pageToken)
		}
		r, err := q.Do()
		if err != nil {
			return fs, err
		}
		fs = append(fs, r.Files...)
		pageToken = r.NextPageToken
		if pageToken == "" {
			break
		}
	}
	return fs, nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	fileId, err := s.pathToId(ctx, path)
	if err != nil {
		return 0, err
	}
	f, err := s.service.Files.Get(fileId).Context(ctx).Download()
	if err != nil {
		return 0, err
	}

	if opt.HasIoCallback {
		iowrap.CallbackReadCloser(f.Body, opt.IoCallback)
	}

	return io.Copy(w, f.Body)
}

// We pass the fileId of the directory, and it returns the fileId of the content in the directory
func (s *Storage) searchContentInDir(ctx context.Context, dirId string, contentName string) (fileId string, err error) {
	fileList, err := s.listFilesInDir(ctx, dirId)

	if err != nil {
		return " ", err
	}

	for _, file := range fileList {
		// What if the file does not exist???
		if file.Name == contentName {
			fileId = file.Id
		}
	}

	if len(fileId) == 0 {
		return "", err
	}
	return fileId, nil
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	_, err = s.pathToId(ctx, path)
	if err != nil {
		return nil, err
	}
	rp := s.getAbsPath(path)
	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	return o, nil
}

// First we need make sure this file is not exist
// If it is, then we upload it
// If the path is like `foo.txt`, then we upload it directly.
// But if the path is like `foo/bar.txt`, we need create the directory one by one
func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	r = io.LimitReader(r, size)

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}
	_, err = s.pathToId(ctx, path)
	var fileId string
	if err != nil {
		// upload
		tmp := strings.Split(path, "/")
		for i := 0; i < len(tmp); i++ {
			if i == 0 {
				fileId, err = s.createDir(ctx, tmp[0], "root")
			} else if i != len(tmp)-1 {
				fileId, err = s.createDir(ctx, tmp[i], fileId)
			} else {
				file := &drive.File{Name: tmp[i]}
				_, err = s.service.Files.Create(file).Context(ctx).Media(r).Do()
			}
		}
	} else {
		// update
		fileId, err = s.pathToId(ctx, path)
		newFile := &drive.File{Name: s.getFileName(path)}
		_, err = s.service.Files.Update(fileId, newFile).Context(ctx).Media(r).Do()
	}

	if err != nil {
		return 0, err
	}
	return size, nil
}
