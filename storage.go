package gdrive

import (
	"context"
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

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	fileId, err := s.pathToId(path)
	err = s.service.Files.Delete(fileId).Do()
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	nextFn := func(ctx context.Context, page *ObjectPage) error {
		fs, err := s.listAllFiles(ctx, path)
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
			o.SetContentLength(int64(f.Size))
			page.Data = append(page.Data, o)
		}
		return IterateDone
	}
	oi = NewObjectIterator(ctx, nextFn, nil)
	return
}

func (s *Storage) listAllFiles(ctx context.Context, path string) ([]*drive.File, error) {
	var fs []*drive.File
	searchId, err := s.pathToId(path)
	if err != nil {
		return nil, err
	}
	pageToken := ""
	for {
		q := s.service.Files.List().Context(ctx).Q(searchId)
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

func (s *Storage) pathToId(path string) (fileId string, err error) {
	fileName := s.getFileName(path)
	// This means path is just a file or directory, like `foo.txt` or `dir`
	if fileName == path {
		return s.nameToId(fileName), nil
	} else {
		absPath := s.getAbsPath(path)
		tmp := strings.Split(absPath, "/")
		// First we need the FileId of the root directory, or we can not do a search
		fileId = s.nameToId(tmp[0])
		for i := 0; i < len(tmp)-1; i++ {
			subId, _ := s.searchContentInDir(fileId, tmp[i+1])
			fileId = subId
		}
		if err != nil {
			return " ", err
		}
		return fileId, nil
	}
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	fileId, err := s.pathToId(path)
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
	_, err = s.pathToId(path)
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
	_, err = s.pathToId(path)
	if err != nil {
		// upload
		tmp := strings.Split(path, "/")
		var superId string
		for i := 0; i < len(tmp); i++ {
			if i == 0 {
				dir := &drive.File{
					Name:     tmp[i],
					MimeType: "application/vnd.google-apps.folder"}
				f, _ := s.service.Files.Create(dir).Do()
				superId = f.Id
			} else if i != len(tmp)-1 {
				dir := &drive.File{
					Name:     tmp[i],
					Parents:  []string{superId},
					MimeType: "application/vnd.google-apps.folder"}
				f, _ := s.service.Files.Create(dir).Do()
				superId = f.Id

			} else {
				file := &drive.File{Name: tmp[i]}
				_, _ = s.service.Files.Create(file).Context(ctx).Media(r).Do()
			}
		}
	} else {
		// update
		fileId, err := s.pathToId(path)
		newFile := &drive.File{Name: s.getFileName(path)}
		s.service.Files.Update(fileId, newFile).Context(ctx).Media(r).Do()

		if err != nil {
			return 0, err
		}
	}
	return size, nil

}
