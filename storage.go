package gdrive

import (
	"context"
	"fmt"
	"github.com/beyondstorage/go-storage/v4/services"
	"io"
	"strings"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"

	. "github.com/beyondstorage/go-storage/v4/types"
	"google.golang.org/api/drive/v3"
)

const DirectoryMimeType = "application/vnd.google-apps.folder"

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.ID = s.getAbsPath(path)
	o.Path = path
	return o
}

func (s *Storage) createDir(ctx context.Context, path string, opt pairStorageCreateDir) (o *Object, err error) {
	path = s.getAbsPath(path)
	pathUnits := strings.Split(path, "/")
	parentsId := "root"
	for i := 0; i < len(pathUnits); i++ {
		parentsId, err = s.mkDir(ctx, parentsId, pathUnits[i])
		if err != nil {
			return nil, err
		}
	}

	o = s.newObject(true)
	o.ID = path
	o.Path = path
	o.Mode = ModeDir

	return o, nil

}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	var fileId string
	fileId, err = s.pathToId(ctx, path)
	if err != nil {
		return err
	}
	err = s.service.Files.Delete(fileId).Do()
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		limit: 200,
		path:  s.getAbsPath(path),
	}

	if !opt.HasListMode || opt.ListMode.IsDir() {
		return NewObjectIterator(ctx, s.nextObjectPage, input), nil
	} else {
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta
}

// Create a directory by passing it's name and the parents' fileId.
// It will return the fileId of the directory whether it exist or not.
// If error occurs, it will return an empty string and error.
func (s *Storage) mkDir(ctx context.Context, parents string, dirName string) (string, error) {
	id, err := s.searchContentInDir(ctx, parents, dirName)
	if err != nil {
		return "", err
	}
	// Simply return the fileId if the directory already exist
	if id != "" {
		return id, nil
	}

	// create a directory if not exist
	dir := &drive.File{
		Name:     dirName,
		Parents:  []string{parents},
		MimeType: DirectoryMimeType,
	}
	f, err := s.service.Files.Create(dir).Context(ctx).Do()
	if err != nil {
		return "", err
	}
	return f.Id, nil
}

func (s *Storage) nextObjectPage(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)
	dirId, err := s.pathToId(ctx, input.path)
	if err != nil {
		return err
	}
	q := s.service.Files.List().Q(fmt.Sprintf("parents=%s", dirId))

	if input.pageToken != "" {
		q = q.PageToken(input.pageToken)
	}
	r, err := q.Do()

	if err != nil {
		return err
	}
	for _, f := range r.Files {
		o := s.newObject(true)
		// There is no way to get the path of the file directly, we have to do this
		o.Path = input.path + "/" + f.Name
		switch f.MimeType {
		case DirectoryMimeType:
			o.Mode = ModeDir
		default:
			o.Mode = ModeRead
		}
		page.Data = append(page.Data, o)
	}

	input.pageToken = r.NextPageToken
	return nil
}

// TODO: add cache support
// pathToId converts path to fileId, as we talked in RFC-14.
// Ref: https://github.com/beyondstorage/go-service-gdrive/blob/master/docs/rfcs/14-gdrive-for-go-storage-design.md
// If success, we will return the fileId of the path we passing, and nil for sure.
// But if the path is not exist, we will return an empty string and error.
func (s *Storage) pathToId(ctx context.Context, path string) (fileId string, err error) {
	absPath := s.getAbsPath(path)
	pathUnits := strings.Split(absPath, "/")
	fileId = "root"
	// Traverse the whole path, break the loop if we fails at one search
	for i := 0; i < len(pathUnits); i++ {
		fileId, err = s.searchContentInDir(ctx, fileId, pathUnits[i])
		if fileId == "" {
			break
		}
	}

	if err != nil {
		return "", err
	}

	return fileId, nil
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

// Search something in directory by passing it's name and the fileId of the folder.
// It will return the fileId of the content we want, and nil for sure.
// If nothing is found, we will return an empty string and nil.
// We will only return non nil if error occurs.
func (s *Storage) searchContentInDir(ctx context.Context, dirId string, contentName string) (fileId string, err error) {
	searchArg := fmt.Sprintf("name = %s and parents = %s", contentName, dirId)
	fileList, err := s.service.Files.List().Context(ctx).Q(searchArg).Do()
	if err != nil {
		return "", err
	}
	// Because we assume that the path is unique, so there would be only two results: One file matches or none
	if len(fileList.Files) == 0 {
		return "", nil
	}
	return fileList.Files[0].Id, nil

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

// First we need make sure this file is not exist.
// If it is, then we upload it, or we will overwrite it.
func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	r = io.LimitReader(r, size)

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}
	fileId, err := s.pathToId(ctx, path)
	if err != nil {
		// upload
		file := &drive.File{Name: s.getFileName(path)}
		_, err = s.service.Files.Create(file).Context(ctx).Media(r).Do()
		if err != nil {
			return 0, err
		}
	} else {
		// update
		newFile := &drive.File{Name: s.getFileName(path)}
		_, err = s.service.Files.Update(fileId, newFile).Context(ctx).Media(r).Do()
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}
