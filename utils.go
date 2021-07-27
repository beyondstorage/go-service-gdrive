package gdrive

import (
	"context"
	"fmt"
	"strings"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/services"
	"github.com/beyondstorage/go-storage/v4/types"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Storage is the example client.
type Storage struct {
	name         string
	workDir      string
	service      *drive.Service
	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	types.UnimplementedStorager
	types.UnimplementedDirer
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf(
		"Storager gdrive {Name: %s, WorkDir: %s}",
		s.name, s.workDir,
	)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...types.Pair) (types.Storager, error) {
	return newStorager(pairs...)
}

func newStorager(pairs ...types.Pair) (store *Storage, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()

	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return nil, err
	}

	store = &Storage{
		name:    opt.Name,
		workDir: "/",
	}
	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}
	cred, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}

	//TODO: To make it easier, we just support authorized it
	//via API key, maybe we can support OAuth in the future.
	var token string
	switch cred.Protocol() {
	case credential.ProtocolAPIKey:
		token = cred.APIKey()
	default:
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}

	ctx := context.Background()
	store.service, err = drive.NewService(ctx, option.WithAPIKey(token))
	return store, nil
}

func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	e, ok := err.(*googleapi.Error)
	if !ok {
		return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
	}

	//According to the docs, errors with the same error code may have
	//multiple causes, to determine the specific type of error,
	//we should evaluate the reason filed of the returned JSON
	//Ref: https://developers.google.com/drive/api/v3/handle-errors
	switch e.Errors[0].Reason {
	case "authError":
		return fmt.Errorf("%w: %v", credential.ErrInvalidValue, err)
	case "dailyLimitExceeded", "rateLimitExceeded", "userRateLimitExceeded":
		return fmt.Errorf("%w: %v", services.ErrRequestThrottled, err)
	case "backendError":
		return fmt.Errorf("%w: %v", services.ErrServiceInternal, err)
	case "notFound":
		return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	case "insufficientFilePermissions", "appNotAuthorizedToFile":
		return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
	default:
		return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
	}
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

func (s *Storage) newObject(done bool) *types.Object {
	return types.NewObject(s, done)
}

// getAbsPath will calculate object storage's abs path
func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

// getRelPath will get object storage's rel path.
func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

// getFileName will get a file's name without path
func (s *Storage) getFileName(path string) string {
	if strings.Contains(path, "/") {
		tmp := strings.Split(path, "/")
		return tmp[len(tmp)-1]
	} else {
		return path
	}
}
