package tests

import (
	"os"
	"testing"

	"github.com/beyondstorage/go-service-gdrive"
	"github.com/beyondstorage/go-storage/v4/types"
	ps "github.com/beyondstorage/go-storage/v4/pairs"

	"github.com/google/uuid"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for gdrive")

	store, err := gdrive.NewStorager(
		ps.WithName(os.Getenv("STORAGE_GDRIVE_NAME")),
		ps.WithCredential(os.Getenv("STORAGE_GDRIVE_CREDENTIAL")),
		ps.WithWorkDir("/"+uuid.New().String()),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}

	t.Cleanup(func() {
		err = store.Delete("")
		if err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return store
}
