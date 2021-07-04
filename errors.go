package gdrive

import (
	"github.com/beyondstorage/go-storage/v4/services"
)

var (
	ErrRateLimitExceeded  = services.NewErrorCode("Rate limit exceeded")
	ErrBackEnd            = services.NewErrorCode("Backend Error")
	ErrInvalidCredentials = services.NewErrorCode("Invalid credentials")
)
