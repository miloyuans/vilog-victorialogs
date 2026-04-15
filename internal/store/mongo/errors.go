package mongostore

import "errors"

var ErrNotFound = errors.New("mongo: document not found")
