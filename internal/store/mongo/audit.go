package mongostore

import (
	"context"
	"fmt"

	"vilog-victorialogs/internal/model"
)

func (s *Store) CreateAuditLog(ctx context.Context, audit model.AuditLog) error {
	if _, err := s.collection(collectionAuditLogs).InsertOne(ctx, audit); err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	}
	return nil
}
