package mongostore

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vilog-victorialogs/internal/model"
)

func (s *Store) CreateQuerySegment(ctx context.Context, segment model.QuerySegment) error {
	if _, err := s.collection(collectionQuerySegments).InsertOne(ctx, segment); err != nil {
		return fmt.Errorf("insert query segment: %w", err)
	}
	return nil
}

func (s *Store) ListQuerySegments(ctx context.Context, jobID string) ([]model.QuerySegment, error) {
	cursor, err := s.collection(collectionQuerySegments).Find(
		ctx,
		bson.M{"job_id": jobID},
		options.Find().SetSort(bson.D{{Key: "sequence", Value: 1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("find query segments: %w", err)
	}
	defer cursor.Close(ctx)

	var segments []model.QuerySegment
	if err := cursor.All(ctx, &segments); err != nil {
		return nil, fmt.Errorf("decode query segments: %w", err)
	}
	return segments, nil
}
