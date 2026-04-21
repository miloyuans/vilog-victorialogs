package mongostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vilog-victorialogs/internal/model"
)

func (s *Store) CreateQueryJob(ctx context.Context, job model.QueryJob) error {
	if _, err := s.collection(collectionQueryJobs).InsertOne(ctx, job); err != nil {
		return fmt.Errorf("insert query job: %w", err)
	}
	return nil
}

func (s *Store) UpdateQueryJob(ctx context.Context, job model.QueryJob) error {
	result, err := s.collection(collectionQueryJobs).ReplaceOne(ctx, bson.M{"_id": job.ID}, job)
	if err != nil {
		return fmt.Errorf("replace query job: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) PatchQueryJob(ctx context.Context, jobID string, update bson.M) error {
	_, err := s.collection(collectionQueryJobs).UpdateOne(ctx, bson.M{"_id": jobID}, update)
	if err != nil {
		return fmt.Errorf("update query job: %w", err)
	}
	return nil
}

func (s *Store) GetQueryJob(ctx context.Context, jobID string) (model.QueryJob, error) {
	var job model.QueryJob
	err := s.collection(collectionQueryJobs).FindOne(ctx, bson.M{"_id": jobID}).Decode(&job)
	if err == nil {
		return job, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.QueryJob{}, ErrNotFound
	}
	return model.QueryJob{}, fmt.Errorf("find query job: %w", err)
}

func (s *Store) ListExpiredQueryJobs(ctx context.Context, now time.Time, limit int64) ([]model.QueryJob, error) {
	opts := options.Find().SetSort(bson.D{{Key: "expires_at", Value: 1}})
	if limit > 0 {
		opts.SetLimit(limit)
	}
	cursor, err := s.collection(collectionQueryJobs).Find(ctx, bson.M{"expires_at": bson.M{"$lte": now.UTC()}}, opts)
	if err != nil {
		return nil, fmt.Errorf("find expired query jobs: %w", err)
	}
	defer cursor.Close(ctx)

	var jobs []model.QueryJob
	if err := cursor.All(ctx, &jobs); err != nil {
		return nil, fmt.Errorf("decode expired query jobs: %w", err)
	}
	return jobs, nil
}

func (s *Store) DeleteQueryJob(ctx context.Context, jobID string) error {
	if _, err := s.collection(collectionQuerySegments).DeleteMany(ctx, bson.M{"job_id": jobID}); err != nil {
		return fmt.Errorf("delete query segments: %w", err)
	}
	if _, err := s.collection(collectionQueryJobs).DeleteOne(ctx, bson.M{"_id": jobID}); err != nil {
		return fmt.Errorf("delete query job: %w", err)
	}
	return nil
}
