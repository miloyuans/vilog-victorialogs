package mongostore

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vilog-victorialogs/internal/model"
)

func (s *Store) ListDatasources(ctx context.Context, onlyEnabled bool) ([]model.Datasource, error) {
	filter := bson.M{}
	if onlyEnabled {
		filter["enabled"] = true
	}

	cursor, err := s.collection(collectionDatasources).Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("find datasources: %w", err)
	}
	defer cursor.Close(ctx)

	var datasources []model.Datasource
	if err := cursor.All(ctx, &datasources); err != nil {
		return nil, fmt.Errorf("decode datasources: %w", err)
	}
	return datasources, nil
}

func (s *Store) GetDatasource(ctx context.Context, id string) (model.Datasource, error) {
	var datasource model.Datasource
	err := s.collection(collectionDatasources).FindOne(ctx, bson.M{"_id": id}).Decode(&datasource)
	if err == nil {
		return datasource, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.Datasource{}, ErrNotFound
	}
	return model.Datasource{}, fmt.Errorf("find datasource: %w", err)
}

func (s *Store) CreateDatasource(ctx context.Context, datasource model.Datasource) error {
	if _, err := s.collection(collectionDatasources).InsertOne(ctx, datasource); err != nil {
		return fmt.Errorf("insert datasource: %w", err)
	}
	return nil
}

func (s *Store) UpdateDatasource(ctx context.Context, datasource model.Datasource) error {
	result, err := s.collection(collectionDatasources).ReplaceOne(ctx, bson.M{"_id": datasource.ID}, datasource)
	if err != nil {
		return fmt.Errorf("replace datasource: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteDatasource(ctx context.Context, id string) error {
	result, err := s.collection(collectionDatasources).DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("delete datasource: %w", err)
	}
	if result.DeletedCount == 0 {
		return ErrNotFound
	}

	cleanupTargets := []string{
		collectionServiceCatalog,
		collectionDatasourceTagSnapshots,
		collectionDatasourceRetentionBindings,
		collectionDeleteTasks,
	}
	for _, name := range cleanupTargets {
		if _, err := s.collection(name).DeleteMany(ctx, bson.M{"datasource_id": id}); err != nil {
			return fmt.Errorf("cleanup %s for datasource %s: %w", name, id, err)
		}
	}
	return nil
}
