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

func (s *Store) ListTagDefinitions(ctx context.Context) ([]model.TagDefinition, error) {
	cursor, err := s.collection(collectionTagDefinitions).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("find tag definitions: %w", err)
	}
	defer cursor.Close(ctx)

	var tags []model.TagDefinition
	if err := cursor.All(ctx, &tags); err != nil {
		return nil, fmt.Errorf("decode tag definitions: %w", err)
	}
	return tags, nil
}

func (s *Store) CreateTagDefinition(ctx context.Context, tag model.TagDefinition) error {
	if _, err := s.collection(collectionTagDefinitions).InsertOne(ctx, tag); err != nil {
		return fmt.Errorf("insert tag definition: %w", err)
	}
	return nil
}

func (s *Store) UpdateTagDefinition(ctx context.Context, tag model.TagDefinition) error {
	result, err := s.collection(collectionTagDefinitions).ReplaceOne(ctx, bson.M{"_id": tag.ID}, tag)
	if err != nil {
		return fmt.Errorf("replace tag definition: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteTagDefinition(ctx context.Context, id string) error {
	result, err := s.collection(collectionTagDefinitions).DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("delete tag definition: %w", err)
	}
	if result.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) UpsertAutoTagDefinition(ctx context.Context, tag model.TagDefinition) error {
	update := bson.M{
		"$set": bson.M{
			"display_name":    tag.DisplayName,
			"ui_type":         tag.UIType,
			"multi":           tag.Multi,
			"enabled":         tag.Enabled,
			"auto_discovered": tag.AutoDiscovered,
			"priority":        tag.Priority,
			"updated_at":      tag.UpdatedAt,
		},
		"$addToSet": bson.M{
			"datasource_ids": bson.M{"$each": tag.DatasourceIDs},
		},
		"$setOnInsert": bson.M{
			"_id":           tag.ID,
			"name":          tag.Name,
			"field_name":    tag.FieldName,
			"service_names": []string{},
			"created_at":    tag.CreatedAt,
		},
	}

	_, err := s.collection(collectionTagDefinitions).UpdateOne(
		ctx,
		bson.M{"name": tag.Name, "field_name": tag.FieldName},
		update,
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert auto tag definition: %w", err)
	}
	return nil
}

func (s *Store) ReplaceServiceCatalog(ctx context.Context, datasourceID, serviceField string, services []string, ttl time.Duration) error {
	if _, err := s.collection(collectionServiceCatalog).DeleteMany(ctx, bson.M{"datasource_id": datasourceID}); err != nil {
		return fmt.Errorf("delete existing service catalog: %w", err)
	}
	if len(services) == 0 {
		return nil
	}

	now := time.Now().UTC()
	docs := make([]any, 0, len(services))
	for _, serviceName := range services {
		docs = append(docs, model.ServiceCatalogEntry{
			ID:           datasourceID + ":" + serviceName,
			DatasourceID: datasourceID,
			ServiceName:  serviceName,
			ServiceField: serviceField,
			LastSeenAt:   now,
			ExpireAt:     now.Add(ttl),
		})
	}

	if _, err := s.collection(collectionServiceCatalog).InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("insert service catalog: %w", err)
	}
	return nil
}

func (s *Store) ListServiceCatalog(ctx context.Context, datasourceID string) ([]model.ServiceCatalogEntry, error) {
	filter := bson.M{"expire_at": bson.M{"$gt": time.Now().UTC()}}
	if datasourceID != "" {
		filter["datasource_id"] = datasourceID
	}

	cursor, err := s.collection(collectionServiceCatalog).Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "service_name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("find service catalog: %w", err)
	}
	defer cursor.Close(ctx)

	var services []model.ServiceCatalogEntry
	if err := cursor.All(ctx, &services); err != nil {
		return nil, fmt.Errorf("decode service catalog: %w", err)
	}
	return services, nil
}

func (s *Store) UpsertSnapshot(ctx context.Context, snapshot model.DatasourceTagSnapshot) error {
	_, err := s.collection(collectionDatasourceTagSnapshots).UpdateOne(
		ctx,
		bson.M{"datasource_id": snapshot.DatasourceID},
		bson.M{
			"$set": bson.M{
				"datasource_id":           snapshot.DatasourceID,
				"discovered_at":           snapshot.DiscoveredAt,
				"service_field":           snapshot.ServiceField,
				"pod_field":               snapshot.PodField,
				"message_field":           snapshot.MessageField,
				"time_field":              snapshot.TimeField,
				"tag_candidates":          snapshot.TagCandidates,
				"high_cardinality_fields": snapshot.HighCardinalityFields,
				"notify_status":           snapshot.NotifyStatus,
			},
			"$setOnInsert": bson.M{
				"_id": snapshot.ID,
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert snapshot: %w", err)
	}
	return nil
}

func (s *Store) GetSnapshot(ctx context.Context, datasourceID string) (model.DatasourceTagSnapshot, error) {
	var snapshot model.DatasourceTagSnapshot
	err := s.collection(collectionDatasourceTagSnapshots).FindOne(ctx, bson.M{"datasource_id": datasourceID}).Decode(&snapshot)
	if err == nil {
		return snapshot, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.DatasourceTagSnapshot{}, ErrNotFound
	}
	return model.DatasourceTagSnapshot{}, fmt.Errorf("find snapshot: %w", err)
}

func (s *Store) ResetDatasourceDiscovery(ctx context.Context, datasourceID string) error {
	targets := []string{
		collectionServiceCatalog,
		collectionDatasourceTagSnapshots,
	}
	for _, target := range targets {
		if _, err := s.collection(target).DeleteMany(ctx, bson.M{"datasource_id": datasourceID}); err != nil {
			return fmt.Errorf("reset discovery %s for datasource %s: %w", target, datasourceID, err)
		}
	}
	return nil
}
