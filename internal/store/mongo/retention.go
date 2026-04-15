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

func (s *Store) ListRetentionTemplates(ctx context.Context) ([]model.RetentionPolicyTemplate, error) {
	cursor, err := s.collection(collectionRetentionPolicyTemplates).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("find retention templates: %w", err)
	}
	defer cursor.Close(ctx)

	var templates []model.RetentionPolicyTemplate
	if err := cursor.All(ctx, &templates); err != nil {
		return nil, fmt.Errorf("decode retention templates: %w", err)
	}
	return templates, nil
}

func (s *Store) GetRetentionTemplate(ctx context.Context, id string) (model.RetentionPolicyTemplate, error) {
	var template model.RetentionPolicyTemplate
	err := s.collection(collectionRetentionPolicyTemplates).FindOne(ctx, bson.M{"_id": id}).Decode(&template)
	if err == nil {
		return template, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.RetentionPolicyTemplate{}, ErrNotFound
	}
	return model.RetentionPolicyTemplate{}, fmt.Errorf("find retention template: %w", err)
}

func (s *Store) CreateRetentionTemplate(ctx context.Context, template model.RetentionPolicyTemplate) error {
	if _, err := s.collection(collectionRetentionPolicyTemplates).InsertOne(ctx, template); err != nil {
		return fmt.Errorf("insert retention template: %w", err)
	}
	return nil
}

func (s *Store) UpdateRetentionTemplate(ctx context.Context, template model.RetentionPolicyTemplate) error {
	result, err := s.collection(collectionRetentionPolicyTemplates).ReplaceOne(ctx, bson.M{"_id": template.ID}, template)
	if err != nil {
		return fmt.Errorf("replace retention template: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListRetentionBindings(ctx context.Context) ([]model.DatasourceRetentionBinding, error) {
	cursor, err := s.collection(collectionDatasourceRetentionBindings).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "updated_at", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("find retention bindings: %w", err)
	}
	defer cursor.Close(ctx)

	var bindings []model.DatasourceRetentionBinding
	if err := cursor.All(ctx, &bindings); err != nil {
		return nil, fmt.Errorf("decode retention bindings: %w", err)
	}
	return bindings, nil
}

func (s *Store) ListRetentionBindingsByDatasource(ctx context.Context, datasourceID string, onlyEnabled bool) ([]model.DatasourceRetentionBinding, error) {
	filter := bson.M{"datasource_id": datasourceID}
	if onlyEnabled {
		filter["enabled"] = true
	}

	cursor, err := s.collection(collectionDatasourceRetentionBindings).Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "updated_at", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("find retention bindings by datasource: %w", err)
	}
	defer cursor.Close(ctx)

	var bindings []model.DatasourceRetentionBinding
	if err := cursor.All(ctx, &bindings); err != nil {
		return nil, fmt.Errorf("decode retention bindings by datasource: %w", err)
	}
	return bindings, nil
}

func (s *Store) GetRetentionBinding(ctx context.Context, id string) (model.DatasourceRetentionBinding, error) {
	var binding model.DatasourceRetentionBinding
	err := s.collection(collectionDatasourceRetentionBindings).FindOne(ctx, bson.M{"_id": id}).Decode(&binding)
	if err == nil {
		return binding, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.DatasourceRetentionBinding{}, ErrNotFound
	}
	return model.DatasourceRetentionBinding{}, fmt.Errorf("find retention binding: %w", err)
}

func (s *Store) CreateRetentionBinding(ctx context.Context, binding model.DatasourceRetentionBinding) error {
	if _, err := s.collection(collectionDatasourceRetentionBindings).InsertOne(ctx, binding); err != nil {
		return fmt.Errorf("insert retention binding: %w", err)
	}
	return nil
}

func (s *Store) UpdateRetentionBinding(ctx context.Context, binding model.DatasourceRetentionBinding) error {
	result, err := s.collection(collectionDatasourceRetentionBindings).ReplaceOne(ctx, bson.M{"_id": binding.ID}, binding)
	if err != nil {
		return fmt.Errorf("replace retention binding: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) CreateDeleteTask(ctx context.Context, task model.DeleteTask) error {
	if _, err := s.collection(collectionDeleteTasks).InsertOne(ctx, task); err != nil {
		return fmt.Errorf("insert delete task: %w", err)
	}
	return nil
}

func (s *Store) UpdateDeleteTask(ctx context.Context, task model.DeleteTask) error {
	result, err := s.collection(collectionDeleteTasks).ReplaceOne(ctx, bson.M{"_id": task.ID}, task)
	if err != nil {
		return fmt.Errorf("replace delete task: %w", err)
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) GetDeleteTask(ctx context.Context, id string) (model.DeleteTask, error) {
	var task model.DeleteTask
	err := s.collection(collectionDeleteTasks).FindOne(ctx, bson.M{"_id": id}).Decode(&task)
	if err == nil {
		return task, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return model.DeleteTask{}, ErrNotFound
	}
	return model.DeleteTask{}, fmt.Errorf("find delete task: %w", err)
}

func (s *Store) ListDeleteTasks(ctx context.Context) ([]model.DeleteTask, error) {
	cursor, err := s.collection(collectionDeleteTasks).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "started_at", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("find delete tasks: %w", err)
	}
	defer cursor.Close(ctx)

	var tasks []model.DeleteTask
	if err := cursor.All(ctx, &tasks); err != nil {
		return nil, fmt.Errorf("decode delete tasks: %w", err)
	}
	return tasks, nil
}

func (s *Store) HasActiveDeleteTask(ctx context.Context, datasourceID string) (bool, error) {
	count, err := s.collection(collectionDeleteTasks).CountDocuments(ctx, bson.M{
		"datasource_id": datasourceID,
		"status":        bson.M{"$in": []string{"queued", "running"}},
	})
	if err != nil {
		return false, fmt.Errorf("count active delete tasks: %w", err)
	}
	return count > 0, nil
}

func (s *Store) CountDeleteTasksSince(ctx context.Context, datasourceID string, since time.Time) (int64, error) {
	count, err := s.collection(collectionDeleteTasks).CountDocuments(ctx, bson.M{
		"datasource_id": datasourceID,
		"started_at":    bson.M{"$gte": since},
	})
	if err != nil {
		return 0, fmt.Errorf("count delete tasks since: %w", err)
	}
	return count, nil
}
