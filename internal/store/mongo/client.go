package mongostore

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"vilog-victorialogs/internal/config"
)

const (
	collectionDatasources                 = "datasources"
	collectionTagDefinitions              = "tag_definitions"
	collectionServiceCatalog              = "service_catalog"
	collectionDatasourceTagSnapshots      = "datasource_tag_snapshots"
	collectionQueryCache                  = "query_cache"
	collectionQueryJobs                   = "query_jobs"
	collectionQuerySegments               = "query_segments"
	collectionRetentionPolicyTemplates    = "retention_policy_templates"
	collectionDatasourceRetentionBindings = "datasource_retention_bindings"
	collectionDeleteTasks                 = "delete_tasks"
	collectionAuditLogs                   = "audit_logs"
)

type Store struct {
	client   *mongo.Client
	database *mongo.Database
}

func New(ctx context.Context, cfg config.MongoConfig) (*Store, error) {
	clientOptions := options.Client().
		ApplyURI(cfg.URI).
		SetConnectTimeout(cfg.ConnectTimeout)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
	defer cancel()

	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping mongo: %w", err)
	}

	return &Store{
		client:   client,
		database: client.Database(cfg.Database),
	}, nil
}

func (s *Store) Name() string {
	return "mongo"
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx, readpref.Primary())
}

func (s *Store) Database() *mongo.Database {
	return s.database
}

func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *Store) InitIndexes(ctx context.Context) error {
	indexSpecs := map[string][]mongo.IndexModel{
		collectionDatasources: {
			{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		collectionTagDefinitions: {
			{Keys: bson.D{{Key: "name", Value: 1}, {Key: "field_name", Value: 1}}},
		},
		collectionServiceCatalog: {
			{Keys: bson.D{{Key: "datasource_id", Value: 1}, {Key: "service_name", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "expire_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
		},
		collectionDatasourceTagSnapshots: {
			{Keys: bson.D{{Key: "datasource_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		collectionQueryCache: {
			{Keys: bson.D{{Key: "kind", Value: 1}, {Key: "cache_key", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "expire_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
		},
		collectionQueryJobs: {
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "started_at", Value: -1}}},
			{Keys: bson.D{{Key: "expires_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0)},
			{Keys: bson.D{{Key: "started_at", Value: -1}}},
		},
		collectionQuerySegments: {
			{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "sequence", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "created_at", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: 1}}},
		},
		collectionRetentionPolicyTemplates: {
			{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		collectionDatasourceRetentionBindings: {
			{Keys: bson.D{{Key: "datasource_id", Value: 1}}},
			{Keys: bson.D{{Key: "policy_template_id", Value: 1}}},
		},
		collectionDeleteTasks: {
			{Keys: bson.D{{Key: "datasource_id", Value: 1}, {Key: "status", Value: 1}}},
			{Keys: bson.D{{Key: "task_id", Value: 1}}},
			{Keys: bson.D{{Key: "started_at", Value: -1}}},
		},
		collectionAuditLogs: {
			{Keys: bson.D{{Key: "resource_type", Value: 1}, {Key: "created_at", Value: -1}}},
			{Keys: bson.D{{Key: "resource_id", Value: 1}, {Key: "created_at", Value: -1}}},
		},
	}

	for name, indexes := range indexSpecs {
		if len(indexes) == 0 {
			continue
		}
		if _, err := s.collection(name).Indexes().CreateMany(ctx, indexes); err != nil {
			return fmt.Errorf("create indexes for %s: %w", name, err)
		}
	}

	return nil
}

func (s *Store) collection(name string) *mongo.Collection {
	return s.database.Collection(name)
}
