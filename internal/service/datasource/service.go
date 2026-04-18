package datasource

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	cachesvc "vilog-victorialogs/internal/service/cache"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store  *mongostore.Store
	cache  *cachesvc.Service
	client *victorialogs.Client
	logger *zap.Logger
}

func New(store *mongostore.Store, cacheService *cachesvc.Service, client *victorialogs.Client, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Service{
		store:  store,
		cache:  cacheService,
		client: client,
		logger: logger,
	}
}

func (s *Service) List(ctx context.Context) ([]model.Datasource, error) {
	return s.store.ListDatasources(ctx, false)
}

func (s *Service) ListEnabled(ctx context.Context) ([]model.Datasource, error) {
	return s.store.ListDatasources(ctx, true)
}

func (s *Service) Get(ctx context.Context, id string) (model.Datasource, error) {
	return s.store.GetDatasource(ctx, id)
}

func (s *Service) Create(ctx context.Context, req model.DatasourceUpsertRequest, actor string) (model.Datasource, error) {
	datasource, err := buildDatasource("", req, time.Time{})
	if err != nil {
		return model.Datasource{}, err
	}
	now := time.Now().UTC()
	datasource.ID = util.NewPrefixedID("ds")
	datasource.CreatedAt = now
	datasource.UpdatedAt = now

	if err := s.store.CreateDatasource(ctx, datasource); err != nil {
		return model.Datasource{}, err
	}
	_ = s.audit(ctx, "datasource", datasource.ID, "create", actor, map[string]any{"name": datasource.Name})
	return datasource, nil
}

func (s *Service) Update(ctx context.Context, id string, req model.DatasourceUpsertRequest, actor string) (model.Datasource, error) {
	existing, err := s.store.GetDatasource(ctx, id)
	if err != nil {
		return model.Datasource{}, err
	}

	datasource, err := buildDatasource(id, mergeDatasourceRequest(existing, req), existing.CreatedAt)
	if err != nil {
		return model.Datasource{}, err
	}
	datasource.UpdatedAt = time.Now().UTC()

	if err := s.store.UpdateDatasource(ctx, datasource); err != nil {
		return model.Datasource{}, err
	}
	if datasourceRuntimeChanged(existing, datasource) {
		s.invalidateDatasourceRuntimeState(ctx, datasource, "update")
	}
	_ = s.audit(ctx, "datasource", datasource.ID, "update", actor, map[string]any{"name": datasource.Name})
	return datasource, nil
}

func (s *Service) Delete(ctx context.Context, id, actor string) error {
	existing, err := s.store.GetDatasource(ctx, id)
	if err != nil {
		return err
	}
	if err := s.store.DeleteDatasource(ctx, id); err != nil {
		return err
	}
	s.invalidateDatasourceRuntimeState(ctx, existing, "delete")
	_ = s.audit(ctx, "datasource", id, "delete", actor, map[string]any{"name": existing.Name})
	return nil
}

func (s *Service) SyncConfigured(ctx context.Context, items []config.ConfiguredDatasource) error {
	for _, item := range items {
		id := configuredDatasourceID(item)
		req := model.DatasourceUpsertRequest{
			Name:           item.Name,
			BaseURL:        item.BaseURL,
			Enabled:        boolPtr(item.Enabled),
			TimeoutSeconds: item.TimeoutSeconds,
			Headers:        item.Headers,
			QueryPaths:     item.QueryPaths,
			FieldMapping:   item.FieldMapping,
			SupportsDelete: boolPtr(item.SupportsDelete),
		}

		existing, err := s.store.GetDatasource(ctx, id)
		if err != nil && !errors.Is(err, mongostore.ErrNotFound) {
			return fmt.Errorf("load configured datasource %s: %w", id, err)
		}

		createdAt := existing.CreatedAt
		datasource, err := buildDatasource(id, req, createdAt)
		if err != nil {
			return fmt.Errorf("build configured datasource %s: %w", id, err)
		}
		now := time.Now().UTC()
		if createdAt.IsZero() {
			datasource.CreatedAt = now
		}
		datasource.UpdatedAt = now

		if existing.ID == "" {
			if err := s.store.CreateDatasource(ctx, datasource); err != nil {
				return fmt.Errorf("create configured datasource %s: %w", id, err)
			}
			s.logger.Info("configured datasource synced",
				zap.String("id", datasource.ID),
				zap.String("name", datasource.Name),
				zap.String("mode", "created"),
			)
			continue
		}

		if err := s.store.UpdateDatasource(ctx, datasource); err != nil {
			return fmt.Errorf("update configured datasource %s: %w", id, err)
		}
		if datasourceRuntimeChanged(existing, datasource) {
			s.invalidateDatasourceRuntimeState(ctx, datasource, "config_sync")
		}
		s.logger.Info("configured datasource synced",
			zap.String("id", datasource.ID),
			zap.String("name", datasource.Name),
			zap.String("mode", "updated"),
		)
	}
	return nil
}

func mergeDatasourceRequest(existing model.Datasource, req model.DatasourceUpsertRequest) model.DatasourceUpsertRequest {
	merged := req
	if strings.TrimSpace(merged.Name) == "" {
		merged.Name = existing.Name
	}
	if strings.TrimSpace(merged.BaseURL) == "" {
		merged.BaseURL = existing.BaseURL
	}
	if merged.Enabled == nil {
		enabled := existing.Enabled
		merged.Enabled = &enabled
	}
	if merged.TimeoutSeconds <= 0 {
		merged.TimeoutSeconds = existing.TimeoutSeconds
	}
	if merged.SupportsDelete == nil {
		supportsDelete := existing.SupportsDelete
		merged.SupportsDelete = &supportsDelete
	}
	if merged.Headers == (model.DatasourceHeaders{}) {
		merged.Headers = existing.Headers
	}

	if merged.QueryPaths == (model.DatasourceQueryPaths{}) {
		merged.QueryPaths = existing.QueryPaths
	} else {
		if merged.QueryPaths.Query == "" {
			merged.QueryPaths.Query = existing.QueryPaths.Query
		}
		if merged.QueryPaths.FieldNames == "" {
			merged.QueryPaths.FieldNames = existing.QueryPaths.FieldNames
		}
		if merged.QueryPaths.FieldValues == "" {
			merged.QueryPaths.FieldValues = existing.QueryPaths.FieldValues
		}
		if merged.QueryPaths.StreamFieldNames == "" {
			merged.QueryPaths.StreamFieldNames = existing.QueryPaths.StreamFieldNames
		}
		if merged.QueryPaths.StreamFieldValues == "" {
			merged.QueryPaths.StreamFieldValues = existing.QueryPaths.StreamFieldValues
		}
		if merged.QueryPaths.Facets == "" {
			merged.QueryPaths.Facets = existing.QueryPaths.Facets
		}
		if merged.QueryPaths.DeleteRunTask == "" {
			merged.QueryPaths.DeleteRunTask = existing.QueryPaths.DeleteRunTask
		}
		if merged.QueryPaths.DeleteActiveTasks == "" {
			merged.QueryPaths.DeleteActiveTasks = existing.QueryPaths.DeleteActiveTasks
		}
		if merged.QueryPaths.DeleteStopTask == "" {
			merged.QueryPaths.DeleteStopTask = existing.QueryPaths.DeleteStopTask
		}
	}

	if merged.FieldMapping == (model.DatasourceFieldMapping{}) {
		merged.FieldMapping = existing.FieldMapping
	} else {
		if merged.FieldMapping.ServiceField == "" {
			merged.FieldMapping.ServiceField = existing.FieldMapping.ServiceField
		}
		if merged.FieldMapping.PodField == "" {
			merged.FieldMapping.PodField = existing.FieldMapping.PodField
		}
		if merged.FieldMapping.MessageField == "" {
			merged.FieldMapping.MessageField = existing.FieldMapping.MessageField
		}
		if merged.FieldMapping.TimeField == "" {
			merged.FieldMapping.TimeField = existing.FieldMapping.TimeField
		}
	}

	return merged
}

func (s *Service) Test(ctx context.Context, id string) (model.DatasourceTestResponse, error) {
	datasource, err := s.store.GetDatasource(ctx, id)
	if err != nil {
		return model.DatasourceTestResponse{}, err
	}
	if err := s.client.Ping(ctx, datasource); err != nil {
		return model.DatasourceTestResponse{
			OK:      false,
			Message: err.Error(),
		}, nil
	}
	return model.DatasourceTestResponse{
		OK:      true,
		Message: "connectivity check passed",
	}, nil
}

func (s *Service) audit(ctx context.Context, resourceType, resourceID, action, actor string, payload map[string]any) error {
	return s.store.CreateAuditLog(ctx, model.AuditLog{
		ID:           util.NewPrefixedID("audit"),
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Action:       action,
		Actor:        actor,
		Payload:      payload,
		CreatedAt:    time.Now().UTC(),
	})
}

func buildDatasource(id string, req model.DatasourceUpsertRequest, createdAt time.Time) (model.Datasource, error) {
	if strings.TrimSpace(req.Name) == "" {
		return model.Datasource{}, fmt.Errorf("name is required")
	}
	if strings.TrimSpace(req.BaseURL) == "" {
		return model.Datasource{}, fmt.Errorf("base_url is required")
	}
	if _, err := url.ParseRequestURI(strings.TrimSpace(req.BaseURL)); err != nil {
		return model.Datasource{}, fmt.Errorf("invalid base_url: %w", err)
	}

	queryPaths := model.DefaultDatasourceQueryPaths()
	if req.QueryPaths.Query != "" {
		queryPaths.Query = req.QueryPaths.Query
	}
	if req.QueryPaths.FieldNames != "" {
		queryPaths.FieldNames = req.QueryPaths.FieldNames
	}
	if req.QueryPaths.FieldValues != "" {
		queryPaths.FieldValues = req.QueryPaths.FieldValues
	}
	if req.QueryPaths.StreamFieldNames != "" {
		queryPaths.StreamFieldNames = req.QueryPaths.StreamFieldNames
	}
	if req.QueryPaths.StreamFieldValues != "" {
		queryPaths.StreamFieldValues = req.QueryPaths.StreamFieldValues
	}
	if req.QueryPaths.Facets != "" {
		queryPaths.Facets = req.QueryPaths.Facets
	}
	if req.QueryPaths.DeleteRunTask != "" {
		queryPaths.DeleteRunTask = req.QueryPaths.DeleteRunTask
	}
	if req.QueryPaths.DeleteActiveTasks != "" {
		queryPaths.DeleteActiveTasks = req.QueryPaths.DeleteActiveTasks
	}
	if req.QueryPaths.DeleteStopTask != "" {
		queryPaths.DeleteStopTask = req.QueryPaths.DeleteStopTask
	}

	fieldMapping := model.DefaultDatasourceFieldMapping()
	if req.FieldMapping.TimeField != "" || req.FieldMapping.ServiceField != "" || req.FieldMapping.PodField != "" || req.FieldMapping.MessageField != "" {
		fieldMapping = req.FieldMapping
		defaults := model.DefaultDatasourceFieldMapping()
		if strings.TrimSpace(fieldMapping.ServiceField) == "" {
			fieldMapping.ServiceField = defaults.ServiceField
		}
		if strings.TrimSpace(fieldMapping.PodField) == "" {
			fieldMapping.PodField = defaults.PodField
		}
		if strings.TrimSpace(fieldMapping.MessageField) == "" {
			fieldMapping.MessageField = defaults.MessageField
		}
		if strings.TrimSpace(fieldMapping.TimeField) == "" {
			fieldMapping.TimeField = defaults.TimeField
		}
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	supportsDelete := false
	if req.SupportsDelete != nil {
		supportsDelete = *req.SupportsDelete
	}

	timeoutSeconds := req.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}

	return model.Datasource{
		ID:             id,
		Name:           strings.TrimSpace(req.Name),
		BaseURL:        strings.TrimRight(strings.TrimSpace(req.BaseURL), "/"),
		Enabled:        enabled,
		TimeoutSeconds: timeoutSeconds,
		Headers:        req.Headers,
		QueryPaths:     queryPaths,
		FieldMapping:   fieldMapping,
		SupportsDelete: supportsDelete,
		CreatedAt:      createdAt,
	}, nil
}

var configuredDatasourceIDSanitizer = regexp.MustCompile(`[^a-z0-9]+`)

func configuredDatasourceID(item config.ConfiguredDatasource) string {
	if trimmed := strings.TrimSpace(item.ID); trimmed != "" {
		return trimmed
	}
	slug := strings.ToLower(strings.TrimSpace(item.Name))
	slug = configuredDatasourceIDSanitizer.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "datasource"
	}
	return "cfg_" + slug
}

func boolPtr(value bool) *bool {
	return &value
}

func datasourceRuntimeChanged(before, after model.Datasource) bool {
	return before.Name != after.Name ||
		before.BaseURL != after.BaseURL ||
		before.Enabled != after.Enabled ||
		before.TimeoutSeconds != after.TimeoutSeconds ||
		before.SupportsDelete != after.SupportsDelete ||
		before.Headers != after.Headers ||
		before.QueryPaths != after.QueryPaths ||
		before.FieldMapping != after.FieldMapping
}

func (s *Service) invalidateDatasourceRuntimeState(ctx context.Context, datasource model.Datasource, reason string) {
	if err := s.store.ResetDatasourceDiscovery(ctx, datasource.ID); err != nil {
		s.logger.Warn("reset datasource discovery state failed",
			zap.Error(err),
			zap.String("datasource_id", datasource.ID),
			zap.String("reason", reason),
		)
	}
	if err := s.store.DeleteCacheEntriesByPrefix(ctx, cachesvc.KindServiceList, datasource.ID); err != nil {
		s.logger.Warn("clear datasource service-list cache failed",
			zap.Error(err),
			zap.String("datasource_id", datasource.ID),
			zap.String("reason", reason),
		)
	}
	if err := s.store.DeleteCacheEntriesByPrefix(ctx, cachesvc.KindTagValues, datasource.ID+":"); err != nil {
		s.logger.Warn("clear datasource tag-values cache failed",
			zap.Error(err),
			zap.String("datasource_id", datasource.ID),
			zap.String("reason", reason),
		)
	}
	if s.cache != nil {
		if err := s.cache.PurgeDatasourceArtifacts(datasource.ID); err != nil {
			s.logger.Warn("purge datasource local cache failed",
				zap.Error(err),
				zap.String("datasource_id", datasource.ID),
				zap.String("reason", reason),
			)
		}
	}
	s.logger.Info("datasource runtime state invalidated",
		zap.String("datasource_id", datasource.ID),
		zap.String("datasource_name", datasource.Name),
		zap.String("reason", reason),
	)
}
