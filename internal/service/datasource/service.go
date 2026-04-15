package datasource

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/model"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store  *mongostore.Store
	client *victorialogs.Client
}

func New(store *mongostore.Store, client *victorialogs.Client) *Service {
	return &Service{
		store:  store,
		client: client,
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

	datasource, err := buildDatasource(id, req, existing.CreatedAt)
	if err != nil {
		return model.Datasource{}, err
	}
	datasource.UpdatedAt = time.Now().UTC()

	if err := s.store.UpdateDatasource(ctx, datasource); err != nil {
		return model.Datasource{}, err
	}
	_ = s.audit(ctx, "datasource", datasource.ID, "update", actor, map[string]any{"name": datasource.Name})
	return datasource, nil
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
		if fieldMapping.TimeField == "" {
			fieldMapping.TimeField = "_time"
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
