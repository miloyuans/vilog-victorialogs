package retention

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"vilog-victorialogs/internal/client/victorialogs"
	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
	mongostore "vilog-victorialogs/internal/store/mongo"
	"vilog-victorialogs/internal/util"
)

type Service struct {
	store  *mongostore.Store
	client *victorialogs.Client
	cfg    config.RetentionConfig
}

func New(store *mongostore.Store, client *victorialogs.Client, cfg config.RetentionConfig) *Service {
	return &Service{
		store:  store,
		client: client,
		cfg:    cfg,
	}
}

func (s *Service) ListTemplates(ctx context.Context) ([]model.RetentionPolicyTemplate, error) {
	return s.store.ListRetentionTemplates(ctx)
}

func (s *Service) CreateTemplate(ctx context.Context, req model.RetentionPolicyTemplateUpsertRequest, actor string) (model.RetentionPolicyTemplate, error) {
	template, err := buildTemplate("", req, time.Time{})
	if err != nil {
		return model.RetentionPolicyTemplate{}, err
	}
	now := time.Now().UTC()
	template.ID = util.NewPrefixedID("tpl")
	template.CreatedAt = now
	template.UpdatedAt = now

	if err := s.store.CreateRetentionTemplate(ctx, template); err != nil {
		return model.RetentionPolicyTemplate{}, err
	}
	_ = s.audit(ctx, "retention_template", template.ID, "create", actor, map[string]any{"name": template.Name})
	return template, nil
}

func (s *Service) UpdateTemplate(ctx context.Context, id string, req model.RetentionPolicyTemplateUpsertRequest, actor string) (model.RetentionPolicyTemplate, error) {
	existing, err := s.store.GetRetentionTemplate(ctx, id)
	if err != nil {
		return model.RetentionPolicyTemplate{}, err
	}
	template, err := buildTemplate(id, req, existing.CreatedAt)
	if err != nil {
		return model.RetentionPolicyTemplate{}, err
	}
	template.UpdatedAt = time.Now().UTC()

	if err := s.store.UpdateRetentionTemplate(ctx, template); err != nil {
		return model.RetentionPolicyTemplate{}, err
	}
	_ = s.audit(ctx, "retention_template", template.ID, "update", actor, map[string]any{"name": template.Name})
	return template, nil
}

func (s *Service) ListBindings(ctx context.Context) ([]model.DatasourceRetentionBinding, error) {
	return s.store.ListRetentionBindings(ctx)
}

func (s *Service) CreateBinding(ctx context.Context, req model.DatasourceRetentionBindingUpsertRequest, actor string) (model.DatasourceRetentionBinding, error) {
	binding, err := s.buildBinding(ctx, "", req, time.Time{})
	if err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	now := time.Now().UTC()
	binding.ID = util.NewPrefixedID("bind")
	binding.CreatedAt = now
	binding.UpdatedAt = now

	if err := s.store.CreateRetentionBinding(ctx, binding); err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	_ = s.audit(ctx, "retention_binding", binding.ID, "create", actor, map[string]any{"datasource_id": binding.DatasourceID})
	return binding, nil
}

func (s *Service) UpdateBinding(ctx context.Context, id string, req model.DatasourceRetentionBindingUpsertRequest, actor string) (model.DatasourceRetentionBinding, error) {
	existing, err := s.store.GetRetentionBinding(ctx, id)
	if err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	binding, err := s.buildBinding(ctx, id, req, existing.CreatedAt)
	if err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	binding.LastRunAt = existing.LastRunAt
	binding.LastTaskID = existing.LastTaskID
	binding.LastStatus = existing.LastStatus
	binding.UpdatedAt = time.Now().UTC()

	if err := s.store.UpdateRetentionBinding(ctx, binding); err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	_ = s.audit(ctx, "retention_binding", binding.ID, "update", actor, map[string]any{"datasource_id": binding.DatasourceID})
	return binding, nil
}

func (s *Service) RunDatasource(ctx context.Context, datasourceID, actor string) (model.RetentionRunResponse, error) {
	bindings, err := s.store.ListRetentionBindingsByDatasource(ctx, datasourceID, true)
	if err != nil {
		return model.RetentionRunResponse{}, err
	}
	if len(bindings) == 0 {
		return model.RetentionRunResponse{}, fmt.Errorf("no enabled retention bindings for datasource %s", datasourceID)
	}

	task, err := s.RunBinding(ctx, bindings[0].ID, actor)
	if err != nil {
		return model.RetentionRunResponse{}, err
	}
	return model.RetentionRunResponse{
		DatasourceID: datasourceID,
		Tasks:        []model.DeleteTask{task},
	}, nil
}

func (s *Service) RunBinding(ctx context.Context, bindingID, actor string) (model.DeleteTask, error) {
	binding, err := s.store.GetRetentionBinding(ctx, bindingID)
	if err != nil {
		return model.DeleteTask{}, err
	}
	template, err := s.store.GetRetentionTemplate(ctx, binding.PolicyTemplateID)
	if err != nil {
		return model.DeleteTask{}, err
	}
	datasource, err := s.store.GetDatasource(ctx, binding.DatasourceID)
	if err != nil {
		return model.DeleteTask{}, err
	}
	snapshot, _ := s.store.GetSnapshot(ctx, datasource.ID)

	if !binding.Enabled || !template.Enabled {
		return model.DeleteTask{}, fmt.Errorf("binding or template is disabled")
	}
	if !datasource.SupportsDelete {
		return model.DeleteTask{}, fmt.Errorf("datasource %s does not allow delete tasks", datasource.Name)
	}

	active, err := s.store.HasActiveDeleteTask(ctx, datasource.ID)
	if err != nil {
		return model.DeleteTask{}, err
	}
	if active {
		return model.DeleteTask{}, fmt.Errorf("datasource %s already has an active delete task", datasource.Name)
	}

	countToday, err := s.store.CountDeleteTasksSince(ctx, datasource.ID, time.Now().UTC().Truncate(24*time.Hour))
	if err != nil {
		return model.DeleteTask{}, err
	}
	if countToday >= int64(s.cfg.MaxDeleteTasksPerDay) {
		return model.DeleteTask{}, fmt.Errorf("max delete tasks per day reached for datasource %s", datasource.Name)
	}

	filter, err := s.buildDeleteFilter(ctx, datasource, snapshot, template, binding)
	if err != nil {
		return model.DeleteTask{}, err
	}

	task := model.DeleteTask{
		ID:           util.NewPrefixedID("task"),
		DatasourceID: datasource.ID,
		BindingID:    binding.ID,
		Filter:       filter,
		Status:       "queued",
		StartedAt:    time.Now().UTC(),
	}
	if err := s.store.CreateDeleteTask(ctx, task); err != nil {
		return model.DeleteTask{}, err
	}

	taskID, err := s.client.RunDeleteTask(ctx, datasource, filter)
	if err != nil {
		task.Status = "failed"
		task.ErrorMsg = err.Error()
		finishedAt := time.Now().UTC()
		task.FinishedAt = &finishedAt
		_ = s.store.UpdateDeleteTask(ctx, task)
		return model.DeleteTask{}, err
	}

	task.TaskID = taskID
	task.Status = "running"
	if err := s.store.UpdateDeleteTask(ctx, task); err != nil {
		return model.DeleteTask{}, err
	}

	now := time.Now().UTC()
	binding.LastRunAt = &now
	binding.LastTaskID = task.ID
	binding.LastStatus = task.Status
	_ = s.store.UpdateRetentionBinding(ctx, binding)
	_ = s.audit(ctx, "delete_task", task.ID, "run", actor, map[string]any{"datasource_id": datasource.ID, "binding_id": binding.ID})

	go s.pollTask(task.ID, datasource)

	return task, nil
}

func (s *Service) ListTasks(ctx context.Context) ([]model.DeleteTask, error) {
	return s.store.ListDeleteTasks(ctx)
}

func (s *Service) StopTask(ctx context.Context, id, actor string) error {
	task, err := s.store.GetDeleteTask(ctx, id)
	if err != nil {
		return err
	}
	datasource, err := s.store.GetDatasource(ctx, task.DatasourceID)
	if err != nil {
		return err
	}

	if task.TaskID != "" {
		if err := s.client.StopDeleteTask(ctx, datasource, task.TaskID); err != nil {
			return err
		}
	}

	now := time.Now().UTC()
	task.Status = "stopped"
	task.FinishedAt = &now
	task.ErrorMsg = ""
	if err := s.store.UpdateDeleteTask(ctx, task); err != nil {
		return err
	}
	_ = s.audit(ctx, "delete_task", task.ID, "stop", actor, map[string]any{"datasource_id": datasource.ID})
	return nil
}

func (s *Service) buildBinding(ctx context.Context, id string, req model.DatasourceRetentionBindingUpsertRequest, createdAt time.Time) (model.DatasourceRetentionBinding, error) {
	if strings.TrimSpace(req.DatasourceID) == "" || strings.TrimSpace(req.PolicyTemplateID) == "" {
		return model.DatasourceRetentionBinding{}, fmt.Errorf("datasource_id and policy_template_id are required")
	}
	if _, err := s.store.GetDatasource(ctx, req.DatasourceID); err != nil {
		return model.DatasourceRetentionBinding{}, err
	}
	if _, err := s.store.GetRetentionTemplate(ctx, req.PolicyTemplateID); err != nil {
		return model.DatasourceRetentionBinding{}, err
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	return model.DatasourceRetentionBinding{
		ID:               id,
		DatasourceID:     req.DatasourceID,
		PolicyTemplateID: req.PolicyTemplateID,
		Enabled:          enabled,
		ServiceScope:     uniqueStrings(req.ServiceScope),
		TagScope:         req.TagScope,
		CreatedAt:        createdAt,
	}, nil
}

func buildTemplate(id string, req model.RetentionPolicyTemplateUpsertRequest, createdAt time.Time) (model.RetentionPolicyTemplate, error) {
	if strings.TrimSpace(req.Name) == "" {
		return model.RetentionPolicyTemplate{}, fmt.Errorf("name is required")
	}
	if req.RetentionDays <= 0 {
		return model.RetentionPolicyTemplate{}, fmt.Errorf("retention_days must be positive")
	}
	if err := validateCron(req.Cron); err != nil {
		return model.RetentionPolicyTemplate{}, err
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	return model.RetentionPolicyTemplate{
		ID:            id,
		Name:          req.Name,
		RetentionDays: req.RetentionDays,
		Cron:          req.Cron,
		Enabled:       enabled,
		CreatedAt:     createdAt,
	}, nil
}

func validateCron(expr string) error {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(expr); err != nil {
		return fmt.Errorf("invalid cron: %w", err)
	}
	return nil
}

func (s *Service) buildDeleteFilter(ctx context.Context, datasource model.Datasource, snapshot model.DatasourceTagSnapshot, template model.RetentionPolicyTemplate, binding model.DatasourceRetentionBinding) (string, error) {
	if template.RetentionDays > s.cfg.MaxDeleteRangeDays {
		return "", fmt.Errorf("retention_days exceeds max_delete_range_days")
	}

	tags, err := s.store.ListTagDefinitions(ctx)
	if err != nil {
		return "", err
	}

	cutoff := time.Now().UTC().Add(-time.Duration(template.RetentionDays) * 24 * time.Hour).Format(time.RFC3339)
	parts := []string{fmt.Sprintf("_time:<%s", cutoff)}

	serviceField := firstNonEmpty(snapshot.ServiceField, datasource.FieldMapping.ServiceField)
	if serviceField != "" && len(binding.ServiceScope) > 0 {
		if filter := buildExactFieldFilter(serviceField, binding.ServiceScope); filter != "" {
			parts = append(parts, filter)
		}
	}
	for tagName, values := range binding.TagScope {
		fieldName := resolveTagField(datasource.ID, tagName, tags)
		if fieldName == "" {
			fieldName = tagName
		}
		if filter := buildExactFieldFilter(fieldName, values); filter != "" {
			parts = append(parts, filter)
		}
	}

	return strings.Join(parts, " "), nil
}

func (s *Service) pollTask(localTaskID string, datasource model.Datasource) {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	ctx := context.Background()
	for range ticker.C {
		task, err := s.store.GetDeleteTask(ctx, localTaskID)
		if err != nil {
			return
		}
		if task.Status == "stopped" || task.Status == "failed" || task.Status == "done" {
			return
		}

		activeTasks, err := s.client.ActiveDeleteTasks(ctx, datasource)
		if err != nil {
			now := time.Now().UTC()
			task.Status = "failed"
			task.ErrorMsg = err.Error()
			task.FinishedAt = &now
			_ = s.store.UpdateDeleteTask(ctx, task)
			return
		}

		if !remoteTaskIsActive(activeTasks, task.TaskID) {
			now := time.Now().UTC()
			task.Status = "done"
			task.FinishedAt = &now
			_ = s.store.UpdateDeleteTask(ctx, task)

			binding, err := s.store.GetRetentionBinding(ctx, task.BindingID)
			if err == nil {
				binding.LastTaskID = task.ID
				binding.LastStatus = task.Status
				binding.LastRunAt = &task.StartedAt
				binding.UpdatedAt = now
				_ = s.store.UpdateRetentionBinding(ctx, binding)
			}
			return
		}
	}
}

func remoteTaskIsActive(tasks []victorialogs.ActiveDeleteTask, taskID string) bool {
	for _, task := range tasks {
		if task.TaskID == taskID {
			return true
		}
	}
	return false
}

func buildExactFieldFilter(field string, values []string) string {
	parts := make([]string, 0, len(values))
	for _, value := range uniqueStrings(values) {
		parts = append(parts, fmt.Sprintf(`%s:=%q`, field, value))
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func resolveTagField(datasourceID, tagName string, tags []model.TagDefinition) string {
	for _, tag := range tags {
		if tag.Name != tagName || !tag.Enabled {
			continue
		}
		if len(tag.DatasourceIDs) > 0 && !contains(tag.DatasourceIDs, datasourceID) {
			continue
		}
		return tag.FieldName
	}
	return ""
}

func uniqueStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
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
