package httpserver

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"vilog-victorialogs/internal/model"
	"vilog-victorialogs/internal/util"
)

func (s *Server) registerDatasourceRoutes(api *gin.RouterGroup) {
	group := api.Group("/datasources")
	group.GET("", s.listDatasources)
	group.POST("", s.createDatasource)
	group.PUT("/:id", s.updateDatasource)
	group.POST("/:id/test", s.testDatasource)
	group.POST("/:id/discover", s.discoverDatasource)
}

func (s *Server) registerQueryRoutes(api *gin.RouterGroup) {
	group := api.Group("/query")
	group.POST("/search", s.searchLogs)
	group.GET("/services", s.listServices)
	group.GET("/tags", s.listQueryTags)
	group.GET("/tag-values", s.listTagValues)
}

func (s *Server) registerTagRoutes(api *gin.RouterGroup) {
	group := api.Group("/tags")
	group.GET("", s.listTags)
	group.POST("", s.createTag)
	group.PUT("/:id", s.updateTag)
	group.DELETE("/:id", s.deleteTag)
}

func (s *Server) registerRetentionRoutes(api *gin.RouterGroup) {
	group := api.Group("/retention")
	group.GET("/templates", s.listRetentionTemplates)
	group.POST("/templates", s.createRetentionTemplate)
	group.PUT("/templates/:id", s.updateRetentionTemplate)
	group.GET("/bindings", s.listRetentionBindings)
	group.POST("/bindings", s.createRetentionBinding)
	group.PUT("/bindings/:id", s.updateRetentionBinding)
	group.POST("/run/:datasource_id", s.runRetention)
	group.GET("/tasks", s.listRetentionTasks)
	group.POST("/tasks/:id/stop", s.stopRetentionTask)
}

func (s *Server) listDatasources(c *gin.Context) {
	items, err := s.deps.Datasources.List(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, "datasource_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *Server) createDatasource(c *gin.Context) {
	var req model.DatasourceUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Datasources.Create(c.Request.Context(), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "datasource_create_failed", err.Error())
		return
	}
	c.JSON(http.StatusCreated, item)
}

func (s *Server) updateDatasource(c *gin.Context) {
	var req model.DatasourceUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Datasources.Update(c.Request.Context(), c.Param("id"), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "datasource_update_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *Server) testDatasource(c *gin.Context) {
	result, err := s.deps.Datasources.Test(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "datasource_test_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) discoverDatasource(c *gin.Context) {
	result, err := s.deps.Discovery.Discover(c.Request.Context(), c.Param("id"), actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "discover_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.DiscoveryResponse{Snapshot: result})
}

func (s *Server) searchLogs(c *gin.Context) {
	var req model.SearchRequest
	if !bindJSON(c, &req) {
		return
	}

	result, err := s.deps.Query.Search(c.Request.Context(), req)
	if err != nil {
		writeError(c, http.StatusBadRequest, "query_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) listServices(c *gin.Context) {
	datasourceID := c.Query("datasource_id")
	if datasourceID == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "datasource_id is required")
		return
	}

	services, cacheHit, err := s.deps.Discovery.ListServices(c.Request.Context(), datasourceID)
	if err != nil {
		writeError(c, http.StatusBadRequest, "service_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.ServiceListResponse{
		Services: services,
		CacheHit: cacheHit,
	})
}

func (s *Server) listQueryTags(c *gin.Context) {
	datasourceID := c.Query("datasource_id")
	if datasourceID == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "datasource_id is required")
		return
	}

	tags, err := s.deps.Discovery.ListTags(c.Request.Context(), datasourceID, c.Query("service"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "tag_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.TagCatalogResponse{Tags: tags})
}

func (s *Server) listTagValues(c *gin.Context) {
	datasourceID := c.Query("datasource_id")
	field := c.Query("field")
	if datasourceID == "" || field == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "datasource_id and field are required")
		return
	}

	values, cacheHit, err := s.deps.Discovery.ListTagValues(c.Request.Context(), datasourceID, field, c.Query("service"))
	if err != nil {
		writeError(c, http.StatusBadRequest, "tag_values_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.TagValuesResponse{
		Field:    field,
		Values:   values,
		CacheHit: cacheHit,
	})
}

func (s *Server) listTags(c *gin.Context) {
	items, err := s.deps.Discovery.ListAllTags(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, "tag_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *Server) createTag(c *gin.Context) {
	var req model.TagDefinitionUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Discovery.CreateTag(c.Request.Context(), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "tag_create_failed", err.Error())
		return
	}
	c.JSON(http.StatusCreated, item)
}

func (s *Server) updateTag(c *gin.Context) {
	var req model.TagDefinitionUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Discovery.UpdateTag(c.Request.Context(), c.Param("id"), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "tag_update_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *Server) deleteTag(c *gin.Context) {
	if err := s.deps.Discovery.DeleteTag(c.Request.Context(), c.Param("id"), actorFromRequest(c, s.cfg.Security.TrustProxyHeaders)); err != nil {
		writeError(c, http.StatusBadRequest, "tag_delete_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.StatusResponse{Status: "ok"})
}

func (s *Server) listRetentionTemplates(c *gin.Context) {
	items, err := s.deps.Retention.ListTemplates(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, "retention_template_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *Server) createRetentionTemplate(c *gin.Context) {
	var req model.RetentionPolicyTemplateUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Retention.CreateTemplate(c.Request.Context(), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "retention_template_create_failed", err.Error())
		return
	}
	_ = s.reloadScheduler(c)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) updateRetentionTemplate(c *gin.Context) {
	var req model.RetentionPolicyTemplateUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Retention.UpdateTemplate(c.Request.Context(), c.Param("id"), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "retention_template_update_failed", err.Error())
		return
	}
	_ = s.reloadScheduler(c)
	c.JSON(http.StatusOK, item)
}

func (s *Server) listRetentionBindings(c *gin.Context) {
	items, err := s.deps.Retention.ListBindings(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, "retention_binding_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *Server) createRetentionBinding(c *gin.Context) {
	var req model.DatasourceRetentionBindingUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Retention.CreateBinding(c.Request.Context(), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "retention_binding_create_failed", err.Error())
		return
	}
	_ = s.reloadScheduler(c)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) updateRetentionBinding(c *gin.Context) {
	var req model.DatasourceRetentionBindingUpsertRequest
	if !bindJSON(c, &req) {
		return
	}

	item, err := s.deps.Retention.UpdateBinding(c.Request.Context(), c.Param("id"), req, actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "retention_binding_update_failed", err.Error())
		return
	}
	_ = s.reloadScheduler(c)
	c.JSON(http.StatusOK, item)
}

func (s *Server) runRetention(c *gin.Context) {
	result, err := s.deps.Retention.RunDatasource(c.Request.Context(), c.Param("datasource_id"), actorFromRequest(c, s.cfg.Security.TrustProxyHeaders))
	if err != nil {
		writeError(c, http.StatusBadRequest, "retention_run_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) listRetentionTasks(c *gin.Context) {
	items, err := s.deps.Retention.ListTasks(c.Request.Context())
	if err != nil {
		writeError(c, http.StatusInternalServerError, "retention_task_list_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, items)
}

func (s *Server) stopRetentionTask(c *gin.Context) {
	if err := s.deps.Retention.StopTask(c.Request.Context(), c.Param("id"), actorFromRequest(c, s.cfg.Security.TrustProxyHeaders)); err != nil {
		writeError(c, http.StatusBadRequest, "retention_task_stop_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, model.StatusResponse{Status: "ok"})
}

func (s *Server) reloadScheduler(c *gin.Context) error {
	if s.deps.Scheduler == nil || !s.cfg.Retention.SchedulerEnabled {
		return nil
	}
	return s.deps.Scheduler.Reload(c.Request.Context())
}

func bindJSON(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return false
	}
	return true
}

func writeError(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, model.ErrorResponse{
		Error: model.APIError{
			Code:    code,
			Message: message,
		},
	})
}

func actorFromRequest(c *gin.Context, trustProxyHeaders bool) string {
	ip := util.ExtractClientIP(c.Request, trustProxyHeaders)
	if ip == nil {
		return "unknown"
	}
	return ip.String()
}
