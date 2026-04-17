package victorialogs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"vilog-victorialogs/internal/config"
	"vilog-victorialogs/internal/model"
)

type Client struct {
	httpClient *http.Client
	retries    int
}

func New(cfg config.VictoriaLogsConfig) *Client {
	return &Client{
		httpClient: &http.Client{},
		retries:    cfg.RequestRetries,
	}
}

func (c *Client) Ping(ctx context.Context, datasource model.Datasource) error {
	_, err := c.FieldNames(ctx, datasource, ListRequest{
		Query: "*",
		Start: time.Now().UTC().Add(-15 * time.Minute),
		End:   time.Now().UTC(),
		Limit: 10,
	})
	return err
}

func (c *Client) Query(ctx context.Context, datasource model.Datasource, req QueryRequest) ([]map[string]any, error) {
	body, err := c.doFormRequest(ctx, datasource, datasource.QueryPaths.Query, url.Values{
		"query":  []string{req.Query},
		"start":  []string{req.Start.Format(time.RFC3339)},
		"end":    []string{req.End.Format(time.RFC3339)},
		"limit":  []string{fmt.Sprintf("%d", req.Limit)},
		"offset": []string{fmt.Sprintf("%d", req.Offset)},
	}, http.MethodPost)
	if err != nil {
		return nil, err
	}

	return parseJSONLines(body)
}

func (c *Client) FieldNames(ctx context.Context, datasource model.Datasource, req ListRequest) ([]ValueStat, error) {
	return c.listValues(ctx, datasource, datasource.QueryPaths.FieldNames, req)
}

func (c *Client) StreamFieldNames(ctx context.Context, datasource model.Datasource, req ListRequest) ([]ValueStat, error) {
	return c.listValues(ctx, datasource, datasource.QueryPaths.StreamFieldNames, req)
}

func (c *Client) FieldValues(ctx context.Context, datasource model.Datasource, req FieldValuesRequest) ([]ValueStat, error) {
	return c.listValues(ctx, datasource, datasource.QueryPaths.FieldValues, ListRequest{
		Query:       req.Query,
		Start:       req.Start,
		End:         req.End,
		Field:       req.Field,
		Limit:       req.Limit,
		IgnorePipes: req.IgnorePipes,
	})
}

func (c *Client) StreamFieldValues(ctx context.Context, datasource model.Datasource, req FieldValuesRequest) ([]ValueStat, error) {
	return c.listValues(ctx, datasource, datasource.QueryPaths.StreamFieldValues, ListRequest{
		Query:       req.Query,
		Start:       req.Start,
		End:         req.End,
		Field:       req.Field,
		Limit:       req.Limit,
		IgnorePipes: req.IgnorePipes,
	})
}

func (c *Client) Facets(ctx context.Context, datasource model.Datasource, req ListRequest) ([]Facet, error) {
	body, err := c.doFormRequest(ctx, datasource, datasource.QueryPaths.Facets, buildListForm(req), http.MethodPost)
	if err != nil {
		return nil, err
	}

	var response FacetsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode facets response: %w", err)
	}
	return response.Facets, nil
}

func (c *Client) RunDeleteTask(ctx context.Context, datasource model.Datasource, filter string) (string, error) {
	body, err := c.doQueryRequest(ctx, datasource, datasource.QueryPaths.DeleteRunTask, url.Values{
		"filter": []string{filter},
	}, http.MethodGet)
	if err != nil {
		return "", err
	}

	var response DeleteTaskResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("decode delete task response: %w", err)
	}
	return response.TaskID, nil
}

func (c *Client) ActiveDeleteTasks(ctx context.Context, datasource model.Datasource) ([]ActiveDeleteTask, error) {
	body, err := c.doQueryRequest(ctx, datasource, datasource.QueryPaths.DeleteActiveTasks, nil, http.MethodGet)
	if err != nil {
		return nil, err
	}

	var tasks []ActiveDeleteTask
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, fmt.Errorf("decode active delete tasks response: %w", err)
	}
	return tasks, nil
}

func (c *Client) StopDeleteTask(ctx context.Context, datasource model.Datasource, taskID string) error {
	_, err := c.doQueryRequest(ctx, datasource, datasource.QueryPaths.DeleteStopTask, url.Values{
		"task_id": []string{taskID},
	}, http.MethodGet)
	return err
}

func (c *Client) listValues(ctx context.Context, datasource model.Datasource, path string, req ListRequest) ([]ValueStat, error) {
	body, err := c.doFormRequest(ctx, datasource, path, buildListForm(req), http.MethodPost)
	if err != nil {
		return nil, err
	}

	var response ValuesResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode values response: %w", err)
	}
	return response.Values, nil
}

func buildListForm(req ListRequest) url.Values {
	form := url.Values{
		"query": []string{req.Query},
		"start": []string{req.Start.Format(time.RFC3339)},
		"end":   []string{req.End.Format(time.RFC3339)},
	}
	if req.Field != "" {
		form.Set("field", req.Field)
	}
	if req.Limit > 0 {
		form.Set("limit", fmt.Sprintf("%d", req.Limit))
	}
	if req.IgnorePipes {
		form.Set("ignore_pipes", "1")
	}
	return form
}

func (c *Client) doFormRequest(ctx context.Context, datasource model.Datasource, path string, form url.Values, method string) ([]byte, error) {
	return c.doRequest(ctx, datasource, path, method, []byte(form.Encode()), "application/x-www-form-urlencoded", "")
}

func (c *Client) doQueryRequest(ctx context.Context, datasource model.Datasource, path string, query url.Values, method string) ([]byte, error) {
	encodedQuery := ""
	if query != nil {
		encodedQuery = query.Encode()
	}
	return c.doRequest(ctx, datasource, path, method, nil, "", encodedQuery)
}

func (c *Client) doRequest(ctx context.Context, datasource model.Datasource, path, method string, body []byte, contentType, encodedQuery string) ([]byte, error) {
	requestURL, err := datasourceURL(datasource.BaseURL, path, encodedQuery)
	if err != nil {
		return nil, err
	}

	var lastErr error
	attempts := c.retries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(datasource.TimeoutSeconds)*time.Second)
		var requestBody io.Reader
		if body != nil {
			requestBody = bytes.NewReader(body)
		}
		req, reqErr := http.NewRequestWithContext(attemptCtx, method, requestURL, requestBody)
		if reqErr != nil {
			cancel()
			return nil, fmt.Errorf("create request: %w", reqErr)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		applyDatasourceHeaders(req, datasource)

		resp, reqErr := c.httpClient.Do(req)
		if reqErr != nil {
			cancel()
			lastErr = fmt.Errorf("request failed: %w", reqErr)
			continue
		}

		responseBody, readErr := readBody(resp)
		cancel()
		if readErr != nil {
			lastErr = fmt.Errorf("read response body: %w", readErr)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
			continue
		}

		return responseBody, nil
	}

	return nil, lastErr
}

func datasourceURL(baseURL, path, encodedQuery string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse datasource base url: %w", err)
	}
	endpoint, err := url.Parse(strings.TrimSpace(path))
	if err != nil {
		return "", fmt.Errorf("parse datasource path: %w", err)
	}
	resolved := base.ResolveReference(endpoint)
	if encodedQuery != "" {
		resolved.RawQuery = encodedQuery
	}
	return resolved.String(), nil
}

func applyDatasourceHeaders(req *http.Request, datasource model.Datasource) {
	if datasource.Headers.AccountID != "" {
		req.Header.Set("AccountID", datasource.Headers.AccountID)
	}
	if datasource.Headers.ProjectID != "" {
		req.Header.Set("ProjectID", datasource.Headers.ProjectID)
	}
	if datasource.Headers.Authorization != "" {
		req.Header.Set("Authorization", datasource.Headers.Authorization)
	}
}

func parseJSONLines(data []byte) ([]map[string]any, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return []map[string]any{}, nil
	}

	if trimmed[0] == '[' {
		var rows []map[string]any
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return nil, fmt.Errorf("decode json array response: %w", err)
		}
		return rows, nil
	}

	rows := make([]map[string]any, 0)
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 0, 128*1024), 16*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("decode json line: %w", err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan json lines: %w", err)
	}
	return rows, nil
}

func readBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
