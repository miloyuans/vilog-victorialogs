package telegram

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"vilog-victorialogs/internal/config"
)

type Service struct {
	enabled    bool
	apiBaseURL string
	botToken   string
	chatID     string
	timeout    time.Duration
	httpClient *http.Client
}

func New(cfg config.TelegramConfig) *Service {
	return &Service{
		enabled:    cfg.Enabled,
		apiBaseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
		botToken:   cfg.BotToken,
		chatID:     cfg.ChatID,
		timeout:    cfg.SendTimeout,
		httpClient: &http.Client{},
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.enabled
}

func (s *Service) SendMessage(ctx context.Context, text string) error {
	if !s.Enabled() {
		return nil
	}

	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", s.apiBaseURL, s.botToken)
	form := url.Values{
		"chat_id": []string{s.chatID},
		"text":    []string{text},
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send telegram message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram sendMessage failed with status %d", resp.StatusCode)
	}

	return nil
}
