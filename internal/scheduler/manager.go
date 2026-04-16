package scheduler

import (
	"context"
	"sync"

	"github.com/robfig/cron/v3"

	"vilog-victorialogs/internal/service/retention"
	mongostore "vilog-victorialogs/internal/store/mongo"
)

type Manager struct {
	mu        sync.Mutex
	cron      *cron.Cron
	started   bool
	store     *mongostore.Store
	retention *retention.Service
}

func New(store *mongostore.Store, retentionService *retention.Service) *Manager {
	return &Manager{
		cron:      cron.New(cron.WithSeconds()),
		store:     store,
		retention: retentionService,
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.reloadLocked(ctx); err != nil {
		return err
	}
	if !m.started {
		m.cron.Start()
		m.started = true
	}
	return nil
}

func (m *Manager) Reload(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reloadLocked(ctx)
}

func (m *Manager) Stop() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cron == nil || !m.started {
		return stoppedContext()
	}
	m.started = false
	return m.cron.Stop()
}

func (m *Manager) reloadLocked(ctx context.Context) error {
	if m.cron == nil {
		m.cron = cron.New(cron.WithSeconds())
	} else {
		m.cron.Stop()
		m.cron = cron.New(cron.WithSeconds())
	}

	bindings, err := m.store.ListRetentionBindings(ctx)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if !binding.Enabled {
			continue
		}
		template, err := m.store.GetRetentionTemplate(ctx, binding.PolicyTemplateID)
		if err != nil || !template.Enabled {
			continue
		}

		bindingID := binding.ID
		if _, err := m.cron.AddFunc(template.Cron, func() {
			_, _ = m.retention.RunBinding(context.Background(), bindingID, "scheduler")
		}); err != nil {
			return err
		}
	}

	if m.started {
		m.cron.Start()
	}
	return nil
}

func stoppedContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
