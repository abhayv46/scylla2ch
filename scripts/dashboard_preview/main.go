// Dashboard preview server — serves the dashboard with mock data on port 8095.
// Run: go run ./scripts/dashboard_preview
// Login: admin / test123
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"scylla2ch/config"
	"scylla2ch/dashboard"
	"scylla2ch/metrics"
)

type mockCHChecker struct{}

func (m *mockCHChecker) TableExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (m *mockCHChecker) TableColumns(_ context.Context, _ string) ([]config.CHColumn, error) {
	return nil, nil
}

func main() {
	running := map[string]bool{
		"chat.messages":                 true,
		"chat.rooms":                    true,
		"chat.ai_reply_suggestions":     true,
		"chat.moderation_events":        true,
		"chat.first_exchange_lifecycle":  true,
		"chat.llm_mod_windows":          true,
		"chat.llm_mod_strikes":          true,
		"chat.posts_by_id":              true,
		"chat.comments_by_post":         true,
		"chat.votes_by_post":            true,
	}
	var mu sync.Mutex

	// Seed prometheus metrics with realistic values
	metrics.SetBuildInfo("1.0.0-dev")
	metrics.ScyllaConnected.Set(1)
	metrics.ClickHouseConnected.Set(1)
	metrics.TablesActive.Set(float64(len(running)))
	metrics.ConsumersActive.Set(float64(len(running)))
	metrics.UptimeSeconds.Set(7200)

	for tbl := range running {
		consumed := float64(10000 + rand.Intn(200000))
		metrics.RowsConsumed.WithLabelValues(tbl).Add(consumed)
		metrics.RowsFlushed.WithLabelValues(tbl).Add(consumed - float64(rand.Intn(5)))
		metrics.BufferSize.WithLabelValues(tbl).Set(float64(rand.Intn(500)))
		metrics.CDCLagSeconds.WithLabelValues(tbl).Set(float64(rand.Intn(8)) + rand.Float64())
		metrics.FlushesTotal.WithLabelValues(tbl, "ok").Add(float64(100 + rand.Intn(5000)))
		for i := 0; i < 50; i++ {
			metrics.FlushLatency.WithLabelValues(tbl).Observe(0.005 + rand.Float64()*0.05)
		}
	}

	// Simulate uptime ticking
	startTime := time.Now().Add(-2 * time.Hour)
	go func() {
		for range time.Tick(10 * time.Second) {
			metrics.UptimeSeconds.Set(time.Since(startTime).Seconds())
			// Simulate some traffic
			for tbl := range running {
				metrics.RowsConsumed.WithLabelValues(tbl).Add(float64(rand.Intn(100)))
				metrics.RowsFlushed.WithLabelValues(tbl).Add(float64(rand.Intn(100)))
				metrics.CDCLagSeconds.WithLabelValues(tbl).Set(float64(rand.Intn(5)) + rand.Float64())
				metrics.BufferSize.WithLabelValues(tbl).Set(float64(rand.Intn(300)))
				metrics.FlushLatency.WithLabelValues(tbl).Observe(0.005 + rand.Float64()*0.03)
				metrics.FlushesTotal.WithLabelValues(tbl, "ok").Add(float64(1 + rand.Intn(5)))
			}
		}
	}()

	cfg := &config.Config{
		Scylla: config.ScyllaConfig{
			Keyspace: "chat",
		},
		Batch: config.BatchConfig{
			Size:     10000,
			Interval: 5 * time.Second,
		},
		Tables: config.TablesConfig{
			AutoDiscover: false,
			AutoCreate:   false,
		},
	}

	os.Setenv("SCYLLA2CH_DASHBOARD_USER", "admin")
	os.Setenv("SCYLLA2CH_DASHBOARD_PASS", "test123")

	s := &dashboard.State{
		Running:    &running,
		RunningMu:  &mu,
		Config:     cfg,
		ConfigFile: "config/config.dev.yaml",
		StartTime:  startTime,
		Version:    "1.0.0-dev",
		Logger:     log.New(os.Stderr, "[dashboard-preview] ", log.Ldate|log.Ltime),
		SessionMap: nil,
		CHDBMap:    map[string]string{"chat": "chat"},
		CHChecker:  &mockCHChecker{},
	}

	srv := &http.Server{
		Addr:              ":8095",
		Handler:           dashboard.NewMux(s),
		ReadHeaderTimeout: 5 * time.Second,
	}

	fmt.Println("Dashboard preview: http://localhost:8095")
	fmt.Println("Login: admin / test123")
	fmt.Println("Metrics update every 10s with mock traffic")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
