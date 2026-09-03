package main

import (
	"context"
	"time"

	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	logrus "github.com/sirupsen/logrus"
)

const schedulerReconnectBackoff = time.Second

// schedulerRegistrationTokens rate-limits controller re-subscriptions. The
// Mesos event stream can end with io.EOF when the master closes a connection;
// without a token gate controller.Run immediately reconnects in a tight loop.
func schedulerRegistrationTokens(ctx context.Context) <-chan struct{} {
	tokens := make(chan struct{}, 1)
	tokens <- struct{}{}
	go func() {
		ticker := time.NewTicker(schedulerReconnectBackoff)
		defer ticker.Stop()
		defer close(tokens)
		for {
			select {
			case <-ticker.C:
				select {
				case tokens <- struct{}{}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return tokens
}

type eventHandler struct{ s *Scheduler }

func (h eventHandler) HandleEvent(c context.Context, e *scheduler.Event) error {
	logrus.WithField("event_type", e.GetType().String()).WithField("event_error", e.GetError().GetMessage()).Info("mesos scheduler event received")
	return h.s.handleEvent(c, e)
}
