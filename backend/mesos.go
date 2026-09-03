package main

import (
	"context"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/extras/scheduler/controller"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
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

// frameworkCaller decorates ACKNOWLEDGE calls with the framework ID assigned
// by Mesos. The controller rule owns UUID filtering and ACK error handling;
// this adapter only supplies the call metadata that the rule cannot know.
type frameworkCaller struct{ s *Scheduler }

func (c frameworkCaller) Call(ctx context.Context, call *scheduler.Call) (lib.Response, error) {
	if call.GetType() == scheduler.Call_ACKNOWLEDGE {
		calls.Framework(c.s.currentFrameworkID())(call)
	}
	return c.s.caller.Call(ctx, call)
}

type eventHandler struct{ s *Scheduler }

type schedulerEventHandler struct{ s *Scheduler }

func (h schedulerEventHandler) HandleEvent(c context.Context, e *scheduler.Event) error {
	return h.s.handleEvent(c, e)
}

func (h eventHandler) HandleEvent(c context.Context, e *scheduler.Event) error {
	logrus.WithField("event_type", e.GetType().String()).WithField("event_error", e.GetError().GetMessage()).Info("mesos scheduler event received")
	return controller.AckStatusUpdates(frameworkCaller{h.s}).AndThen().Handle(schedulerEventHandler{h.s}).HandleEvent(c, e)
}
