package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	lib "github.com/m3scluster/clusterd-go/api/v1/lib"
	"github.com/m3scluster/clusterd-go/api/v1/lib/encoding/codecs"
	httpcli "github.com/m3scluster/clusterd-go/api/v1/lib/httpcli"
	httpsched "github.com/m3scluster/clusterd-go/api/v1/lib/httpcli/httpsched"
	"github.com/m3scluster/clusterd-go/api/v1/lib/scheduler/calls"
	"github.com/redis/go-redis/v9"
	logrus "github.com/sirupsen/logrus"
	"valkey-mesos-framework/utils"
)

type Config struct {
	Master, Image, Role, Name, User, RedisServer, ValkeyMetricsAddr, CNI, Domain, MasterHost, Listen, SSLKeyBase64, SSLCertBase64, FrontendURL string
	Password, RedisPassword                                                                                                                    string
	RedisDB, Masters, Slaves, RedisPoolSize                                                                                                    int
	CPU, Memory                                                                                                                                float64
	Port                                                                                                                                       int
	DryRun, InsecureTLS, Checkpoint                                                                                                            bool
	ReconcileLoopTime                                                                                                                          time.Duration
}

// This model supports one master and requires at least one replica. Keep the
// invariant in the backend so callers cannot weaken it by bypassing the UI.
const minSlaves = 1
const minMasters = 1

type Task struct {
	ID, Role, State, Agent, Host string
	Port                         int
	Updated                      time.Time
}

type valkeyMetricsReader interface {
	Info(context.Context, ...string) (string, error)
}

type redisInfoReader struct{ client *redis.Client }

func (r redisInfoReader) Info(ctx context.Context, sections ...string) (string, error) {
	return r.client.Info(ctx, sections...).Result()
}

type Scheduler struct {
	cfg              Config
	mu               sync.Mutex
	frameworkID      string
	desired          bool
	tasks            map[string]*Task
	state            *redis.Client
	caller           calls.Caller
	framework        *lib.FrameworkInfo
	runCancel        context.CancelFunc
	runDone          chan struct{}
	running          bool
	resourceShortage bool
	metricsReader    valkeyMetricsReader
}

const programName = "valkey-mesos-framework"

// version is replaced by the release build with -ldflags
// "main.version=<version>". Keeping a development default makes local builds
// self-describing as well.
var version = "dev"

func logStartup(c Config) {
	logrus.WithFields(logrus.Fields{
		"program":      programName,
		"version":      version,
		"mesos_master": c.Master,
	}).Info("scheduler starting")
}

func atoi(k string, d int) int {
	v, e := strconv.Atoi(utils.Getenv(k, strconv.Itoa(d)))
	if e != nil {
		return d
	}
	return v
}
func loadConfig() Config {
	master := utils.Getenv("MESOS_MASTER", "127.0.0.1:5050")
	if !strings.Contains(master, "://") {
		scheme := "http"
		if utils.Getenv("MESOS_SSL", "false") == "true" {
			scheme = "https"
		}
		master = scheme + "://" + master
	}
	name := utils.Getenv("FRAMEWORK_NAME", "valkey-framework")
	cni := utils.Getenv("MESOS_CNI", "weave")
	if value, ok := utils.LookupEnv("MESOS_CNI"); ok {
		cni = value
	}
	domainDefault := "mesos"
	if cni == "weave" {
		domainDefault = "weave.local"
	}
	domain := strings.Trim(utils.Getenv("MESOS_DOMAIN", domainDefault), ".")
	// Mesos-DNS uses the task hostname and domain; the framework name is not part
	// of the DNS name (for example, master.weave.local).
	masterHost := "master"
	if domain != "" {
		masterHost += "." + domain
	}
	slaves := atoi("VALKEY_SLAVES", 2)
	if slaves < minSlaves {
		slaves = minSlaves
	}
	masters := atoi("VALKEY_MASTERS", 1)
	if masters < minMasters {
		masters = minMasters
	}
	port := atoi("VALKEY_PORT", 6379)
	metricsAddr := utils.Getenv("VALKEY_METRICS_ADDR", net.JoinHostPort(masterHost, strconv.Itoa(port)))
	reconcileLoopTime, _ := time.ParseDuration(utils.Getenv("RECONCILE_WAIT", "30m"))
	return Config{Master: master, Image: utils.Getenv("VALKEY_IMAGE", "valkey/valkey:8-alpine"), Role: utils.Getenv("MESOS_ROLE", "*"), Name: name, User: utils.Getenv("FRAMEWORK_USER", utils.Getenv("USER", "root")), Password: utils.Getenv("MESOS_PASSWORD", ""), RedisServer: utils.Getenv("REDIS_SERVER", "127.0.0.1:6379"), ValkeyMetricsAddr: metricsAddr, RedisPassword: utils.Getenv("REDIS_PASSWORD", ""), RedisDB: atoi("REDIS_DB", 1), RedisPoolSize: atoi("REDIS_POOLSIZE", 0), CNI: cni, Domain: domain, MasterHost: utils.Getenv("VALKEY_MASTER_HOST", masterHost), Masters: masters, Slaves: slaves, CPU: floatEnv("VALKEY_CPU", .2), Memory: floatEnv("VALKEY_MEMORY_MB", 256), Port: port, Listen: utils.Getenv("LISTEN_ADDR", "0.0.0.0:10001"), DryRun: utils.Getenv("MESOS_DRY_RUN", "false") == "true", InsecureTLS: utils.Getenv("MESOS_TLS_INSECURE", "false") == "true", Checkpoint: utils.Getenv("MESOS_CHECKPOINT", "true") == "true", SSLKeyBase64: utils.Getenv("SSL_KEY_BASE64", ""), SSLCertBase64: utils.Getenv("SSL_CRT_BASE64", ""), ReconcileLoopTime: reconcileLoopTime, FrontendURL: utils.Getenv("FRONTEND_URL", "http://localhost:5173")}
}
func floatEnv(k string, d float64) float64 {
	v, e := strconv.ParseFloat(utils.Getenv(k, strconv.FormatFloat(d, 'f', -1, 64)), 64)
	if e != nil {
		return d
	}
	return v
}

func newRedisClient(c Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     c.RedisServer,
		Password: c.RedisPassword,
		DB:       c.RedisDB,
		PoolSize: c.RedisPoolSize,
	})
}

func NewScheduler(c Config) *Scheduler {
	endpoint := strings.TrimRight(c.Master, "/") + "/api/v1/scheduler"
	opts := []httpcli.Opt{
		httpcli.Endpoint(endpoint),
		httpcli.Codec(codecs.ByMediaType[codecs.MediaTypeJSON]),
	}
	configOpts := []httpcli.ConfigOpt{httpcli.Timeout(15 * time.Second)}
	if c.InsecureTLS {
		configOpts = append(configOpts, httpcli.TLSConfig(&tls.Config{InsecureSkipVerify: true}))
	}
	if u := utils.Getenv("MESOS_USERNAME", ""); u != "" && c.Password != "" {
		configOpts = append(configOpts, httpcli.BasicAuth(u, c.Password))
	}
	opts = append(opts, httpcli.Do(httpcli.With(configOpts...)))
	cli := httpcli.New(opts...)
	ft := float64(3600)
	role := c.Role
	framework := &lib.FrameworkInfo{User: c.User, Name: c.Name, Role: &role, FailoverTimeout: &ft, Checkpoint: &c.Checkpoint}
	if c.FrontendURL != "" {
		framework.WebUiURL = &c.FrontendURL
	}
	s := &Scheduler{cfg: c, tasks: map[string]*Task{}, state: newRedisClient(c), caller: httpsched.NewCaller(cli), framework: framework}
	s.metricsReader = redisInfoReader{client: redis.NewClient(&redis.Options{Addr: c.ValkeyMetricsAddr})}
	s.load()
	return s
}
func (s *Scheduler) currentFrameworkID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frameworkID
}
func (s *Scheduler) recordFrameworkID(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frameworkID == id {
		return false
	}
	s.frameworkID = id
	return true
}
func (s *Scheduler) save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"framework_id": s.frameworkID, "desired": s.desired, "tasks": s.tasks, "config": s.cfg})
	if s.state != nil {
		_ = s.state.Set(context.Background(), s.cfg.Name+":state", b, 0).Err()
	}
}
func (s *Scheduler) load() {
	// Redis is the only durable source of truth.
	if s.state == nil {
		return
	}
	if b, err := s.state.Get(context.Background(), s.cfg.Name+":state").Result(); err == nil {
		s.restoreState([]byte(b))
	}
}

func (s *Scheduler) restoreState(b []byte) bool {
	var v struct {
		FrameworkID string           `json:"framework_id"`
		Desired     bool             `json:"desired"`
		Tasks       map[string]*Task `json:"tasks"`
	}
	if json.Unmarshal(b, &v) != nil {
		return false
	}
	s.frameworkID = v.FrameworkID
	if s.framework != nil {
		if v.FrameworkID == "" {
			s.framework.ID = nil
		} else {
			s.framework.ID = &lib.FrameworkID{Value: v.FrameworkID}
		}
	}
	s.desired = v.Desired
	if v.Tasks != nil {
		s.tasks = v.Tasks
	}
	return true
}

var valkeyInfoSections = []string{"memory", "clients", "stats", "replication", "cpu", "commandstats", "latencystats"}

func parseValkeyInfo(raw string) map[string]map[string]any {
	sections := make(map[string]map[string]any, len(valkeyInfoSections))
	section := ""
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			section = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "#")))
			if sections[section] == nil {
				sections[section] = make(map[string]any)
			}
			continue
		}
		separator := strings.IndexByte(line, ':')
		if section == "" || separator <= 0 {
			continue
		}
		key, value := strings.TrimSpace(line[:separator]), strings.TrimSpace(line[separator+1:])
		if number, err := strconv.ParseFloat(value, 64); err == nil {
			sections[section][key] = number
		} else {
			sections[section][key] = value
		}
	}
	return sections
}

func main() {
	c := loadConfig()
	logStartup(c)
	s := NewScheduler(c)

	if c.SSLKeyBase64 != "" && c.SSLCertBase64 != "" {
		go func() {
			if e := serveHTTPS(c, s); e != nil {
				logrus.WithError(e).Error("https server failed")
			}
		}()
	} else {
		// Fallback to HTTP
		go func() {
			if e := http.ListenAndServe(c.Listen, s.handler()); e != nil {
				logrus.WithError(e).Error("http server failed")
			}
		}()
	}

	if utils.Getenv("START_ON_BOOT", "true") == "true" {
		s.start()
	}
	select {}
}

func tlsConfigFromBase64(keyBase64, certBase64 string) (*tls.Config, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return nil, fmt.Errorf("decode SSL key: %w", err)
	}
	certBytes, err := base64.StdEncoding.DecodeString(certBase64)
	if err != nil {
		return nil, fmt.Errorf("decode SSL certificate: %w", err)
	}
	certificate, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("load SSL certificate and key: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

func serveHTTPS(c Config, s *Scheduler) error {
	config, err := tlsConfigFromBase64(c.SSLKeyBase64, c.SSLCertBase64)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:      c.Listen,
		TLSConfig: config,
		Handler:   s.handler(),
	}
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	return server.Serve(tls.NewListener(listener, config))
}
