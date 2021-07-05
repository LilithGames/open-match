package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/linbit/k8s-await-election/pkg/consts"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var Version = "development"

var log = logrus.New()

type AwaitElection struct {
	WithElection       bool
	Name               string
	LockName           string
	LockNamespace      string
	LeaderIdentity     string
	StatusEndpoint     string
	ServiceName        string
	ServiceNamespace   string
	PodIP              string
	NodeName           *string
	ServicePorts       []corev1.EndpointPort
	LeaderExec         func(ctx context.Context) error
	ProcessStatusURL   string
	ProcessStatusDelay int
}

var leaseStart time.Time
var isLeaseHolder bool
var lostLease bool

type ConfigError struct {
	missingEnv string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config: missing required environment variable: '%s'", e.missingEnv)
}

func NewAwaitElectionConfig(exec func(ctx context.Context) error) (*AwaitElection, error) {
	if os.Getenv(consts.AwaitElectionEnabledKey) == "" {
		return &AwaitElection{
			WithElection: false,
			LeaderExec:   exec,
		}, nil
	}

	name := os.Getenv(consts.AwaitElectionNameKey)
	if name == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionNameKey}
	}

	lockName := os.Getenv(consts.AwaitElectionLockNameKey)
	if lockName == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionLockNameKey}
	}

	lockNamespace := os.Getenv(consts.AwaitElectionLockNamespaceKey)
	if lockNamespace == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionLockNamespaceKey}
	}

	leaderIdentity := os.Getenv(consts.AwaitElectionIdentityKey)
	if leaderIdentity == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionIdentityKey}
	}

	// Optional
	statusEndpoint := os.Getenv(consts.AwaitElectionStatusEndpointKey)
	if statusEndpoint == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionStatusEndpointKey}
	}

	podIP := os.Getenv(consts.AwaitElectionPodIP)

	var nodeName *string
	if val, ok := os.LookupEnv(consts.AwaitElectionNodeName); ok {
		nodeName = &val
	}

	serviceName := os.Getenv(consts.AwaitElectionServiceName)
	if serviceName == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionServiceName}
	}

	serviceNamespace := os.Getenv(consts.AwaitElectionServiceNamespace)
	if serviceNamespace == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionServiceNamespace}
	}

	servicePortsJSON := os.Getenv(consts.AwaitElectionServicePortsJson)
	if servicePortsJSON == "" {
		return nil, &ConfigError{missingEnv: consts.AwaitElectionServicePortsJson}
	}

	var servicePorts []corev1.EndpointPort
	err := json.Unmarshal([]byte(servicePortsJSON), &servicePorts)
	if serviceName != "" && err != nil {
		return nil, fmt.Errorf("failed to parse ports from env: %w", err)
	}

	statusUrl := os.Getenv(consts.AwaitElectionProcessStatusURL)
	statusDelay, err := strconv.Atoi(os.Getenv(consts.AwaitElectionProcessStatusDelay))
	if statusUrl != "" && (statusDelay == 0 || err != nil) {
		return nil, fmt.Errorf("fail to prase process status url and delay")
	}

	return &AwaitElection{
		WithElection:       true,
		Name:               name,
		LockName:           lockName,
		LockNamespace:      lockNamespace,
		LeaderIdentity:     leaderIdentity,
		StatusEndpoint:     statusEndpoint,
		PodIP:              podIP,
		NodeName:           nodeName,
		ServiceName:        serviceName,
		ServiceNamespace:   serviceNamespace,
		ServicePorts:       servicePorts,
		LeaderExec:         exec,
		ProcessStatusURL:   statusUrl,
		ProcessStatusDelay: statusDelay,
	}, nil

}

func (el *AwaitElection) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Not running with leader election/kubernetes context, just run the provided function
	if !el.WithElection {
		log.Info("not running with leader election")
		return el.LeaderExec(ctx)
	}

	// Create kubernetes client
	kubeCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to read cluster config: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(kubeCfg)
	if err != nil {
		return fmt.Errorf("failed to create KubeClient for config: %w", err)
	}

	// result of the LeaderExec(ctx) command will be send over this channel
	execResult := make(chan error)

	// Create lock for leader election using provided settings
	lock := resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      el.LockName,
			Namespace: el.LockNamespace,
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: el.LeaderIdentity,
		},
	}

	leaderCfg := leaderelection.LeaderElectionConfig{
		Lock:            &lock,
		Name:            el.Name,
		ReleaseOnCancel: true,
		// Suggested default values
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Infof("OnStartedLeading ....")
				// First we need to register our pod as the service endpoint
				err := el.setServiceEndpoint(ctx, kubeClient)
				if err != nil {
					log.Error("set service Endpoint err", err.Error())
					execResult <- err
					return
				}
				// actual start the command here.
				// Note: this callback is started in a goroutine, so we can block this
				// execution path for as long as we want.
				isLeaseHolder = true
				leaseStart = time.Now()
				err = el.LeaderExec(ctx)
				execResult <- err
			},
			OnNewLeader: func(identity string) {
				log.Infof("long live our new leader: '%s'!", identity)
			},
			OnStoppedLeading: func() {
				lostLease = true
				log.Info("lost leader status")
			},
		},
	}
	elector, err := leaderelection.NewLeaderElector(leaderCfg)
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	statusServerResult := el.startStatusEndpoint(ctx, elector)

	go elector.Run(ctx)

	// the different end conditions:
	// 1. context was cancelled -> error
	// 2. command executed -> either error or nil, depending on return value
	// 3. status endpoint failed -> could not create status endpoint
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-execResult:
		return r
	case r := <-statusServerResult:
		return r
	}
}

func (el *AwaitElection) setServiceEndpoint(ctx context.Context, client *kubernetes.Clientset) error {
	if el.ServiceName == "" {
		return nil
	}

	endpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      el.ServiceName,
			Namespace: el.ServiceNamespace,
		},
		Subsets: []corev1.EndpointSubset{
			{
				Addresses: []corev1.EndpointAddress{{IP: el.PodIP, NodeName: el.NodeName}},
				Ports:     el.ServicePorts,
			},
		},
	}
	_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Create(ctx, endpoints, metav1.CreateOptions{})
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}

		_, err := client.CoreV1().Endpoints(el.ServiceNamespace).Update(ctx, endpoints, metav1.UpdateOptions{})
		return err
	}

	return nil
}

func (el *AwaitElection) startStatusEndpoint(ctx context.Context, elector *leaderelection.LeaderElector) <-chan error {
	statusServerResult := make(chan error)

	if el.StatusEndpoint == "" {
		log.Info("no status endpoint specified, will not be created")
		return statusServerResult
	}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		// check for elector self
		err := elector.Check(2 * time.Second)
		if err != nil {
			log.WithField("err", err).Error("failed to elector.check, reporting unhealthy status")
			writer.WriteHeader(500)
			_, err := writer.Write([]byte("{\"status\": \"expired\"}"))
			if err != nil {
				log.WithField("err", err).Error("failed to serve request")
			}
			return
		}

		if isLeaseHolder && lostLease {
			writer.WriteHeader(500)
			log.Error("lose lease, status need restart")
			_, err := writer.Write([]byte("{\"status\": \"expired\"}"))
			if err != nil {
				log.WithField("err", err).Error("failed to serve request")
			}
			return
		}

		// check process if needed
		if isLeaseHolder && !time.Now().Before(leaseStart.Add(time.Second*time.Duration(el.ProcessStatusDelay))) {
			cli := http.Client{Timeout: time.Second}
			_, err := cli.Get(el.ProcessStatusURL)
			if err != nil {
				log.WithField("err", err).Error("failed to check process status, reporting unhealthy status url:" + el.ProcessStatusURL)
				writer.WriteHeader(500)
				_, err := writer.Write([]byte("{\"status\": \"expired\"}"))
				if err != nil {
					log.WithField("err", err).Error("failed to serve request")
				}
				return
			}
			log.Infof("check Process Status Sucess url:%s", el.ProcessStatusURL)
		}

		log.Info("check Status Ok", " islease:", isLeaseHolder, " start:", leaseStart, " now:", time.Now())

		_, err = writer.Write([]byte("{\"status\": \"ok\"}"))
		if err != nil {
			log.WithField("err", err).Error("failed to serve request")
		}
	})

	statusServer := http.Server{
		Addr:    el.StatusEndpoint,
		Handler: serveMux,
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}
	go func() {
		statusServerResult <- statusServer.ListenAndServe()
	}()
	return statusServerResult
}

// Run the command specified the this program's arguments to completion.
// Stdout and Stderr are inherited from this process. If the provided context is cancelled,
// the started process is killed.
func Execute(ctx context.Context) error {
	log.Infof("starting command '%s' with arguments: '%v'", os.Args[1], os.Args[2:])
	cmd := exec.CommandContext(ctx, os.Args[1], os.Args[2:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

func main() {
	log.WithField("version", Version).Info("running k8s-await-election")

	if len(os.Args) <= 1 {
		log.WithField("args", os.Args).Fatal("Need at least one argument to run")
	}

	awaitElectionConfig, err := NewAwaitElectionConfig(Execute)
	if err != nil {
		log.WithField("err", err).Fatal("failed to create runner")
	}

	err = awaitElectionConfig.Run()
	if err != nil {
		log.WithField("err", err).Fatalf("failed to run")
	}
}
