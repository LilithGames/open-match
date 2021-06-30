package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"log"
)

var (
	curLeader string
)

func main() {
	var leaseLockName string
	var leaseLockNamespace string
	var id string
	var http int

	flag.StringVar(&id, "id", uuid.New().String(), "the holder identity name")
	flag.StringVar(&leaseLockName, "lease-lock-name", "", "the lease lock resource name")
	flag.StringVar(&leaseLockNamespace, "lease-lock-namespace", "", "the lease lock resource namespace")
	flag.IntVar(&http, "http", 8086, "http expose for leader election info")
	flag.Parse()

	if leaseLockName == "" {
		log.Fatal("unable to get lease lock resource name (missing lease-lock-name flag).")
	}

	if leaseLockNamespace == "" {
		log.Fatal("unable to get lease lock resource namespace (missing lease-lock-namespace flag).")
	}

	// leader election uses the Kubernetes API by writing to a
	// lock object, which can be a LeaseLock object (preferred),
	// a ConfigMap, or an Endpoints (deprecated) object.
	// Conflicting writes are detected and each client handles those actions
	// independently.
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		r := gin.Default()
		r.GET("/leader", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"leader": curLeader,
			})
		})
		err := r.Run(fmt.Sprintf(":%d", http))
		if err != nil {
			log.Printf("start http fail %s", err.Error())
		}
	}()

	client := clientset.NewForConfigOrDie(config)
	run := func(ctx context.Context) {
		select {}
	}

	// use a Go context so we can tell the leaderelection code when we
	// want to step down
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// listen for interrupts or the Linux SIGTERM signal and cancel
	// our context, which the leader election code will observe and
	// step down
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Printf("Received termination, signaling shutdown")
		cancel()
	}()

	// we use the Lease lock type since edits to Leases are less common
	// and fewer objects in the cluster watch "all Leases".
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: leaseLockNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// start the leader election code loop
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second,
		RenewDeadline:   15 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				run(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				log.Printf("leader lost: %s", id)
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				log.Printf("new leader elected: %s", identity)
				// we're notified when new leader elected
				curLeader = identity
				if identity == id {
					// I just got the lock
					log.Printf("elected leader success: %s", identity)
					return
				}
			},
		},
	})
}
