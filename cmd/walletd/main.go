// Command walletd runs the wallet service (HTTP API, SQS consumer, outbox
// publisher and pending-reference worker) in a single process; every replica
// is stateless and can be scaled horizontally.
//
//	walletd              run the service
//	walletd healthcheck  probe /health/ready of a running instance (for Docker)
package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pedro/jungle-wallet/internal/bootstrap"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	// Run blocks until SIGINT/SIGTERM, then performs the graceful shutdown
	// sequence bounded by SHUTDOWN_TIMEOUT.
	bootstrap.New().Run()
}

func healthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	c := http.Client{Timeout: 3 * time.Second}
	res, err := c.Get("http://" + addr + "/health/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
