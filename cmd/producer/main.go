// Command producer is a load generator: it fires N enqueue requests at the API,
// optionally rate-limited, and reports throughput. Use it to demo scaling and to
// produce the benchmark numbers in the README.
//
//	go run ./cmd/producer -n 50000           # blast 50k jobs as fast as possible
//	go run ./cmd/producer -n 1000 -rate 200  # 1000 jobs at 200/sec
//	go run ./cmd/producer -n 100 -fail-times 2   # exercise the retry path
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		n          = flag.Int("n", 100, "number of jobs to enqueue")
		rate       = flag.Int("rate", 0, "max jobs per second (0 = unlimited)")
		conc       = flag.Int("c", 50, "number of concurrent HTTP clients")
		jobType    = flag.String("type", "send_email", "job type")
		apiURL     = flag.String("url", "http://localhost:8080", "API base URL")
		sleepMS    = flag.Int("sleep-ms", 0, "simulated work per job (handler sleep)")
		failTimes  = flag.Int("fail-times", 0, "handler fails this many attempts, then succeeds")
		failAlways = flag.Bool("fail-always", false, "handler always fails (drives jobs to the DLQ)")
	)
	flag.Parse()

	payload := map[string]any{}
	if *sleepMS > 0 {
		payload["sleep_ms"] = *sleepMS
	}
	if *failTimes > 0 {
		payload["fail_times"] = *failTimes
	}
	if *failAlways {
		payload["fail_always"] = true
	}
	body, _ := json.Marshal(map[string]any{"type": *jobType, "payload": payload})

	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256},
	}

	var ok, failed int64
	jobs := make(chan struct{}, *conc)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req, _ := http.NewRequest(http.MethodPost, *apiURL+"/v1/jobs", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusAccepted {
					atomic.AddInt64(&ok, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}

	var ticker *time.Ticker
	if *rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
	}

	fmt.Printf("enqueuing %d %q jobs (concurrency=%d, rate=%d)...\n", *n, *jobType, *conc, *rate)
	start := time.Now()
	for i := 0; i < *n; i++ {
		if ticker != nil {
			<-ticker.C
		}
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	throughput := float64(atomic.LoadInt64(&ok)) / elapsed.Seconds()
	fmt.Printf("done: enqueued=%d failed=%d elapsed=%s throughput=%.0f jobs/sec\n",
		ok, failed, elapsed.Round(time.Millisecond), throughput)
}
