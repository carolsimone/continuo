package metrics

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// streamCollector reads each watched stream's length and Redis memory usage
// when scraped.
type streamCollector struct {
	rc      *goredis.Client
	streams []string
	length  *prometheus.Desc
	used    *prometheus.Desc
	maxmem  *prometheus.Desc
}

// WatchStreams registers a collector that reports the length of every stream in
// streams plus Redis used and maximum memory, read from Redis at scrape time.
func WatchStreams(reg *pkgmetrics.Registry, rc *goredis.Client, streams []string) {
	reg.Register(&streamCollector{
		rc:      rc,
		streams: streams,
		length: prometheus.NewDesc("continuo_stream_length", "Entries currently held by a Redis stream, read when scraped.",
			[]string{"stream"}, nil),
		used: prometheus.NewDesc("continuo_redis_used_memory_bytes", "Bytes of memory Redis reports in use, read when scraped.", nil, nil),
		maxmem: prometheus.NewDesc("continuo_redis_maxmemory_bytes",
			"Redis memory limit in bytes; 0 means no limit. Read when scraped.", nil, nil),
	})
}

func (c *streamCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.length
	ch <- c.used
	ch <- c.maxmem
}

func (c *streamCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pipe := c.rc.Pipeline()
	lens := make([]*goredis.IntCmd, len(c.streams))
	for i, s := range c.streams {
		lens[i] = pipe.XLen(ctx, s)
	}
	info := pipe.Info(ctx, "memory")
	if _, err := pipe.Exec(ctx); err != nil {
		ch <- prometheus.NewInvalidMetric(c.length, err)
		return
	}
	used, limit, err := parseMemory(info.Val())
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.used, err)
		return
	}
	for i, s := range c.streams {
		ch <- prometheus.MustNewConstMetric(c.length, prometheus.GaugeValue, float64(lens[i].Val()), s)
	}
	ch <- prometheus.MustNewConstMetric(c.used, prometheus.GaugeValue, used)
	ch <- prometheus.MustNewConstMetric(c.maxmem, prometheus.GaugeValue, limit)
}

// parseMemory extracts used_memory and maxmemory from an INFO memory reply.
func parseMemory(info string) (used, limit float64, err error) {
	var haveUsed bool
	sc := bufio.NewScanner(strings.NewReader(info))
	for sc.Scan() {
		key, val, ok := strings.Cut(strings.TrimSpace(sc.Text()), ":")
		if !ok {
			continue
		}
		switch key {
		case "used_memory":
			if used, err = strconv.ParseFloat(val, 64); err != nil {
				return 0, 0, fmt.Errorf("parse used_memory %q: %w", val, err)
			}
			haveUsed = true
		case "maxmemory":
			if limit, err = strconv.ParseFloat(val, 64); err != nil {
				return 0, 0, fmt.Errorf("parse maxmemory %q: %w", val, err)
			}
		}
	}
	if !haveUsed {
		return 0, 0, fmt.Errorf("INFO memory reply has no used_memory")
	}
	return used, limit, nil
}
