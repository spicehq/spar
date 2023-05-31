package spar

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/montanaflynn/stats"
	"github.com/spiceai/gospice/v2"
)

type Spear struct {
	Time  time.Time `json:"ts"`
	Value int       `json:"value"`
}

type SparClient struct {
	spiceClient *gospice.SpiceClient

	durationsChan chan float64
}

func NewSparClient() *SparClient {
	return &SparClient{
		spiceClient:   gospice.NewSpiceClient(),
		durationsChan: make(chan float64, runtime.NumCPU()*100),
	}
}

func (p *SparClient) Init() error {
	if err := p.spiceClient.Init("323337|b42eceab2e7c4a60a04ad57bebea830d"); err != nil {
		return fmt.Errorf("failed to initialize spice client: %w", err)
	}

	return nil
}

func (p *SparClient) Close() {
	if p.spiceClient != nil {
		p.spiceClient.Close()
	}
}

func (p *SparClient) Throw(ctx context.Context, sql string) error {
	start := time.Now()
	reader, err := p.spiceClient.FireQuery(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to query spice: %w", err)
	}
	duration := time.Since(start)
	defer reader.Release()

	rowCount := 0

	for reader.Next() {
		arr := array.RecordToStructArray(reader.Record())
		rowCount += arr.Len()
	}

	log.Printf("Got %d rows in time=%s\n", rowCount, duration.Round(time.Microsecond))

	p.durationsChan <- float64(duration)

	return nil
}

func (p *SparClient) PrintStats() error {
	durations := make([]float64, len(p.durationsChan))

	for i := 0; i < len(durations); i++ {
		durations[i] = <-p.durationsChan
	}

	sort.SliceStable(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	mean, err := stats.Mean(durations)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("count=%d\n", len(durations))
	fmt.Printf("min=%s\n", time.Duration(durations[0]))
	fmt.Printf("max=%s\n", time.Duration(durations[len(durations)-1]))
	fmt.Printf("avg=%s\n", time.Duration(mean))

	return nil
}
