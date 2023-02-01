package spar

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/montanaflynn/stats"
	"github.com/valyala/fasthttp"
)

type Spear struct {
	Time  time.Time `json:"ts"`
	Value int       `json:"value"`
}

type SparClient struct {
	uri    *fasthttp.URI
	client *fasthttp.Client

	durationsChan chan float64
}

func NewSparClient(uri *fasthttp.URI) *SparClient {
	return &SparClient{
		uri:           uri,
		client:        &fasthttp.Client{},
		durationsChan: make(chan float64, runtime.NumCPU()*100),
	}
}

func (p *SparClient) Throw() error {
	start := time.Now()

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetURI(p.uri)
	req.Header.SetMethod("POST")

	randVal := rand.Intn(5)
	if randVal > 2 {
		randVal = 1
	} else {
		randVal = -1
	}

	randTime := rand.Intn(20 * 1000)

	spear := Spear{
		Time:  time.Now().Add(-10 * time.Second).Add(time.Duration(randTime)),
		Value: randVal,
	}

	if err := json.NewEncoder(req.BodyWriter()).Encode(spear); err != nil {
		return err
	}

	if err := p.client.Do(req, resp); err != nil {
		return err
	}

	statusCode := resp.StatusCode()
	statusText := http.StatusText(statusCode)

	var status aurora.Value
	if statusCode >= 200 && statusCode < 300 {
		status = aurora.BrightGreen(statusText)
	} else if statusCode >= 400 && statusCode < 500 {
		status = aurora.BrightYellow(statusText)
	} else {
		status = aurora.BrightRed(statusText)
	}

	duration := time.Since(start)
	p.durationsChan <- float64(duration)

	body := resp.Body()
	content := " " + strings.TrimSpace(strings.SplitN(string(body), "\n", 2)[0])
	contentLength := len(body)

	fmt.Printf("%s (%d bytes) from %s: time=%s%s\n", status, contentLength, aurora.BrightBlue(string(req.Host())), duration.Round(time.Microsecond), content)

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
