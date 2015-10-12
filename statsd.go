package smartproxy

import (
	"math"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	log "github.com/ngaut/logging"
	"smartproxy/statsd"
)

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func percentile(perc float64, arr []uint64, length int) uint64 {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Floor(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

func (p *ProxyServer) QpsStats() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			p.LastQPS = atomic.SwapInt64(&p.OpCount, 0)

			select {
			//non-block sending
			case p.QpsChan <- p.LastQPS:
			default:
			}
		case <-p.Quit:
			goto quit
		}
	}
quit:
	log.Warning("quit QPS stats loop")
}

func (p *ProxyServer) QpsSend() {
	for {
		client := statsd.NewClient(p.Conf.Statsd, p.Conf.StatsdPrefix)
		err := client.CreateSocket()
		if err != nil {
			// log.Warningf("ERROR: failed to create UDP socket to statsd(%s)", client)
			continue
		}
		select {
		case t := <-p.TimeChan:
			client.Timing(".time", t)
			// log.Info("get response rtt ", t)

		case qps := <-p.QpsChan:
			client.Incr("qps", qps)
			// log.Info("get response qps ", qps)
		case <-p.Quit:
			goto quit
		}
		client.Close()
	}
quit:
	log.Warning("quit Qps Send loop")
}

func (p *ProxyServer) StatsdMemStats() {
	ticker := time.NewTicker(10 * time.Second)
	var lastMemStats runtime.MemStats

	for {
		select {
		case <-ticker.C:
			client := statsd.NewClient(p.Conf.Statsd, p.Conf.StatsdPrefix)
			err := client.CreateSocket()
			if err != nil {
				// log.Warningf("ERROR: failed to create UDP socket to statsd(%s)", client)
				continue
			}

			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			// sort the GC pause array
			length := len(memStats.PauseNs)
			if int(memStats.NumGC) < length {
				length = int(memStats.NumGC)
			}
			gcPauses := make(Uint64Slice, length)
			copy(gcPauses, memStats.PauseNs[:length])
			sort.Sort(gcPauses)

			client.Gauge("mem.heap_objects", int64(memStats.HeapObjects))
			client.Gauge("mem.heap_idle_bytes", int64(memStats.HeapIdle))
			client.Gauge("mem.heap_in_use_bytes", int64(memStats.HeapInuse))
			client.Gauge("mem.heap_released_bytes", int64(memStats.HeapReleased))
			client.Gauge("mem.gc_pause_usec_100", int64(percentile(100.0, gcPauses, len(gcPauses))/1000))
			client.Gauge("mem.gc_pause_usec_99", int64(percentile(99.0, gcPauses, len(gcPauses))/1000))
			client.Gauge("mem.gc_pause_usec_95", int64(percentile(95.0, gcPauses, len(gcPauses))/1000))
			client.Gauge("mem.next_gc_bytes", int64(memStats.NextGC))
			client.Incr("mem.gc_runs", int64(memStats.NumGC-lastMemStats.NumGC))

			lastMemStats = memStats
			client.Close()
		case <-p.Quit:
			goto quit
		}

	}
quit:
	log.Warning("quit StatsdMemStats loop")
}
