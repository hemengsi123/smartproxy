package smartproxy

import (
	"fmt"
	"smartproxy/redis"
	"sync"

	log "github.com/ngaut/logging"
)

func (s *Session) SpecCommandProcess(req *redis.Request) {
	// log.Info("Spec command Process ", req)

	switch req.Name() {
	case "SINTERSTORE":
		s.SINTERSTORE(req)
	case "SMOVE":
		s.SMOVE(req)
	case "DEL":
		s.DEL(req)
	case "RPOPLPUSH":
		s.RPOPLPUSH(req)
	case "SDIFFSTORE":
		s.SDIFFSTORE(req)
	case "SINTER":
		s.SINTER(req)
	case "SDIFF":
		s.SDIFF(req)
	case "MGET":
		s.MGET(req)
	case "ZINTERSTORE":
		s.ZINTERSTORE(req)
	case "ZUNIONSTORE":
		s.ZUNIONSTORE(req)
	case "RENAME":
		s.RENAME(req)
	case "RENAMENX":
		s.RENAMENX(req)
	case "MSET":
		s.MSET(req)
	case "MSETNX":
		s.MSETNX(req)
	case "PROXY":
		s.PROXY(req)
	default:
		log.Fatalf("Unknown Spec Command: %s, we won't expect this happen ", req.Name())
	}
}

//we will finish these commands later
func (s *Session) MSETNX(req *redis.Request)      { s.write2client(OK_BYTES) }
func (s *Session) ZUNIONSTORE(req *redis.Request) { s.write2client(OK_BYTES) }
func (s *Session) RENAMENX(req *redis.Request)    { s.write2client(OK_BYTES) }
func (s *Session) SDIFF(req *redis.Request)       { s.write2client(OK_BYTES) }
func (s *Session) SINTER(req *redis.Request)      { s.write2client(OK_BYTES) }
func (s *Session) SINTERSTORE(req *redis.Request) { s.write2client(OK_BYTES) }
func (s *Session) RENAME(req *redis.Request)      { s.write2client(OK_BYTES) }
func (s *Session) RPOPLPUSH(req *redis.Request)   { s.write2client(OK_BYTES) }
func (s *Session) SDIFFSTORE(req *redis.Request)  { s.write2client(OK_BYTES) }
func (s *Session) SMOVE(req *redis.Request)       { s.write2client(OK_BYTES) }
func (s *Session) ZINTERSTORE(req *redis.Request) { s.write2client(OK_BYTES) }

func (s *Session) MSET(req *redis.Request) {
	pair := req.Args()
	if len(pair)%2 != 0 {
		err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
		s.write2client([]byte(err))
		return
	}

	p := make(chan int, s.MulOpParallel)
	for i := 0; i < s.MulOpParallel; i++ {
		p <- 1
	}

	defer func() {
		close(p)
	}()
	wg := sync.WaitGroup{}
	wg.Add(len(pair) / 2)
	partialErr := 0
	// we just ignore return code, MSET reuturn OK unless anyone set error
	for i := 0; i < len(pair); i += 2 {
		go func(k string, v string) {
			<-p
			// log.Info("In MSET goroutine ", k, v)
			cmdslice := []string{"SET", k, v}
			r := redis.NewRequest(cmdslice)
			resp := s.Proxy.Backend.OnSET(r)
			if resp.Err() != nil && resp.Err() != redis.Nil {
				// log.Warning("MSET error ", cmdslice, resp.Err())
				partialErr += 1
			}
			p <- 1
			wg.Done()
		}(pair[i], pair[i+1])
	}
	wg.Wait()

	if partialErr == 0 {
		s.write2client(OK_BYTES)
	} else {
		d := fmt.Sprintf("- %d MSET failed, partial key/value %d set\r\n", partialErr, len(pair)/2-partialErr)
		s.write2client([]byte(d))
	}
}

func (s *Session) MGET(req *redis.Request) {
	p := make(chan int, s.MulOpParallel)
	for i := 0; i < s.MulOpParallel; i++ {
		p <- 1
	}

	defer func() {
		close(p)
	}()

	keys := req.Args()
	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	// we should ensure the KEY's order
	result := make([][]byte, len(keys))

	for idx, key := range keys {
		go func(key string, idx int) {
			<-p
			// log.Info("In MGET goroutine ", key)
			cmdslice := []string{"GET", key}
			r := redis.NewRequest(cmdslice)
			resp := s.Proxy.Backend.OnGET(r)
			result[idx] = resp.Reply()
			p <- 1
			wg.Done()
		}(key, idx)
	}

	wg.Wait()
	mergeResp := []byte(fmt.Sprintf("*%d\r\n", len(keys)))
	for _, res := range result {
		mergeResp = append(mergeResp, res...)
	}
	// log.Info("MGET merger resp ", string(mergeResp))
	s.write2client(mergeResp)
}

func (s *Session) DEL(req *redis.Request) {
	var result int64
	// 串行会很慢，可以考滤开goroutine并行执行
	// 但是这个goroutine量一定要控制，不能有多少key就多少goroutine
	p := make(chan int, s.MulOpParallel)
	for i := 0; i < s.MulOpParallel; i++ {
		p <- 1
	}

	defer func() {
		close(p)
	}()

	keys := req.Args()
	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	for _, key := range keys {
		go func(key string) {
			<-p
			// log.Info("In DEL goroutine ", key)
			cmdslice := []string{"DEL", key}
			r := redis.NewRequest(cmdslice)
			resp := s.Proxy.Backend.OnDEL(r)
			result += resp.Val()
			p <- 1
			wg.Done()
		}(key)
	}

	wg.Wait()
	mergeResp := redis.FormatInt(result)
	// log.Info("DEL merger resp ", mergeResp, result)
	s.write2client(mergeResp)
}
