package smartproxy

import (
	"bufio"
	"net"
	"smartproxy/redis"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/ngaut/logging"
)

func HandleConn(ps *ProxyServer, c net.Conn) {
	addr := c.RemoteAddr().String()
	// log.Info("start process Session, receive remote host ", addr)

	s := NewSession(ps, c)
	if int64(len(ps.SessMgr)) > ps.Conf.MaxConn {
		log.Warning("reached max connection, close ", addr)
		s.Close()
		return
	}

	ps.SessMgr[addr] = s
	defer delete(ps.SessMgr, addr)

	for {
		reqstr, err := parseReq(s.r)

		//for stats
		s.LastAccess = time.Now().UnixNano() / 1e3
		atomic.AddInt64(&s.Proxy.OpCount, 1)

		req := redis.NewRequest(reqstr)
		req.SetError(err)

		if err != nil {
			if strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "use of closed network connection") {
				// log.Warning("Session ended  by ", err.Error())
				return
			}

			e := s.Write2client(req)
			if e != nil {
				// log.Warning("Write2client ", e)
				return
			}
			continue
		}

		reply, shouldClose, handled, err := preCheckCommand(req)

		// log.Info(req, reply, shouldClose, handled, err)

		req.SetReply(reply)
		req.SetError(err)

		if err != nil || shouldClose || handled {
			s.Write2client(req)
			if shouldClose {
				// log.("should close from ", c.RemoteAddr())
				s.Close()
				return
			}
			continue
		}
		// spec command : mget mset  del inter union  .....
		if isSpecCommand(req.Name()) {
			s.SpecCommandProcess(req)
			continue
		}
		s.Forward(req)
	}
}

type Session struct {
	Conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	Proxy *ProxyServer

	LastAccess int64 // unixtime stamp
	QuitChan   chan int

	MulOpParallel int
}

func NewSession(ps *ProxyServer, conn net.Conn) *Session {
	s := &Session{
		Conn:          conn,
		r:             bufio.NewReaderSize(conn, 4096),
		w:             bufio.NewWriterSize(conn, 4096),
		Proxy:         ps,
		LastAccess:    time.Now().Unix(),
		QuitChan:      make(chan int, 1),
		MulOpParallel: ps.Conf.MulOpParallel,
	}
	return s
}

func (s *Session) Forward(req *redis.Request) {
	s.forward(req)
	s.Write2client(req)
}

func (s *Session) forward(req *redis.Request) {
	resp := s.Proxy.Dispatch(req)
	// log.Info("session forward got response: ", resp)
	req.SetResp(resp)
}

func (s *Session) Write2client(req *redis.Request) error {
	return s.write2client(req.Result())
}

func (s *Session) write2client(data []byte) error {
	defer func() {
		if e := recover(); e != nil {
			log.Warning("write2client panice: ", e)
		}
	}()
	s.w.Write(data)
	err := s.w.Flush()

	//stats
	now := time.Now().UnixNano() / 1e3
	// select must non-block
	select {
	case s.Proxy.TimeChan <- (now - s.LastAccess):
	default:
	}

	return err
}

func (s *Session) Close() {
	defer func() {
		if e := recover(); e != nil {
			log.Warning("close panic: ", e)
		}
	}()
	close(s.QuitChan)
	s.Conn.Close()
}
