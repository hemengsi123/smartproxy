package smartproxy

import (
	"net"
	"reflect"
	"runtime"
	"smartproxy/redis"
	"smartproxy/util"
	"strings"
	"sync"
	"time"

	log "github.com/ngaut/logging"
)

type ProxyServer struct {
	//net listen tcp4
	Listen net.Listener

	//proxy config struct
	Conf *ProxyConfig

	//redis cluster client
	Backend *redis.ClusterClient

	Lock        sync.Mutex
	SessMgr     map[string]*Session
	RedisMethod map[string]reflect.Value

	Quit    chan bool
	Wg      util.WaitGroupWrapper
	Startup time.Time

	//stats
	TimeChan chan int64
	QpsChan  chan int64
	LastQPS  int64
	OpCount  int64
}

func NewProxyServer(c *ProxyConfig) *ProxyServer {
	opt := &redis.ClusterOptions{
		Addrs:    c.Nodes,
		PoolSize: c.PoolSizePerNode,
	}

	ps := &ProxyServer{
		Conf:        c,
		Quit:        make(chan bool, 1),
		Backend:     redis.NewClusterClient(opt),
		SessMgr:     make(map[string]*Session, 1024),
		RedisMethod: make(map[string]reflect.Value, 120),
		Startup:     time.Now(),
		TimeChan:    make(chan int64, 1024),
		QpsChan:     make(chan int64, 1024),
	}

	go ps.ExpireClient()
	return ps
}

func (ps *ProxyServer) Dispatch(req *redis.Request) redis.Cmder {

	name := req.Name()

	method, ok := ps.RedisMethod[name]

	if !ok {
		method = reflect.ValueOf(ps.Backend).MethodByName("On" + name)
		ps.RedisMethod[name] = method
	}

	if method.IsValid() {
		in := []reflect.Value{reflect.ValueOf(req)}
		callResult := method.Call(in)
		if callResult[0].Interface() != nil {
			return callResult[0].Interface().(redis.Cmder)
		}
	} else {
		return ps.Backend.OnReflectUnvalid(req)
	}
	return ps.Backend.OnUnDenfined(req)
}

func (ps *ProxyServer) ExpireClient() {
	ticker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-ps.Quit:
			goto quit
		case <-ticker.C:
			now := time.Now().Unix()
			for addr, s := range ps.SessMgr {
				log.Infof("%s session idle time %d", addr, now-s.LastAccess/1e6)
				if now-s.LastAccess/1e6 > ps.Conf.IdleTime {
					log.Warningf("session %s time out, we close forcely", s.Conn.RemoteAddr().String())
					ps.Lock.Lock()
					delete(ps.SessMgr, addr)
					ps.Lock.Unlock()
					s.Close()
				}
			}
		}

	}
quit:
	log.Info("quit Proxy ExpireClient")
}

func (ps *ProxyServer) Close() {

	err := ps.Listen.Close()
	if err != nil {
		log.Warning("Close Listener err ", err)
	}
	log.Info("Proxy Server Close Listener ")
	close(ps.Quit)
	ps.Wg.Wait()
	log.Warning("Proxy Server Close ....")
}

func (ps *ProxyServer) Init() {
	log.Info("Proxy Server Init ....")

	l, err := net.Listen("tcp4", "0.0.0.0:"+ps.Conf.Port)
	// net.Listen(net, laddr)
	if err != nil {
		log.Fatalf("Proxy Server Listen on port : %s failed ", ps.Conf.Port)
	}
	log.Info("Proxy Server Listen on port ", ps.Conf.Port)
	ps.Listen = l
}

func (ps *ProxyServer) Run() {

	log.Info("Proxy Server Run ....")
	for {
		conn, err := ps.Listen.Accept()
		if err != nil {
			log.Warning("got error when Accept network connect ", err)
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Warningf("NOTICE: temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Warningf("ERROR: listener.Accept() - %s", err)
			}
			break
		}

		go HandleConn(ps, conn)
	}
}
