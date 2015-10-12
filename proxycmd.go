package smartproxy

import (
	"fmt"
	"smartproxy/redis"
	"strconv"
	"strings"
	"time"

	log "github.com/ngaut/logging"
)

func (s *Session) PROXY(req *redis.Request) {
	op := strings.ToLower(req.Args()[0])
	// log.Warning("PROXY ", req.Args())
	switch op {
	case "info":
		if len(req.Args()) != 1 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		s.proxyInfo(req)
	case "black":
		if len(req.Args()) < 2 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		s.proxyBlack(req)
	case "config":
		// proxy config set name value
		if len(req.Args()) < 3 || len(req.Args()) > 4 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		s.proxyConf(req)
	default:
		log.Warning("Unknow proxy op type: ", req.Args())
		err := fmt.Sprintf("-%s\r\n", UnknowProxyOpType)
		s.write2client([]byte(err))
		return
	}

}

//loglevel  idletime  mulparallel  statsd  slaveok
func (s *Session) proxyConf(req *redis.Request) {
	// proxy config set loglevel info
	// proxy config set idletime 200
	// proxy config set slaveok 1|0
	// proxy config set mulparallel 30
	// proxy config get statsd
	args := req.Args()
	// config get|set
	switch strings.ToLower(args[1]) {
	case "get":
		if len(req.Args()) != 3 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		cfgname := strings.ToLower(args[2])
		reply := s.proxyConfigGetByName(cfgname)
		s.write2client(reply)
		return
	case "set":
		if len(req.Args()) != 4 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		cfgname := strings.ToLower(args[2])
		value := strings.ToLower(args[3])
		reply := s.proxyConfigSetByName(cfgname, value)
		s.write2client(reply)
		return
	default:
		s.write2client([]byte("-wrong proxy config op type\r\n"))
		return
	}
}

// setbyname will set name by value
// return old value of name
func (s *Session) proxyConfigSetByName(name string, value string) []byte {
	var reply []byte

	switch name {
	case "loglevel":
		v := strings.ToLower(value)
		if v != "info" || v != "warning" || v != "debug" {
			reply = []byte("-loglevel must be info warning or debug\r\n")
			return reply
		}
		reply = s.proxyConfigGetByName(name)
		log.SetLevelByString(v)
	case "idletime":
		v, err := strconv.Atoi(value)
		if err != nil {
			reply = []byte("-unavailable idletime\r\n")
			return reply
		}
		if v < MinIdleTime || v > MaxIdleTime {
			reply = []byte("-unavailable idletime, must between 10 ~ 300\r\n")
			return reply
		}
		reply = s.proxyConfigGetByName("idletime")
		s.Proxy.Conf.IdleTime = int64(v)
	case "slaveok":
		v, err := strconv.Atoi(value)
		if err != nil {
			reply = []byte("-unavailable slaveok,must 0 or 1\r\n")
			return reply
		}
		if v != 0 || v != 1 {
			reply = []byte("-unavailable slaveok,must 0 or 1\r\n")
			return reply
		}
		reply = s.proxyConfigGetByName("slaveok")
		if v == 1 {
			s.Proxy.Conf.SlaveOk = true
		} else {
			s.Proxy.Conf.SlaveOk = false
		}
	case "mulparallel":
		v, err := strconv.Atoi(value)
		if err != nil {
			reply = []byte("-unavailable mulparallel\r\n")
			return reply
		}
		if v < MinMulOpParallel || v > MaxMulOpParallel {
			reply = []byte("-unavailable mulparallel, must between 5 ~ 100\r\n")
			return reply
		}
		reply = s.proxyConfigGetByName("mulparallel")
		s.Proxy.Conf.MulOpParallel = v
	case "statsd":
		reply = s.proxyConfigGetByName("statsd")
		s.Proxy.Conf.Statsd = value
	case "maxconn":
		v, err := strconv.Atoi(value)
		if err != nil {
			reply = []byte("-unavailable maxconn\r\n")
			return reply
		}
		if v < MinMaxConn || v > MaxMaxConn {
			reply = []byte("-unavailable maxconn, must between 100 ~ 60000\r\n")
			return reply
		}
		reply = s.proxyConfigGetByName("maxconn")
		s.Proxy.Conf.MaxConn = int64(v)
	default:
		reply = []byte("-wrong proxy config name\r\n")
	}
	return reply
}

func (s *Session) proxyConfigGetByName(name string) []byte {
	var reply []byte
	switch name {
	case "loglevel":
		t := log.GetLogLevel()
		s, _ := log.LogTypeToString(log.LogType(t))
		reply = redis.FormatString(s)
	case "idletime":
		time := s.Proxy.Conf.IdleTime
		reply = redis.FormatInt(int64(time))
	case "maxconn":
		time := s.Proxy.Conf.MaxConn
		reply = redis.FormatInt(int64(time))
	case "slaveok":
		if s.Proxy.Conf.SlaveOk {
			reply = redis.FormatInt(1)
		} else {
			reply = redis.FormatInt(0)
		}
	case "mulparallel":
		parallel := s.Proxy.Conf.MulOpParallel
		reply = redis.FormatInt(int64(parallel))
	case "statsd":
		statsd := s.Proxy.Conf.Statsd
		reply = redis.FormatString(statsd)
	default:
		reply = []byte("-wrong proxy config name\r\n")
	}
	return reply
}

func (s *Session) proxyInfo(req *redis.Request) {
	name := fmt.Sprintf("name:%s", s.Proxy.Conf.Name)
	id := fmt.Sprintf("id:%s", s.Proxy.Conf.Id)
	port := fmt.Sprintf("port:%s", s.Proxy.Conf.Port)
	statsd := fmt.Sprintf("statsd:%s", s.Proxy.Conf.Statsd)
	zk := fmt.Sprintf("zk:%s", s.Proxy.Conf.Zk)
	zkpath := fmt.Sprintf("zkpath:%s", s.Proxy.Conf.ZkPath)
	qps := fmt.Sprintf("qps:%d", s.Proxy.LastQPS)
	conns := fmt.Sprintf("conns:%d", len(s.Proxy.SessMgr))
	nodes := "nodes:"
	r := []string{name, id, port, statsd, zk, zkpath, qps, conns, nodes}
	for _, h := range s.Proxy.Conf.Nodes {
		hs := fmt.Sprintf("%s", h)
		r = append(r, hs)
	}
	reply := redis.FormatStringSlice(r)
	s.write2client(reply)
}

func (s *Session) proxyBlack(req *redis.Request) {
	args := strings.ToLower(req.Args()[1])
	// log.Warning(req.Args())
	switch args {
	// proxy black remove keyname
	case "remove":
		if len(req.Args()) != 3 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		// delete(BlackKeyLists, req.Args()[-1])
		key := req.Args()[2]
		if _, exists := BlackKeyLists[key]; exists {
			log.Warning("remove black key ", key)
			delete(BlackKeyLists, key)
			s.write2client(OK_BYTES)
		} else {
			s.write2client([]byte("-remove key not exists\r\n"))
		}

	case "get":
		if len(req.Args()) != 2 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		ks := make([]string, 0)
		for k, _ := range BlackKeyLists {
			ks = append(ks, k)
		}
		d := redis.FormatStringSlice(ks)
		s.write2client(d)
	case "set":
		//proxy black set 3600 keyname1
		if len(req.Args()) != 4 {
			err := fmt.Sprintf("-%s\r\n", WrongArgumentCount)
			s.write2client([]byte(err))
			return
		}
		t, err := strconv.Atoi(req.Args()[3])
		if err != nil {
			log.Warningf("black key: %s time unavailable %s", req.Args()[2], req.Args()[3])
			err := fmt.Sprintf("-%s\r\n", BlackTimeUnavaliable)
			s.write2client([]byte(err))
			return
		}
		if t > 86400 || t < 0 {
			log.Warningf("black key: %s time unavailable %s", req.Args()[2], req.Args()[3])
			s.write2client([]byte("-black time must between 0 ~ 86400\r\n"))
			return
		}
		BlackKeyLists[req.Args()[2]] = &BlackKey{
			Name:     req.Args()[2],
			Startup:  time.Now(),
			Deadline: time.Now().Add(time.Duration(t) * time.Second),
		}
		s.write2client(OK_BYTES)
		return
	default:
		err := fmt.Sprintf("-%s\r\n", UnknowProxyOpType)
		s.write2client([]byte(err))
		return
	}
	return
}
