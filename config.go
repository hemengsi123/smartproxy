package smartproxy

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/astaxie/beego/config"
	log "github.com/ngaut/logging"
)

type ProxyConfig struct {
	Id              string   // uniq id
	Name            string   // product name
	Port            string   // proxy listen port
	Nodes           []string // redis node like 127.0.0.1:6379
	SlaveOk         bool     // if we can read from slave
	IdleTime        int64
	MaxConn         int64
	MulOpParallel   int
	PoolSizePerNode int

	Statsd       string // statsd addr
	StatsdPrefix string

	Zk     string
	ZkPath string

	FileName string
	Config   config.ConfigContainer
}

func NewProxyConfig(filename string) *ProxyConfig {
	c, err := config.NewConfig("ini", filename)
	if err != nil {
		log.Fatal("read config file failed ", err)
	}

	loglevel := c.DefaultString("log::loglevel", "info")
	log.SetLevelByString(loglevel)

	logfile := c.DefaultString("log::logfile", "")
	if logfile != "" {
		err := log.SetOutputByName(logfile)
		if err != nil {
			log.Fatal("Set log Output failed ", err)
		}
		log.Info("Set log Output to file ", logfile)
		log.SetRotateByDay()
	}

	cpus := c.DefaultInt("proxy::cpus", 4)
	log.Info("set runtime GOMAXPROCS to ", cpus)
	runtime.GOMAXPROCS(cpus)

	pc := &ProxyConfig{
		Id:              c.DefaultString("product::id", ""),
		Name:            c.DefaultString("product::name", ""),
		Port:            c.DefaultString("proxy::port", ""),
		SlaveOk:         c.DefaultBool("proxy::slaveok", false),
		IdleTime:        c.DefaultInt64("proxy::idletime", 300),
		MaxConn:         c.DefaultInt64("proxy::maxconn", 60000),
		Statsd:          c.DefaultString("proxy::statsd", ""),
		Zk:              c.DefaultString("zk::zk", ""),
		ZkPath:          c.DefaultString("zk::zkpath", ""),
		MulOpParallel:   c.DefaultInt("proxy::mulparallel", 10),
		PoolSizePerNode: c.DefaultInt("proxy::poolsizepernode", 30),
		StatsdPrefix:    c.DefaultString("proxy::prefix", "redis.proxy."),
		FileName:        filename,
	}

	pc.Config = c

	nodes := c.DefaultString("proxy::nodes", "")
	if nodes == "" {
		log.Fatal("proxy nodes must not empty ")
	}
	pc.Nodes = strings.Split(nodes, ",")

	if pc.Id == "" || pc.Name == "" || pc.Port == "" {
		log.Fatal("id name or port must not empty")
	}

	if pc.PoolSizePerNode < MinPoolSizePerNode || pc.PoolSizePerNode > MaxPoolSizePerNode {
		log.Info("Adjust PoolSizePerNode to 30")
		pc.PoolSizePerNode = 30
	}

	if pc.MulOpParallel < MinMulOpParallel || pc.MulOpParallel > MaxMulOpParallel {
		log.Info("Adjust MulOpParallel to 10")
		pc.MulOpParallel = 10
	}
	if pc.MaxConn < MinMaxConn || pc.MaxConn > MaxMaxConn {
		log.Info("Adjust MaxConn to 60000")
		pc.MaxConn = 60000
	}

	if pc.IdleTime < MinIdleTime || pc.IdleTime > MaxIdleTime {
		log.Info("Adjust MaxConn to 300")
		pc.IdleTime = 300
	}

	fcpu := c.DefaultString("debug::cpufile", "")
	if fcpu != "" {
		f, err := os.Create(fcpu)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	fmem := c.DefaultString("debug::memfile", "")
	if fmem != "" {
		f, err := os.Create(fmem)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
	}
	return pc
}

func (ps *ProxyServer) SaveConfigToFile() {
	ticker := time.NewTicker(3600 * time.Second)
	for {
		select {
		case <-ticker.C:
			newaddr := ps.Backend.GetAddrs()
			oldaddr := ps.Conf.Nodes
			if (len(newaddr) != len(oldaddr)) && (len(newaddr) != 0) {

				ps.Conf.Nodes = newaddr
				// persistent nodes info
				nodes := strings.Join(newaddr, ",")
				log.Warning("addr changed to ", nodes)
				ps.Conf.Config.Set("proxy::nodes", nodes)
				err := ps.Conf.Config.SaveConfigFile(ps.Conf.FileName)
				if err != nil {
					log.Warning("persistent config failed ", err)
				}
			}
		case <-ps.Quit:
			goto quit
		}

	}
quit:
	log.Warning("quit SaveConfigToFile...")
}
