package main

import (
	"flag"

	log "github.com/ngaut/logging"
	proxy "smartproxy"
	"smartproxy/util"
)

var (
	cfg = flag.String("config_file", "example.ini", "smart proxy config file")
)

func main() {
	flag.Parse()

	proxyConf := proxy.NewProxyConfig(*cfg)
	log.Info(proxyConf)

	s := proxy.NewProxyServer(proxyConf)
	s.Init()
	s.Wg.Wrap(s.Run)
	s.Wg.Wrap(s.QpsStats)
	s.Wg.Wrap(s.QpsSend)
	s.Wg.Wrap(s.SaveConfigToFile)

	util.RegisterSignalAndWait()

	s.Close()
	log.Warning("quit redis proxy")
}
