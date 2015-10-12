package util

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/ngaut/logging"
)

func RegisterSignalAndWait() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGINT)

	quit := <-sc
	log.Warning("Receive signal ", quit.String())
}
