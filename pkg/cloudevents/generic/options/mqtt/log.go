package mqtt

import (
	"github.com/eclipse/paho.golang/paho/log"
	"k8s.io/klog/v2"
)

type PahoDebugLogger struct {
}

var _ log.Logger = &PahoDebugLogger{}

func (l *PahoDebugLogger) Println(v ...interface{}) {
	klog.V(4).Info(v...)
}

func (l *PahoDebugLogger) Printf(format string, v ...interface{}) {
	klog.V(4).Infof(format, v...)
}
