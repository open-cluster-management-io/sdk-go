package mqtt

import (
	"fmt"

	"github.com/eclipse/paho.golang/paho/log"
	"k8s.io/klog/v2"
)

type PahoDebugLogger struct {
}

var _ log.Logger = &PahoDebugLogger{}

func (l *PahoDebugLogger) Println(v ...interface{}) {
	fmt.Println("-------------")
	fmt.Println(v...)
	fmt.Println("-------------")
	klog.V(4).Info(v...)
}

func (l *PahoDebugLogger) Printf(format string, v ...interface{}) {
	fmt.Println("-------------")
	fmt.Printf(format, v...)
	fmt.Println("-------------")
	klog.V(4).Infof(format, v...)
}
