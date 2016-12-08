package logger

import (
	"os"
	"testing"
)

var LOG_NAME = "ut.log"

func init() {
	os.Remove(LOG_NAME)
	SetRollingDaily(".", LOG_NAME)
	SetLevel(DEBUG)
	SetConsole(true)
}

func TestManual(t *testing.T) {
	Debug("this is debug", "really debug")
	Info("this is info", "really info")
	Warn("this is warn", "really warn")
	Error("this is error", "really error")
	Fatal("this is fatal", "really fatal")

	Debugf("this is %s, really %s", "debug", "debug")
	Infof("this is %s, really %s", "info", "info")
	Warnf("this is %s, really %s", "warn", "warn")
	Errorf("this is %s, really %s", "error", "error")
	Fatalf("this is %s, really %s", "fatal", "fatal")
}
