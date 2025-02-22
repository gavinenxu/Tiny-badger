package utils

import (
	"log"
	"os"
)

type Logger interface {
	Errorf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

type loggerLevel int

const (
	DEBUG loggerLevel = iota
	INFO
	WARNING
	ERROR
)

type DefaultLogger struct {
	*log.Logger // inherit from system logger
	level       loggerLevel
}

func NewDefaultLogger(level loggerLevel) *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "tiny badger: ", log.LstdFlags),
		level:  level,
	}
}

func (l *DefaultLogger) Errorf(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.Errorf("Error: "+format, args...)
	}
}

func (l *DefaultLogger) Warningf(format string, args ...interface{}) {
	if l.level <= WARNING {
		l.Warningf("Warning: "+format, args...)
	}
}

func (l *DefaultLogger) Infof(format string, args ...interface{}) {
	if l.level <= INFO {
		l.Infof("Info: "+format, args...)
	}
}

func (l *DefaultLogger) Debugf(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.Debugf("Debug: "+format, args...)
	}
}
