package utils

import (
	"fmt"
	conf "mongoshake/collector/configure"
	"os"
	"strings"

	"github.com/nightlyone/lockfile"
	LOG "github.com/vinllen/log4go"
)

const (
	WorkGood       uint64 = 0
	GetReady       uint64 = 1
	FetchBad       uint64 = 2
	TunnelSendBad  uint64 = 4
	TunnelSyncBad  uint64 = 8
	ReplicaExecBad uint64 = 16

	ConnectModePrimary            = "primary"
	ConnectModeSecondaryPreferred = "secondaryPreferred"
	ConnectModeStandalone         = "standalone"
	MajorityWriteConcern          = "majority"

	GlobalDiagnosticPath = "diagnostic"
	// This is the time of golang was born to the world
	GolangSecurityTime = "2006-01-02T15:04:05Z"
)

// Build info
var BRANCH = "$"
var SIGNALPROFILE = "$"
var SIGNALSTACK = "$"

func init() {
	// prepare global folders
	Mkdirs(GlobalDiagnosticPath /*, GlobalStoragePath*/)
}

func AppDatabase() string {
	return conf.Options.ContextStorageDB
}

func APPConflictDatabase() string {
	return AppDatabase() + "_conflict"
}

func RunStatusMessage(status uint64) string {
	switch status {
	case WorkGood:
		return "Good"
	case GetReady:
		return "prepare for ready"
	case FetchBad:
		return "can't fetch oplog from source MongoDB"
	case TunnelSendBad:
		return "collector send oplog to tunnel failed"
	case TunnelSyncBad:
		return "receiver fetch from tunnel failed"
	case ReplicaExecBad:
		return "receiver replica executed failed"
	default:
		return "unknown"
	}
}

func InitialLogger(logDir, logFile, level string, logBuffer bool, verbose bool) error {
	logLevel := parseLogLevel(level)
	if verbose {
		LOG.AddFilter("console", logLevel, LOG.NewConsoleLogWriter())
	}

	if len(logDir) == 0 {
		logDir = "logs"
	}
	// check directory exists
	if _, err := os.Stat(logDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
			return fmt.Errorf("create log.dir[%v] failed[%v]", logDir, err)
		}
	}

	if len(logFile) != 0 {
		if logBuffer {
			LOG.LogBufferLength = 32
		} else {
			LOG.LogBufferLength = 0
		}
		fileLogger := LOG.NewFileLogWriter(fmt.Sprintf("%s/%s", logDir, logFile), true)
		fileLogger.SetRotateSize(100 * 1024 * 1024)
		fileLogger.SetRotateDaily(true)
		fileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		fileLogger.SetRotateMaxBackup(30)
		LOG.AddFilter("file", logLevel, fileLogger)

		// error log单独打印在一个文件
		errorLogFile := logFile
		lastDotIndex := strings.LastIndex(errorLogFile, ".")
		if lastDotIndex != -1 {
			errorLogFile = errorLogFile[:lastDotIndex] + "-error" + errorLogFile[lastDotIndex:]
		} else {
			errorLogFile = errorLogFile + "-error"
		}
		errorFileLogger := LOG.NewFileLogWriter(fmt.Sprintf("%s/%s", logDir, errorLogFile), true)
		errorFileLogger.SetRotateSize(100 * 1024 * 1024)
		errorFileLogger.SetRotateDaily(true)
		errorFileLogger.SetFormat("[%D %T] [%L] [%s] %M")
		errorFileLogger.SetRotateMaxBackup(30)
		LOG.AddFilter("file", logLevel, errorFileLogger)
	} else {
		return fmt.Errorf("log.file[%v] shouldn't be empty", logFile)
	}

	return nil
}

func parseLogLevel(level string) LOG.Level {
	switch strings.ToLower(level) {
	case "debug":
		return LOG.DEBUG
	case "info":
		return LOG.INFO
	case "warning":
		return LOG.WARNING
	case "error":
		return LOG.ERROR
	default:
		return LOG.DEBUG
	}
}

func WritePid(id string) (err error) {
	var lock lockfile.Lockfile
	lock, err = lockfile.New(id)
	if err != nil {
		return err
	}
	if err = lock.TryLock(); err != nil {
		return err
	}

	return nil
}

func DelayFor(ms int64) {
	YieldInMs(ms)
}
