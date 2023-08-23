package main

import (
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// CMD
func main() {
	stype := flag.String("type", "", "Specify the type of server to be started (ms for master; cs for chunkserver)")
	addr := flag.String("addr", "", "Specify the address of the server to be started")
	dir := flag.String("dir", "", "Specify the path the server should store data")
	master := flag.String("master", "", "Specify the master address if starting a chunkserver")
	flag.Parse()

	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	), zap.AddCaller())
	defer logger.Sync()
	atom.SetLevel(zap.DebugLevel)
	undo := zap.ReplaceGlobals(logger)
	defer undo()

	if *stype == "cs" {
		if *addr != "" {
			if *dir != "" {
				if *master != "" {
					// valid arguments
					zap.L().Info("Starting chunkserver")
					err := startChunkServer(*addr, *dir, *master)
					fmt.Println(err.Error())
				}
			}
		}
	} else if *stype == "ms" {
		if *addr != "" {
			if *dir != "" {
				// valid arguments
				zap.L().Info("Starting master server")
				err := startMaster(*addr, *dir)
				if err != nil {
					fmt.Println(err.Error())
				}
			}
		}
	}
}
