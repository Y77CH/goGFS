package main

import (
	"flag"
	"fmt"
)

// CMD
func main() {
	stype := flag.String("type", "", "Specify the type of server to be started (ms for master; cs for chunkserver)")
	addr := flag.String("addr", "", "Specify the address of the server to be started")
	dir := flag.String("dir", "", "Specify the path the server should store data")
	master := flag.String("master", "", "Specify the master address if starting a chunkserver")
	flag.Parse()

	if *stype == "cs" {
		if *addr != "" {
			if *dir != "" {
				if *master != "" {
					// valid arguments
					fmt.Println("Starting chunkserver")
					err := startChunkServer(*addr, *dir, *master)
					fmt.Println(err.Error())
				}
			}
		}
	} else if *stype == "ms" {
		if *addr != "" {
			if *dir != "" {
				// valid arguments
				fmt.Println("Starting master server")
				err := startMaster(*addr, *dir)
				fmt.Println(err.Error())
			}
		}
	}
	fmt.Println("Invalid option")
}
