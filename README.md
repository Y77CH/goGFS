# goGFS

This is a Go implementation of the Google File System according to [this famous paper](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).

This implementation is a project in my 2023 summer research of distributed system and thus will be in research / educational setting.

Implementation is separated to steps and design for each step is documented in [goGFS.md](goGFS.md) file.

There are still some functions that need to be fixed or refinements that can be made, which are available in [TODOs.md](TODOs.md)

## Run

To start a new chunkserver, use the following command:

```shell
go run server/chunkserver.go -addr <address> -dir <data directory>
```

To use the system, call APIs in the `client.go` file in app code (an example app is `app.go`), and run the following:

```shell
go run app.go client.go dummy_master.go
```

(for now, the master is not yet developed, so the `dummy_master.go` is used)