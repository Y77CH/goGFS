# goGFS

This is a Go implementation of the Google File System according to [this famous paper](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).

This implementation is a project in my 2023 summer research of distributed system and thus will be in research / educational setting.

Implementation is separated to steps and design for each step is documented in the [changelog](#Changelog) section.

There are still some functions that need to be fixed or refinements that can be made, which are available in [TODO](#TODO) section.

## Run

### VSCode Integration

An easy way to run the system locally (single machine) is to use VSCode's run & debugging function. By opening this folder as a workspace, the `launch.json` I defined will be automatically loaded, and you will see the services I have defined. You can easily run them by selecting the desired service and conveniently add breakpoints if debug is necessary. Note that you may need to change relevant arguments like data directory, `dir`.

### Manual

You can also run the system through command line which is what you should be doing when running the system in a distributed environment.

Start the master server using the following:

```shell
go run server/start.go server/chunkserver.go server/master.go server/shared.go -type ms -addr <address> -dir <data directory>
```

To start a new chunkserver, use the following command:

```shell
go run server/start.go server/chunkserver.go server/master.go server/shared.go -type cs -addr <address> -dir <data directory> -master <master address>
```

To use the system, call APIs (defined in the `client.go` file) in your app code (an example app is `app.go`), and run the following:

```shell
go run app.go client.go
```

## Changelog

Overall design choices will be documented here; specific technical implementations (eg. whether a variable should be `int64` or `int`) will be found in code comments.

Each version will be self-contained (i.e. runnable).

General roadmap (may change):

* In 0.1.X and 0.2.X, I have implemented the chunkserver.
* In 0.3.X, I have developed the master, but only its core function: maintain metadata. This is an important design of GFS, because it separates data flow and control flow.
* After that, in 0.4.X, I have developed the lease mechanism. This is a core feature, because it (to some extent) solves the concurrent write issue which may lead to data inconsistency (different write order).
* Next, in 0.5.X, I have added the `create` and `delete` calls **without** namespace management yet.
* In the 1.0 version, I'm completing the recovery feature: the master should be able to recover by replaying operation log.
* In the 1.1.X version, I will be benchmarking the current implementation and fixing functionality bugs along the way.

### goGFS v1.1

In this version, I will deploy the system to [CloudLab](https://www.cloudlab.us) which simulates a bare metal real world cluster, and collect basic data of the current implementation. A detailed list of analysis is documented below:

* Whether the entire system works (functionality, recovery).
* Collect speed of read and write, and also network limit.
* Whether the system works under large load (large data size, large amount of clients).

Meanwhile, I have done following refinements:

* Split reads: when the read crosses chunk boundary, the client automatically separates the request into two
* Split write: when the write crosses boundary, the client will first write to the end; and then create new chunk(s) for writing

### goGFS v1.0

Chunkserver recovery has been partially completed in 0.3.X where chunkservers are able to register at startup / restart, and the master will use heartbeat to ensure liveness of each chunkserver. However, in this version, I will enhance this feature such that when the chunkserver starts / restarts and do not receive heartbeat within twice the heartbeat interval, it will try reconnection.

In addition, I will try to implement the recovery of master by writing and replaying operation log.

(Overriding Change) In the previous versions, I attempted to save metadata of master in each operation by parsing and writing part of the in-memory data structure directly to file. That is not feasible if the cluster is big enough. Therefore, I will be overriding that `writeMeta` and the part in `startMaster` that reads metadata to logging steps of change in the namespace and the chunk mapping.

Right now, the system will have the following logging:

```
NMSP_ADD <path>: add <path> to namespace
NMSP_DEL <path>: delete file <path> (with its chunks) from the namespace
FILE_ADD <path> <handle>: add chunk with handle <handle> to file <path>
```

### goGFS v0.5

In this version, I have developed a minimal version of create and delete where the namespace manager is not yet involved.

(Bug Fix) One problem that I did not notice is that when the system first starts, the master will not record chunkservers even if they register. Because the master stores servers in a map where filename is the key, servers that do not have any file will not be recorded in the map. To address this issue, I created a special entry in the chunk mappings stored by master: the master will have an empty string (`""`) key which is reserved to hold all the idle chunkservers.

### goGFS v0.4

This function is actually half developed in 0.2 where I have already implemented the chunkserver side.

The remaining part is essentially four tasks:

* Master reassigns primary if lease expired
* Client sends all requests to primary until lease expires
* Chunkserver asks for extension in heartbeat if  receiving requests when lease expire

Specification on primary assignment: the only condition to check is expiration. If the lease has been granted but expired, then the master should reset lease expiration to now+10s; if never granted, the lease expiration time will be zero time, which can also viewed as lease expired (but long ago).

A primary principle I followed when designing chunkserver is keeping in-memory data structures minimum, as it will increase the complexity of locking. Therefore, I kept lease expiration as an argument in each request which will therefore avoid chunkserver, client, master all to keep record of lease expiration.

#### Test Explained

* Client starts a read call. Because no primary is recorded (server just started), it will ask for primary before write.
* Then the client waits for 3 seconds (can be arbitrary value), and starts a read. Because the primary has been recorded, and not yet expired, the primary should request the same primary.
* Next, the client waits for 12 seconds and starts an append. Because the recorded primary has expired, the client should ask the master again for a new primary (can be same server but with different expire time).

### goGFS v0.3.1

Enhance the master: a core feature of master is to maintain metadata.

In this version, I will enable the master to regularly make heartbeat rpc call to poll chunk info from known chunkservers.

At the same time, in order for the heartbeat messages to be useful, version number will be added.

Because the paper did not make explicit claims about how does the version number work, I will note down my design here:

* Chunkserver starts -> load persistent `versions.json` -> register to master
* Chunkserver writes successful -> write to `versions.json`
* Master heartbeats chunkserver -> chunkserver loads `versions.json` and reply

### goGFS v0.3

Implement a minimal master that can respond to client asking for chunk locations.

In order to implement the above function, the master has to be able to:

* Start up and load metadata (mapping between filename and chunk metadata needs to be available for master to receive chunkserver registration and respond chunk location)
* Accept chunkserver registration (required for reading)

Due to the fact that I'm creating a master (instead of a dummy one), I will move all server side definitions to a separated file (otherwise, defining all data types in chunkserver / master is not reasonable).

At the same time, because the both master and chunkserver need to be started as single processes, I will need to create a separate start go file as program entrance point.

Because this is a minimal implementation, I won't be adding version number support for now.

#### Design Considerations

One design that is not explicitly described in the paper is how does the chunkserver know its chunks' version numbers (at startup or reconnecting), and whether this information is stored persistently.

### goGFS v0.2

Implement the record append and write function of chunkserver and relevant part of the client with a dummy master.

#### Workflow of a Write (without Master)

1. Client asks master for primary and secondary replicas.
2. Master replies with the primary and secondary replicas.
   The client caches data for future mutations.
3. The client pushes the data to all the replicas.
   Each chunkserver stores data in an internal LRU buffer cache until data used or aged out.
4. Once all the replicas acknowledged receiving data,
   the client sends a write request to primary where data pushed earlier are identified.
   The primary assigns consecutive serial numbers to all mutations
   and applies mutation to local in the serialized order.
5. The primary forwards the write to all secondary,
   and each secondary applies mutation with the serialized order.
6. All secondaries reply to primary regarding if they have completed the operation.
7. The primary replies to the client. If there is any error, the write fails and the client has to retry.

#### Notes

Because I wish to implement client and chunkserver first (which is why current version is `0.x`),
the dummy master will always pick the chunkserver at `127.0.0.1:12345` as the primary.

### goGFS v0.1.2

Update the server such that it will handle concurrent read, instead of blocking new requests.

### goGFS v0.1.1

Change the project structure such that servers will be launched separatedly as new processes
which better simulates multi-machine distributed system.

#### Notes

This comes at the cost of completely separating the client and server side which leads to the issue that,
for example, RPC call and reply data structures have to be redefined.
However, I do believe that this is acceptable.

### goGFS v0.1

Implement the read function in chunkserver and relevant part of the client with a dummy master.

#### Workflow of a Read

1. Client calls dummy master with file name, chunk index (file size divided by chunk size)
   and dummy master replies with the chunk handle and dummy list of replicas.
2. Client sends chunk handle and byte range (i.e. starting offset + length of read) to chunkserver.
3. Chunkserver replies with the corresponding chunk data.

#### Design Considerations

I understand that for a real distributed system, the gfs servers should be started as services
(i.e. in another process, and preferrably from other machines).
However, for this initial version, I will be starting them only as go routines for simplicity.

Instead of using a custom bit map, or workarounds like `big.Int`, I decided to use `int64` for chunk handle,
as there is no real scenario where I need to, for example, set an individual bit or do bit arithmetic.

#### Notes

* RPC parts referred to the Princeton COS418 [slide](https://www.cs.princeton.edu/courses/archive/spring21/cos418/docs/precept3_rpcs_in_go.pdf).
* Quick Reference: 1 MB = 1024*1024 = 1048576 bytes

## TODO

There are some features / edge cases that are important but may not be implemented before large portion of the work is done.

- [ ] Separate the read / write if it is crossing chunk boundary
- [ ] Implement "read from closest" and pipelining during data push (paper 3.2)
- [ ] Implement primary serializing mutations via batching
- [ ] Figure out a way to test lease extension
- [ ] Further features: namespace locking, checkpoint (b-tree)
- [ ] Further functionalities: garbage collection, re-replication, rebalancing, stale detection, master replication, checksumming
