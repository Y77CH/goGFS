# goGFS

Overall design choices will be documented here;
specific technical implementations (eg. whether a variable should be `int64` or `int`) will be found in code comments.

## goGFS v0.2

Implement the record append and write function of chunkserver and relevant part of the client with a dummy master.

### Workflow of a Write (without Master)

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

### Notes

Because I wish to implement client and chunkserver first (which is why current version is `0.x`), 
the dummy master will always pick the chunkserver at `127.0.0.1:12345` as the primary.

## goGFS v0.1.2

Update the server such that it will handle concurrent read, instead of blocking new requests.

## goGFS v0.1.1

Change the project structure such that servers will be launched separatedly as new processes 
which better simulates multi-machine distributed system.

### Notes

This comes at the cost of completely separating the client and server side which leads to the issue that, 
for example, RPC call and reply data structures have to be redefined.
However, I do believe that this is acceptable. 

## goGFS v0.1

Implement the read function in chunkserver and relevant part of the client with a dummy master.

### Workflow of a Read

1. Client calls dummy master with file name, chunk index (file size divided by chunk size) 
and dummy master replies with the chunk handle and dummy list of replicas.
2. Client sends chunk handle and byte range (i.e. starting offset + length of read) to chunkserver.
3. Chunkserver replies with the corresponding chunk data.

### Design Considerations

I understand that for a real distributed system, the gfs servers should be started as services 
(i.e. in another process, and preferrably from other machines). 
However, for this initial version, I will be starting them only as go routines for simplicity.

Instead of using a custom bit map, or workarounds like `big.Int`, I decided to use `int64` for chunk handle, 
as there is no real scenario where I need to, for example, set an individual bit or do bit arithmetic.

### Notes

RPC parts referred to the Princeton COS418 [slide](https://www.cs.princeton.edu/courses/archive/spring21/cos418/docs/precept3_rpcs_in_go.pdf).

Quick Reference: 1 MB = 1024*1024 = 1048576 bytes