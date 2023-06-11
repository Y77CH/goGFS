# goGFS

## goGFS v0.1.1

Change the project structure such that servers will be launched separatedly as new processes 
which better simulates multi-machine distributed system.

Key takeaway: how to structure multi main functions in a Go project.

### Notes

This comes at the cost of completely separating the client and server side which leads to the issue that, 
for example, RPC call and reply data structures have to be redefined.
However, I do believe that this is acceptable. 

## goGFS v0.1

Implement the read function in chunkserver and relevant part of the client with a dummy master.

Key takeaway: how to build an application with go's built in RPC.

### Workflow of a Read

1. Client calls dummy master with file name, chunk index (file size divided by chunk size) 
and dummy master replies with the chunk handle and dummy list of replicas.
2. Client sends chunk handle and byte range (i.e. starting offset + length of read) to chunkserver.
3. Chunkserver replies with the corresponding chunk data.

### Design Considerations

* I understand that for a real distributed system, the gfs servers should be started as services 
(i.e. in another process, and preferrably from other machines). 
However, for this initial version, I will be starting them only as go routines for simplicity.
* Instead of using a custom bit map, or workarounds like `big.Int`, I decided to use `int64` for chunk handle, 
as there is no real scenario where I need to, for example, set an individual bit or do bit arithmetic.
* When a read fails, the client library will return error.

### Notes

* RPC parts referred to the Princeton COS418 slide[^1].
* Quick Reference: 1 MB = 1024*1024 = 1048576 bytes

[^1]: https://www.cs.princeton.edu/courses/archive/spring21/cos418/docs/precept3_rpcs_in_go.pdf