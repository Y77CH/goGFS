# goGFS

### goGFS v0.1

Implement the read function in chunkserver and relevant part of the client.

1. Client provides file name, chunk index (file size divided by chunk size)
2. Dummy master replies with the chunk handle and dummy list of replicas.
3. Client sends chunk handle and byte range to chunkserver.
4. Chunkserver replies with the corresponding chunk data.

Edge conditions:
* The client may read a byte range that crosses two chunks. In this case, the client should read two times.