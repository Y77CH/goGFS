# TODOs

## Fix / Refinements
- [ ] Edge condition: The client may read a byte range that crosses two chunks. In this case, the client should read two times.

## Some Day Improvements
- [ ] Because (at least at this moment), I'm running servers and clients on the same machine, 
there is no "closest" machine in reading, 
so the client will simply read the first replica in the array returned by master.