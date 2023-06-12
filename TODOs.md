# TODOs

## Function Fixes & Implementations
- [ ] Edge condition: The client may read a byte range that crosses two chunks. In this case, the client should read two times.
- [ ] Implement primary cache
- [ ] Implement retry (eg. in record append)
- [ ] **Implement automated E2E tests and unit tests.**

## Refinements
- [ ] Implement "read from closest" and pipelinging during data push (section 3.2).
- [ ] Structure error handling (eg. how & where to print logs, what format / message to be used)
- [ ] Structure logging such that each function prints relevant information like func name, server address