package main

// the function that is used to simulate master
func getChunkHandleAndLocation(fileName string, chunkIndex int64) (chunkHandle, []string) {
	dummyList := *new([1]string)
	dummyList[0] = "127.0.0.1:12345"
	if fileName == "test" {
		if chunkIndex == 0 {
			return 0, dummyList[:]
		} else {
			return 1, dummyList[:]
		}
	}
	return 1, dummyList[:]
}
