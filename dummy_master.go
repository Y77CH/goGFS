package main

// the function that is used to simulate master for returning chunkhandle and replicas
func GetChunkHandleAndLocation(fileName string, chunkIndex int64) (chunkHandle, []string) {
	dummyList := *new([1]string)
	dummyList[0] = "127.0.0.1:12345"
	if fileName == "test.txt" {
		if chunkIndex == 0 {
			return 0, dummyList[:]
		} else {
			return 1, dummyList[:]
		}
	}
	return 0, nil
}

// the function that is used to simulate master for returning chunkhandle, replicas (where primary is at index 0)
func GetPrimaryAndReplicas(fileName string) (chunkHandle, []string) {
	dummyList := []string{"127.0.0.1:12345", "127.0.0.1:12346"}
	if fileName == "test.txt" {
		return 1, dummyList
	}
	return 0, nil
}
