package main

import (
	"fmt"
	"sync"
)

func testRead() {
	readContent, err := Read("/home/ychen/test3.txt", 0, 4)
	if err == nil {
		fmt.Println(string(readContent))
	} else {
		fmt.Println(err.Error())
	}
}

func testEasyAppend() {
	// texts are extracted from Effective Go
	var data = "Go is a new language. Although it borrows ideas from existing languages, it has unusual properties that make effective Go programs different in character from programs written in its relatives. A straightforward translation of a C++ or Java program into Go is unlikely to produce a satisfactory result—Java programs are written in Java, not Go. On the other hand, thinking about the problem from a Go perspective could produce a successful but quite different program.\n"
	_, err := RecordAppend("/home/ychen/test.txt", []byte(data))
	if err != nil {
		fmt.Println("testEasyAppend Failed: error")
		fmt.Println(err)
	}
}

func testEasyWrite() {
	var data = "TEST"
	err := Write("/home/ychen/test2.txt", 0, []byte(data))
	if err != nil {
		fmt.Println("testEasyWrite Failed: error")
		fmt.Println(err)
	}
}

func testConcAppend() {
	var data = []string{
		// texts are extracted from Effective Go
		"Go is a new language. Although it borrows ideas from existing languages, it has unusual properties that make effective Go programs different in character from programs written in its relatives. A straightforward translation of a C++ or Java program into Go is unlikely to produce a satisfactory result—Java programs are written in Java, not Go. On the other hand, thinking about the problem from a Go perspective could produce a successful but quite different program.\n",
		"In other words, to write Go well, it's important to understand its properties and idioms. It's also important to know the established conventions for programming in Go, such as naming, formatting, program construction, and so on, so that programs you write will be easy for other Go programmers to understand.\n",
		"This document gives tips for writing clear, idiomatic Go code. It augments the language specification, the Tour of Go, and How to Write Go Code, all of which you should read first.\n",
		"Note added January, 2022: This document was written for Go's release in 2009, and has not been updated significantly since. Although it is a good guide to understand how to use the language itself, thanks to the stability of the language, it says little about the libraries and nothing about significant changes to the Go ecosystem since it was written, such as the build system, testing, modules, and polymorphism.\n",
		"There are no plans to update it, as so much has happened and a large and growing set of documents, blogs, and books do a fine job of describing modern Go usage. Effective Go continues to be useful, but the reader should understand it is far from a complete guide. See issue 28782 for context.\n",
		"The Go package sources are intended to serve not only as the core library but also as examples of how to use the language.\n",
		"Moreover, many of the packages contain working, self-contained executable examples you can run directly from the golang.org web site, such as this one (if necessary, click on the word \"Example\" to open it up).\n",
		"If you have a question about how to approach a problem or how something might be implemented, the documentation, code and examples in the library can provide answers, ideas and background.\n"}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := RecordAppend("test.txt", []byte(data[i]))
			if err != nil {
				fmt.Println("ERROR: testConcAppend Failed: error")
			}
		}(i)
	}
	wg.Wait()
}

func testConcWrite() {
	var data = []string{"TEST1", "TEST2", "TEST3", "TEST4", "TEST5", "TEST6", "TEST7", "TEST8"}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := Write("test.txt", 67108864+int64(i*9), []byte(data[i]))
			if err != nil {
				fmt.Println("ERROR: testConcWrite Failed: error")
			}
		}(i)
	}
	wg.Wait()
}

func testCreate() {
	var filename = "/home/ychen/test.txt"
	err := Create(filename)
	if err != nil {
		fmt.Println(err)
	}
	filename = "/home/ychen/test2.txt"
	Create(filename)
	if err != nil {
		fmt.Println(err)
	}
}

func testDelete() {
	var filename = "/home/ychen/test3.txt"
	err := Delete(filename)
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	// testCreate()
	// testEasyAppend()
	// testEasyWrite()
	// testRead()
	testDelete()
}
