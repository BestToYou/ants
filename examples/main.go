// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

var sum int32

func myFunc(i interface{}) {
	n := i.(int32)
	atomic.AddInt32(&sum, n)
	time.Sleep(1000 * 15 * time.Millisecond)

	fmt.Printf("run with %d\n", n)
}

func demoFunc(i int) {

	resp, err := http.Get("http://www.baidu.com")
	if err != nil {
		// handle error
		log.Println(err)
		return
	}

	defer resp.Body.Close()

	headers := resp.Header

	for k, v := range headers {
		fmt.Printf("k=%v, v=%v\n", k, v)
	}

	fmt.Printf("resp status %s,statusCode %d\n", resp.Status, resp.StatusCode)

}

func main() {
	start := time.Now()
	defer ants.Release()

	runTimes := 10000

	// Use the common pool.
	var wg sync.WaitGroup
	//syncCalculateSum := func() {
	//	demoFunc()
	//	wg.Done()
	//}
	var (
		defaultAntsPool1, _ = ants.NewPool(100)
	)

	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = defaultAntsPool1.Submit(func() {
			demoFunc(i)
			wg.Done()
		})
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", ants.Running())
	fmt.Printf("finish all tasks.\n")

	// Use the pool with a method,
	// set 10 to the capacity of goroutine pool and 1 second for expired duration.
	p, _ := ants.NewPoolWithFunc(math.MaxInt32, func(i interface{}) {
		myFunc(i)
		wg.Done()
	})
	defer p.Release()
	// Submit tasks one by one.
	for i := 0; i < runTimes; i++ {
		wg.Add(1)
		_ = p.Invoke(int32(i))
	}
	wg.Wait()
	fmt.Printf("running goroutines: %d\n", p.Running())
	fmt.Printf("finish all tasks, result is %d\n", sum)
	end := time.Since(start)

	fmt.Println(end)
}
