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

package ants

import (
	"runtime"
	"time"
)

// goWorker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
// goWorker 是运行任务的实际执行者，它启动一个 goroutine 来接受任务并执行函数调用。
type goWorker struct {
	// pool who owns this worker.
	//work的所有者
	pool *Pool

	// task is a job should be done.
	//任务通道，通过这个发送给goWorker
	task chan func() // 需要执行的任务，注意：该chan 可能是缓存区或者非缓存区，如果是多核的话，缓存区的大小是1

	// recycleTime will be updated when putting a worker back into queue.
	recycleTime time.Time // 回收时间
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker) run() {
	//增加running的个数
	w.pool.addRunning(1)
	go func() {
		defer func() {
			//减少正在运行的个数
			w.pool.addRunning(-1)
			//使用sync.Pool对象池管理和创建worker对象
			w.pool.workerCache.Put(w)
			if p := recover(); p != nil {
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					w.pool.options.Logger.Printf("worker exits from a panic: %v\n", p)
					var buf [4096]byte
					n := runtime.Stack(buf[:], false)
					w.pool.options.Logger.Printf("worker exits from panic: %s\n", string(buf[:n]))
				}
			}
			// Call Signal() here in case there are goroutines waiting for available workers.
			//在这里发信号，让其他work开始干活
			w.pool.cond.Signal()
		}()
		//阻塞等待task
		for f := range w.task {
			if f == nil {
				return
			}
			f()
			// 将w存到pool.workers中下次可以再次获取
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}
