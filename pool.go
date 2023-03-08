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
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync"
)

// Pool accepts the tasks from client, it limits the total of goroutines to a given number by recycling goroutines.
type Pool struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	//协程池数量：该 Pool 的容量，也就是开启 worker 数量的上限
	capacity int32

	// running is the number of the currently running goroutines.
	//正在运行的goroutines的数量
	running int32

	// lock for protecting the worker queue.
	//ants中实现自旋锁使用二进制指数规避算法,如果锁已经被其他线程获取，那么该线程将循环等待，然后不断地判断是否能够被成功获取，知直到获取到锁才会退出循环。
	lock sync.Locker

	// workers is a slice that store the available workers.
	//存放池中所有的worker,workerArray包含可用workers队列和过期workers队列，只会从可用workers队列中取可用worker
	workers workerArray

	// state is used to notice the pool to closed itself.
	//记录池子的状态 （关闭、开启），0打开，1关闭
	state int32

	// cond for waiting to get an idle worker.
	//条件等待，得到一个空闲work的条件：条件等待是不同协程各用一个锁，主协程发送信号的时候随机一个解锁
	cond *sync.Cond

	// workerCache speeds up the obtainment of a usable worker in function:retrieveWorker.
	//workerCache加快了在函数retrieveWorker中获取可用worker的速度,这个sync池避免重复创建对象。
	workerCache sync.Pool

	// waiting is the number of goroutines already been blocked on pool.Submit(), protected by pool.lock
	//waiting是pool.Submit（）上已被阻止的goroutine的数量，受pool.lock保护
	waiting int32

	// 清道夫，定时清理workerarray 队列中过期的worker
	purgeDone int32
	stopPurge context.CancelFunc

	// 定时器是否已经结束状态，结束为1 ，否则正在运行为0，在定时器被关闭被设置为1，在reboot的时候设置为0，默认值也是0
	ticktockDone int32
	//这个为了关闭定时器， 当这个关闭的时候，定时器自然会被关闭，主要是为了控制定时器
	stopTicktock context.CancelFunc

	//会有个定时器一直循环去更新这个时间
	now atomic.Value
	//	// 需要自定义加载的配置
	options *Options
}

// purgeStaleWorkers clears stale workers periodically, it runs in an individual goroutine, as a scavenger.
// purgeStaleWorkers定期清除过时的工作程序，它作为清道夫在单独的goroutine中运行。
func (p *Pool) purgeStaleWorkers(ctx context.Context) {
	// 定时器
	ticker := time.NewTicker(p.options.ExpiryDuration)

	defer func() {
		//在return后结束定时器
		ticker.Stop()
		//在设置玩之后设置清理完成
		atomic.StoreInt32(&p.purgeDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		p.lock.Lock()
		// 删除workers中过期的数据,队列是后进先出
		expiredWorkers := p.workers.retrieveExpiry(p.options.ExpiryDuration)
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		for i := range expiredWorkers {
			expiredWorkers[i].task <- nil
			expiredWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up(no worker is running),
		// or another case where the pool capacity has been Tuned up,
		// while some invokers still get stuck in "p.cond.Wait()",
		// then it ought to wake all those invokers.
		//可能存在这样一种情况：所有work都已清理干净（没有work正在运行），
		//或者池容量已调高的另一种情况，
		//尽管一些调用程序仍然会陷入“p.comd.Wait（）”，
		//那么它应该唤醒所有的召唤者
		if p.Running() == 0 || (p.Waiting() > 0 && p.Free() > 0) {
			p.cond.Broadcast()
		}
	}
}

// ticktock is a goroutine that updates the current time in the pool regularly.
// /ticktock是一个goroutine，它定期更新池中的当前时间。
func (p *Pool) ticktock(ctx context.Context) {

	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.ticktockDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		p.now.Store(time.Now())
	}
}

func (p *Pool) goPurge() {
	if p.options.DisablePurge {
		return
	}

	// Start a goroutine to clean up expired workers periodically.

	//利用context，当在 Context 取，消的时候，会关闭这个只读的 Channel，相当于发出了取消信号，然后定时器被销毁了
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

func (p *Pool) goTicktock() {
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

func (p *Pool) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// NewPool generates an instance of ants pool.
func NewPool(size int, options ...Option) (*Pool, error) {
	opts := loadOptions(options...) //加载自定义的options中的配置

	if size <= 0 {
		size = -1
	}
	//禁用删除选项为false:就给设置过期时间，woker被定时清除
	if !opts.DisablePurge {
		if expiry := opts.ExpiryDuration; expiry < 0 {
			return nil, ErrInvalidPoolExpiry
		} else if expiry == 0 {
			opts.ExpiryDuration = DefaultCleanIntervalTime
		}
	}
	//logger为空的情况下，自己设置日志
	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}
	//这里说明每个协程池子都是被独立创建的
	p := &Pool{
		capacity: int32(size),
		lock:     syncx.NewSpinLock(), //设置自旋锁
		options:  opts,
	}
	//work相当于工人，首先创建worker，然后强制类型转换，第一次初始化的时候创建一个工人
	p.workerCache.New = func() interface{} { //sync.pool 初始化
		return &goWorker{
			pool: p,
			task: make(chan func(), workerChanCap),
		}
	}
	//如果实现要创建工人，那么就先放到循环队列，但是这时候工人还没有被使用，所以不会放到workercache中
	if p.options.PreAlloc {
		if size == -1 {
			return nil, ErrInvalidPreAllocSize
		}
		p.workers = newWorkerArray(loopQueueType, size) //循环队列
	} else {
		p.workers = newWorkerArray(stackType, 0)
	}
	//sync.cond初始化
	p.cond = sync.NewCond(p.lock)
	//清除超时的work
	p.goPurge()
	//更新协程池的时间
	p.goTicktock()

	return p, nil
}

// ---------------------------------------------------------------------------

// Submit submits a task to this pool.
//
// Note that you are allowed to call Pool.Submit() from the current Pool.Submit(),
// but what calls for special attention is that you will get blocked with the latest
// Pool.Submit() call once the current Pool runs out of its capacity, and to avoid this,
// you should instantiate a Pool with ants.WithNonblocking(true).
func (p *Pool) Submit(task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	var w *goWorker
	//如果有闲置的work，就会返回一个work ，如果没有 就会返回一个新的work（协程池没有满的时候），
	if w = p.retrieveWorker(); w == nil {
		return ErrPoolOverload
	}
	//把任务给这个work
	w.task <- task
	return nil
}

// Running returns the number of workers currently running.
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *Pool) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *Pool) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Cap returns the capacity of this pool.
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *Pool) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// IsClosed indicates whether the pool is closed.
// IsClosed指示池是否已关闭
func (p *Pool) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Release closes this pool and releases the worker queue.
// 释放将关闭此池并释放工作队列。
func (p *Pool) Release() {
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
// ReleaseTimeout类似于Release，但有了超时，它会在超时前等待所有worker退出。
func (p *Pool) ReleaseTimeout(timeout time.Duration) error {
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}

	if p.stopPurge != nil {
		p.stopPurge()
		p.stopPurge = nil
	}
	//这个调用完之后，&p.purgeDone已经是结束的时间了
	p.stopTicktock()
	p.stopTicktock = nil
	p.Release()

	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if p.Running() == 0 &&
			(p.options.DisablePurge || atomic.LoadInt32(&p.purgeDone) == 1) &&
			atomic.LoadInt32(&p.ticktockDone) == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// Reboot reboots a closed pool.
// 重新启动重新启动关闭的池。
func (p *Pool) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

// ---------------------------------------------------------------------------

func (p *Pool) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *Pool) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// retrieveWorker returns an available worker to run the tasks.
// retrieveWorker返回运行任务的可用work,(如果Nonblocking为false，并且获得不了worker之后就会一直堵塞状态
func (p *Pool) retrieveWorker() (w *goWorker) {
	//（1）声明了一个构造 goWorker 的函数 spawnWorker 用于兜底，内部实际上是从对象池 workerCache 中获取 goWorker；
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker)
		w.run()
	}
	//（2）接下来的核心逻辑就是加锁，然后尝试从池子中取出 goWorker 执行任务；
	p.lock.Lock()

	w = p.workers.detach()
	if w != nil { // first try to fetch the worker from the queue
		p.lock.Unlock()
		//（5）倘若池子容量未超限，且未取到 goWorker，调用 spawnWorker 构造新的 goWorker 用于执行任务.
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// if the worker queue is empty and we don't run out of the pool capacity,
		// then just spawn a new worker goroutine.
		//如果工作队列是空的并且我们没有耗尽池容量，然后就产生一个新的work goroutine。
		p.lock.Unlock()
		spawnWorker()
		//如果队列为空，且池子容量(capacity)也满了，那么判断一下p.options.Nonblocking是否为true，如果为true，说明不想阻塞，那么retrieveWorker返回nil。retrieveWorker返回nil，那么Submit返回ErrPoolOverload错误。
	} else { // otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.

		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
		//这个场景解决阻塞场景下，一直去找个work去运行，当收到自旋锁的信号，尝试去加载
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}
		p.addWaiting(1)
		p.cond.Wait() // block and wait for an available worker
		p.addWaiting(-1)

		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		var nw int
		if nw = p.Running(); nw == 0 { // awakened by the scavenger
			p.lock.Unlock()
			spawnWorker()
			return
		}
		if w = p.workers.detach(); w == nil {
			if nw < p.Cap() {
				p.lock.Unlock()
				spawnWorker()
				return
			}
			goto retry
		}
		p.lock.Unlock()
	}
	return
}

// revertWorker将一个工作程序放回空闲池，并且设置好清理时间，这个清理时间会在清理的时候比较时间的大小
// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool) revertWorker(worker *goWorker) bool {
	//判断是否可以继续
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}
	//设置清理时间
	worker.recycleTime = p.nowTime()
	p.lock.Lock()

	// To avoid memory leaks, add a double check in the lock scope.
	// Issue: https://github.com/panjf2000/ants/issues/113
	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}

	err := p.workers.insert(worker)
	if err != nil {
		p.lock.Unlock()
		return false
	}

	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	//唤醒通知其他等待的worker，让其他work可以继续运行
	p.cond.Signal()
	p.lock.Unlock()
	return true
}
