package cherryActor

import (
	"sync/atomic"
	"unsafe"
)

// 异步通知：当队列从空变为非空时，通过通道发送信号，通知等待的消费者有数据可处理
// 避免忙等待：消费者可以阻塞在 <-q.C 上，而不是不断轮询 Pop() 方法
// 事件驱动：实现生产者-消费者模式中的事件驱动架构
//
//	              queue
//	   /						   \
//	  /					 		    \
//	head                     		tail
//	 |								 |
//	 newNode <-   node  <- node <- node
type (
	queue struct {
		head, tail *queueNode
		C          chan int32
		count      int32
	}

	queueNode struct {
		next *queueNode
		val  interface{}
	}
)

func newQueue() queue {
	stub := &queueNode{}
	q := queue{
		head:  stub,
		tail:  stub,
		C:     make(chan int32, 1),
		count: 0,
	}
	return q
}

// 创建节点 → 原子交换head → 连接节点 → 通知消费者
// 关键点：通过 SwapPointer 确保同一时间只有一个生产者能更新head
// 使用 StorePointer 确保节点的链接对其他线程立即可见
func (p *queue) Push(v interface{}) {
	if v == nil { // 第1行：过滤空值，避免存储nil
		return
	}
	// 创建新节点并设置新节点的值
	n := new(queueNode)
	n.val = v

	// current producer acquires head node
	// 关键步骤 - 原子地交换head指针并获取前一个head, 这是无锁队列的核心：通过原子操作保证并发安全
	prev := (*queueNode)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&p.head)), unsafe.Pointer(n)))

	// release node to consumer
	// 将前一个head节点的next指针指向新节点, 这一步"发布"新节点，使其对消费者可见
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&prev.next)), unsafe.Pointer(n))
	// 增加计数并可能发送通知
	p._setCount(1)
}

func (p *queue) Pop() interface{} {
	// 获取当前tail指针（本地缓存，可能过时）
	tail := p.tail
	// 原子加载tail.next指针，使用LoadPointer保证读取的内存可见性
	next := (*queueNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next)))) // acquire
	// 检查是否有下一个节点
	if next != nil {
		// 移动tail指针到下一个节点
		p.tail = next
		// 获取节点值
		v := next.val
		// 清空节点值，帮助GC
		next.val = nil
		// 减少计数
		p._setCount(-1)
		// 返回值
		return v
	}

	return nil
}

func (p *queue) Empty() bool {
	tail := p.tail
	next := (*queueNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tail.next))))
	return next == nil
}

func (p *queue) Count() int32 {
	return atomic.LoadInt32(&p.count)
}

// 使用 select 的 default 避免阻塞，保证高性能
// 只在队列非空时发送通知
// 发送的是实际元素数量，不只是信号
func (p *queue) _setCount(delta int32) {
	// 原子地更新计数并获取新值
	count := atomic.AddInt32(&p.count, delta)
	// 只有队列非空时才发送通知
	if count > 0 {
		select {
		case p.C <- count: // 尝试发送当前元素数量
		default:
		}
	}
}

func (p *queue) Destroy() {
	close(p.C)
	p.head = nil
	p.tail = nil
	p.count = 0
}
