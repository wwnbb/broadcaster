package broadcaster

import (
	"sync"
	"sync/atomic"
	"time"
)

type Message struct {
	Data      interface{}
	Timestamp time.Time
	Sequence  uint64
}

type ringBuffer struct {
	buffer []atomic.Value
	head   uint64
	tail   uint64
	size   uint64
	mask   uint64
}

func newRingBuffer(size int) *ringBuffer {
	actualSize := nextPowerOf2(uint64(size))
	buffer := make([]atomic.Value, actualSize)
	return &ringBuffer{
		buffer: buffer,
		size:   actualSize,
		mask:   actualSize - 1,
	}
}

func nextPowerOf2(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func (rb *ringBuffer) push(msg Message) {
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	if tail-head >= rb.size {
		atomic.CompareAndSwapUint64(&rb.head, head, head+1)
	}

	rb.buffer[tail&rb.mask].Store(msg)

	atomic.StoreUint64(&rb.tail, tail+1)
}

func (rb *ringBuffer) readAt(pos uint64) (Message, bool) {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	if pos < head || pos >= tail {
		return Message{}, false
	}
	val := rb.buffer[pos&rb.mask].Load()
	if val == nil {
		return Message{}, false
	}
	msg, ok := val.(Message)
	return msg, ok
}

func (rb *ringBuffer) len() int {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return int(tail - head)
}

func (rb *ringBuffer) getHead() uint64 {
	return atomic.LoadUint64(&rb.head)
}

func (rb *ringBuffer) getTail() uint64 {
	return atomic.LoadUint64(&rb.tail)
}

type Subscriber struct {
	ch          chan Message
	bufferSize  int
	readPos     uint64
	closed      uint32
	notify      chan struct{}
	wg          sync.WaitGroup
	broadcaster *Broadcaster
	withHistory bool
	mu          sync.Mutex
}

func (s *Subscriber) Channel() <-chan Message {
	return s.ch
}

func (s *Subscriber) Close() {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		close(s.notify)
		s.wg.Wait()
		close(s.ch)
	}
}

func (s *Subscriber) IsClosed() bool {
	return atomic.LoadUint32(&s.closed) == 1
}

func (s *Subscriber) notifyNewMessage() {
	if s.IsClosed() {
		return
	}

	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Subscriber) run() {
	defer s.wg.Done()

	pendingMessages := make([]Message, 0, s.bufferSize)

	for {
		broadcasterTail := s.broadcaster.ring.getTail()
		broadcasterHead := s.broadcaster.ring.getHead()

		s.mu.Lock()
		currentReadPos := s.readPos
		s.mu.Unlock()

		if currentReadPos < broadcasterHead {
			s.mu.Lock()
			s.readPos = broadcasterHead
			currentReadPos = broadcasterHead
			s.mu.Unlock()
			pendingMessages = pendingMessages[:0]
		}

		for currentReadPos < broadcasterTail && len(pendingMessages) < s.bufferSize {
			if msg, ok := s.broadcaster.ring.readAt(currentReadPos); ok {
				pendingMessages = append(pendingMessages, msg)
				currentReadPos++
			} else {
				currentReadPos = s.broadcaster.ring.getHead()
				pendingMessages = pendingMessages[:0]
				break
			}
		}

		s.mu.Lock()
		s.readPos = currentReadPos
		s.mu.Unlock()

		if len(pendingMessages) > s.bufferSize {
			dropCount := len(pendingMessages) - s.bufferSize
			pendingMessages = pendingMessages[dropCount:]
		}

		for len(pendingMessages) > 0 {
			select {
			case s.ch <- pendingMessages[0]:
				pendingMessages = pendingMessages[1:]
			case <-s.notify:
				if s.IsClosed() {
					return
				}
				goto continueOuter
			}
		}

		select {
		case <-s.notify:
			if s.IsClosed() {
				return
			}
		}

	continueOuter:
	}
}

type Broadcaster struct {
	mu          sync.RWMutex
	ring        *ringBuffer
	subscribers []*Subscriber
	closed      uint32
	sequence    uint64
}

func NewBroadcaster(maxQueueSize int) *Broadcaster {
	return &Broadcaster{
		ring:        newRingBuffer(maxQueueSize),
		subscribers: make([]*Subscriber, 0),
	}
}

func (b *Broadcaster) Publish(data interface{}) {
	if atomic.LoadUint32(&b.closed) == 1 {
		return
	}

	seq := atomic.AddUint64(&b.sequence, 1)
	msg := Message{
		Data:      data,
		Timestamp: time.Now(),
		Sequence:  seq,
	}

	b.ring.push(msg)
	b.mu.RLock()
	for _, sub := range b.subscribers {
		if !sub.IsClosed() {
			sub.notifyNewMessage()
		}
	}
	b.mu.RUnlock()
}

func (b *Broadcaster) Subscribe(bufferSize int, withHistory bool) *Subscriber {
	if atomic.LoadUint32(&b.closed) == 1 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	sub := &Subscriber{
		ch:          make(chan Message, 1),
		bufferSize:  bufferSize,
		notify:      make(chan struct{}, 1),
		broadcaster: b,
		withHistory: withHistory,
	}

	if withHistory {
		sub.readPos = b.ring.getHead()
	} else {
		sub.readPos = b.ring.getTail()
	}

	sub.wg.Add(1)
	go sub.run()

	b.subscribers = append(b.subscribers, sub)

	if withHistory {
		sub.notifyNewMessage()
	}

	return sub
}

func (b *Broadcaster) GetMessageCount() int {
	return b.ring.len()
}

func (b *Broadcaster) GetSubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for _, sub := range b.subscribers {
		if !sub.IsClosed() {
			count++
		}
	}
	return count
}

func (b *Broadcaster) Close() {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return
	}

	b.mu.Lock()
	for _, sub := range b.subscribers {
		sub.Close()
	}
	b.subscribers = nil
	b.mu.Unlock()
}
