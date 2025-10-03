package broadcaster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroadcaster(t *testing.T) {
	fmt.Println("=== Testing Broadcaster (100% Non-Blocking with History) ===")

	// Create broadcaster with max queue size of 100
	broadcaster := NewBroadcaster(100)
	defer broadcaster.Close()

	var wg sync.WaitGroup

	// Start data feed producer
	fmt.Println("Starting data feed producer...")
	stopProducer := make(chan struct{})
	wg.Add(1)
	go dataFeedProducer(broadcaster, stopProducer, &wg)

	// Give producer time to generate some messages
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Broadcaster has %d messages before subscribers join\n\n", broadcaster.GetMessageCount())

	// Create multiple subscribers with different configurations
	fmt.Println("Creating subscribers...")

	// Fast subscriber, no history
	wg.Add(1)
	fastSub := broadcaster.Subscribe(50, false)
	go fastSubscriber("Fast-NoHistory", fastSub, 10*time.Millisecond, &wg)

	// Slow subscriber, no history (will drop messages)
	wg.Add(1)
	slowSub := broadcaster.Subscribe(20, false)
	go slowSubscriber("Slow-NoHistory", slowSub, 100*time.Millisecond, &wg)

	// Fast subscriber WITH history (should catch up quickly)
	wg.Add(1)
	fastHistorySub := broadcaster.Subscribe(50, true)
	go fastSubscriber("Fast-WithHistory", fastHistorySub, 10*time.Millisecond, &wg)

	// Slow subscriber WITH history (will stay at tail, dropping old messages)
	time.Sleep(100 * time.Millisecond)
	fmt.Println("\nCreating SLOW subscriber WITH historical data (should drop from tail)...")
	wg.Add(1)
	slowHistorySub := broadcaster.Subscribe(30, true)
	go slowSubscriber("Slow-WithHistory", slowHistorySub, 80*time.Millisecond, &wg)

	// Very slow subscriber with history and small buffer
	time.Sleep(100 * time.Millisecond)
	fmt.Println("Creating VERY SLOW subscriber WITH historical data (will drop a lot from tail)...")
	wg.Add(1)
	verySlowHistorySub := broadcaster.Subscribe(15, true)
	go slowSubscriber("VerySlow-WithHistory", verySlowHistorySub, 150*time.Millisecond, &wg)

	// Let everything run for a while
	time.Sleep(3 * time.Second)

	// Stop producer
	fmt.Println("\nStopping producer...")
	close(stopProducer)

	// Wait a bit for subscribers to catch up
	time.Sleep(1 * time.Second)

	// Close all subscribers
	fmt.Println("\nClosing all subscribers...")
	fastSub.Close()
	slowSub.Close()
	fastHistorySub.Close()
	slowHistorySub.Close()
	verySlowHistorySub.Close()

	// Wait for all goroutines to finish
	wg.Wait()

	// Show final stats
	fmt.Printf("\nFinal message count in broadcaster: %d\n", broadcaster.GetMessageCount())
	fmt.Printf("Final subscriber count: %d\n", broadcaster.GetSubscriberCount())
	fmt.Println("\n=== Test Complete ===")
}

// dataFeedProducer simulates an intense data feed
func dataFeedProducer(b *Broadcaster, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	counter := 0
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			fmt.Printf("Producer stopped. Total messages sent: %d\n", counter)
			return
		case <-ticker.C:
			counter++
			b.Publish(fmt.Sprintf("Msg-%d", counter))

			if counter%50 == 0 {
				fmt.Printf("Producer sent %d messages, broadcaster queue: %d\n", counter, b.GetMessageCount())
			}
		}
	}
}

// fastSubscriber processes messages quickly
func fastSubscriber(name string, sub *Subscriber, delay time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	received := 0
	var firstSeq, lastSeq uint64

	for msg := range sub.Channel() {
		received++
		if received == 1 {
			firstSeq = msg.Sequence
		}
		lastSeq = msg.Sequence

		time.Sleep(delay) // Simulate processing

		if received%100 == 0 {
			fmt.Printf("[%s] Received %d messages (seq: %d, data: %v)\n",
				name, received, msg.Sequence, msg.Data)
		}
	}

	fmt.Printf("[%s] DONE - Received: %d, FirstSeq: %d, LastSeq: %d\n",
		name, received, firstSeq, lastSeq)
}

// slowSubscriber processes messages slowly and detects dropped messages
func slowSubscriber(name string, sub *Subscriber, delay time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()

	received := 0
	dropped := 0
	var firstSeq, lastSeq, prevSeq uint64

	for msg := range sub.Channel() {
		received++

		if received == 1 {
			firstSeq = msg.Sequence
			prevSeq = msg.Sequence
		} else {
			// Detect dropped messages by checking sequence gaps
			if msg.Sequence > prevSeq+1 {
				dropped += int(msg.Sequence - prevSeq - 1)
			}
			prevSeq = msg.Sequence
		}
		lastSeq = msg.Sequence

		time.Sleep(delay) // Simulate slow processing

		if received%20 == 0 {
			fmt.Printf("[%s] Received %d, dropped %d (seq: %d, data: %v)\n",
				name, received, dropped, msg.Sequence, msg.Data)
		}
	}

	fmt.Printf("[%s] DONE - Received: %d, Dropped: %d, FirstSeq: %d, LastSeq: %d\n",
		name, received, dropped, firstSeq, lastSeq)
}
