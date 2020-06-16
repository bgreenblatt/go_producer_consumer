package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// alias for int32 so that I can create an atomic inc wrapper
// function
type count32 int32

var i count32

// Atomic increment wrapper for count32 data. The new incremented value
// is returned
func (c *count32) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}

type Item struct {
	ID         int       `json:"Id"`
	Timestamp  time.Time `json:"Timestamp"`
	ProducerID int       `json:"ProducerId"`
}

// create a new item for inserting into the channel
func NewItem(id int, producerID int) *Item {
	i := Item{
		ID:         id,
		Timestamp:  time.Now(),
		ProducerID: producerID,
	}
	return &i
}

// produce items one at a time and insert into the passed in channel. The items
// are just timestamps and random numbers, along with a tag to indicate which
// producer created the item. This function is thread safe and can be called 
// as go produce(...) in a loop.  The defer command will decrement the internal
// wait group counter in wg when the produce function finally returns.
func produce(channel chan Item, wg *sync.WaitGroup, myId int) {
	defer wg.Done()
	for i := 0; i < 20; i++ {
		item := NewItem(rand.Intn(100), myId)
		channel <- *item
	}
}

// consume items one at a time that are pulled from the channel. The items
// are just printed out using the standard json marshalling routine. If we
// didn't want to use the json marshalling code, we'd have to print out the
// elements of the Item individually as in the commented out Printf.
// As with produce, this function is thread safe and can be called 
// as go consume(...) in a loop.  The defer command will decrement the internal
// wait group counter in wg when the consume function finally returns.
func consume(channel chan Item, wg *sync.WaitGroup, myId int) {
	defer wg.Done()
	for element := range channel {
		j := i.inc()
		b, err := json.Marshal(element)
		if err != nil {
			fmt.Printf("error formatting json: %v", err)
		} else {
			fmt.Printf("element %d is: %s, consumed by %d\n", j, string(b), myId)
		}
		// fmt.Printf("element %d consumed is %d, produced at %s, by producer %d\n",
		// j, element.ID, element.Timestamp.Format(time.RFC850),
		// element.ProducerID)
		time.Sleep(time.Second)
	}
}

// This program creates three producer threads using goroutines, and three
// consumer threads also using goroutines. They communicate using the standard
// go channel mechanism, so no external locking code is needed.
func main() {
	var producerwg sync.WaitGroup
	var consumerwg sync.WaitGroup
	// this channel can hold 10 items before the producers have to wait
	// this will happen because the producers create items faster than
	// the consumers can pull them out, because of the sleep in the
	// consumer loop
	channel := make(chan Item, 10)

	i = 0
	for i := 0; i < 3; i++ {
		producerwg.Add(1)
		go produce(channel, &producerwg, i)
	}
	for i := 0; i < 6; i++ {
		consumerwg.Add(1)
		go consume(channel, &consumerwg, i)
	}
	producerwg.Wait()
	close(channel)
	consumerwg.Wait()
}
