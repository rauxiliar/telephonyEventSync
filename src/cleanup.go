package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConsumerCleanup struct {
	client   *redis.Client
	group    string
	consumer string
	streams  []string
	threads  int
}

func NewConsumerCleanup(client *redis.Client, group, consumer string, streams []string, threads int) *ConsumerCleanup {
	return &ConsumerCleanup{
		client:   client,
		group:    group,
		consumer: consumer,
		streams:  streams,
		threads:  threads,
	}
}

func (cc *ConsumerCleanup) Start() {
	// Register cleanup function to be called in case of panic
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ERROR] Recovered from panic: %v", r)
			cc.Stop()
			panic(r) // Re-throw panic after cleanup
		}
	}()
}

func (cc *ConsumerCleanup) processPendingMessages(ctx context.Context, stream string) {
	for {
		// Get batch of pending messages
		pendingMsgs, err := cc.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  cc.group,
			Start:  "-",
			End:    "+",
			Count:  100,
		}).Result()

		if err != nil {
			log.Printf("[ERROR] Error getting pending messages: %v", err)
			return
		}

		if len(pendingMsgs) == 0 {
			return // No more pending messages
		}

		log.Printf("[INFO] Processing batch of %d pending messages in stream %s", len(pendingMsgs), stream)

		// Claim and ack all messages in this batch
		for _, msg := range pendingMsgs {
			// Claim the message
			claimedMsgs, err := cc.client.XClaim(ctx, &redis.XClaimArgs{
				Stream:   stream,
				Group:    cc.group,
				Consumer: cc.consumer,
				MinIdle:  0,
				Messages: []string{msg.ID},
			}).Result()

			if err != nil {
				log.Printf("[ERROR] Error claiming message %s: %v", msg.ID, err)
				continue
			}

			// Ack all claimed messages
			for _, claimedMsg := range claimedMsgs {
				err = cc.client.XAck(ctx, stream, cc.group, claimedMsg.ID).Err()
				if err != nil {
					log.Printf("[ERROR] Error acknowledging message %s: %v", claimedMsg.ID, err)
				} else {
					log.Printf("[INFO] Successfully acknowledged message %s", claimedMsg.ID)
				}
			}
		}
	}
}

func (cc *ConsumerCleanup) Stop() {
	log.Printf("[INFO] Stopping cleanup mechanism")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to remove all consumers from all streams
	for _, stream := range cc.streams {
		// Process all pending messages
		cc.processPendingMessages(ctx, stream)

		// Remove each consumer based on thread count
		for i := range cc.threads {
			consumerName := fmt.Sprintf("%s_%d", cc.consumer, i)
			err := cc.client.XGroupDelConsumer(ctx, stream, cc.group, consumerName).Err()
			if err != nil {
				log.Printf("[ERROR] Error removing consumer %s from group %s in stream %s: %v", consumerName, cc.group, stream, err)
				continue
			}
			log.Printf("[INFO] Successfully removed consumer %s from group %s in stream %s", consumerName, cc.group, stream)
		}
	}

	log.Printf("[INFO] Cleanup completed")
}
