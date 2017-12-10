// Package kinesisreader reads in a given kinesis stream and
// publishes the records from all shards into two channels, a record channel
// and an error channel for error handling.
//
// It polls, after reaching the end of the kinesis stream.
// The error handling semantics are such that given an error in reading
// in a stream, it will send off an error stream and then retry that shard.
//package kinesisreader
package kinesisreader

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

//A representation ef recordID: sequenceNumber
type statemap map[string]string

func getIteratorAfterSeqNo(
	streamName string,
	shardID string,
	sequenceNumber *string,
	svc *kinesis.Kinesis,
) (*kinesis.GetShardIteratorOutput, error) {

	params := &kinesis.GetShardIteratorInput{
		ShardId:                aws.String(shardID),
		StreamName:             aws.String(streamName),
		StartingSequenceNumber: sequenceNumber,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
	}

	it, err := svc.GetShardIterator(params)
	if err != nil {
		return nil, err
	}
	return it, nil
}

func getTrimIterator(
	streamName string,
	shardID string,
	svc *kinesis.Kinesis,
) (*kinesis.GetShardIteratorOutput, error) {

	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		StreamName:        aws.String(streamName),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	}

	it, err := svc.GetShardIterator(params)
	if err != nil {
		return nil, err
	}
	return it, nil
}

// readShardToStream looks at a single Kinesis shard and starts with the TRIM_HORIZON
// and reads in all the records to the channel.
// when it reaches the end of the stream it polls kinesis for new records.
func readShardToStream(
	streamName string,
	shardID string,
	eventStream chan<- string,
	errorStream chan<- string,
	svc *kinesis.Kinesis,
	pollingPeriod time.Duration,
	lastSuccessfulSequenceNumber *string,
	recordSuccessCb func(shardID string, seqNumber string),
) error {

	var shardIterator *string

	if lastSuccessfulSequenceNumber == nil {
		it, err := getTrimIterator(streamName, shardID, svc)
		if err != nil {
			msg := fmt.Sprintf("Unrecoverable error: Error getting records from iterator: %v", err)
			errorStream <- msg
			log.Println(msg)
			return errors.New(msg)
		}
		shardIterator = it.ShardIterator
	} else {

		it, err := getIteratorAfterSeqNo(streamName, shardID, lastSuccessfulSequenceNumber, svc)

		if err != nil {
			msg := fmt.Sprintf("Unrecoverable error: Error getting records from iterator: %v", err)
			errorStream <- msg
			log.Println(msg)
			return errors.New(msg)
		}
		shardIterator = it.ShardIterator
	}

	for {
		records, err := svc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		if err != nil {
			errorStream <- fmt.Sprintf("Error getting records from kinesis: %v", err)
			log.Printf("Error getting records from kinesis: %v", err)
			time.Sleep(2 * time.Second)
		}

		if len(records.Records) > 0 {

			for _, r := range records.Records {
				eventStream <- string(r.Data)
				recordSuccessCb(shardID, *r.SequenceNumber)
			}

			shardIterator = records.NextShardIterator
		} else {

			// Reached the end of the shard, wait for a bit and see if there's anything
			// new available. This prevents flooding with new Shard requests
			// when reaching the most recent.
			time.Sleep(pollingPeriod)

			shardIterator = records.NextShardIterator
		}
	}
}

// ReadEventStream reads a given kinesis stream across all it's shards and
// puts the kinesis events into the given channel. In the event of
// an error it will publish the error into a channel as well.
//
// The semantics are presently only to read from the trim horizon
// until the lastest events.
func ReadEventStream(
	streamName string,
	eventStream chan<- string,
	errorStream chan<- string,
	pollingPeriod time.Duration,
	awsConfig *aws.Config,
) error {

	svc := kinesis.New(
		session.New(
			awsConfig.WithMaxRetries(10),
		),
	)

	resp, err := svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(streamName),
		},
	)

	if err != nil {
		return err
	}

	// FIXME actually use this
	// to allow the reading function to save progress.
	state := make(statemap)

	var mutex = &sync.Mutex{}

	for _, shard := range resp.StreamDescription.Shards {
		go readShardToStream(
			streamName,
			*shard.ShardId,
			eventStream,
			errorStream,
			svc,
			pollingPeriod,
			nil,
			func(shardID string, sequenceNumber string) {
				mutex.Lock()
				state[shardID] = sequenceNumber
				mutex.Unlock()
			},
		)
	}
	return nil
}
