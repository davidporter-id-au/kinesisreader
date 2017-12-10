package kinesisreader

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// WARNING! This is an integration test and requires valid
// credentials and access to a valid Kinesis stream
func TestHappyPathSmoketest(t *testing.T) {

	es := make(chan string, 10)
	errors := make(chan string, 10)

	go func() {
		err := ReadEventStream(os.Getenv("KINESIS_STREAM"), es, errors, 5*time.Second, aws.NewConfig().WithRegion("ap-southeast-2"))
		if err != nil {
			t.Errorf("Error started in trying to create event stream: %v", err)
		}
	}()

	for i := 0; i < 100; i++ {
		<-es
	}

	if len(errors) > 0 {
		t.Errorf("Error channel reported errors: %s", <-errors)
	}
}

func TestHappyPathStartingFromSequenceNumbers(t *testing.T) {

	streamName := os.Getenv("KINESIS_STREAM")
	es := make(chan string, 10)
	errors := make(chan string, 10)

	svc := kinesis.New(
		session.New(
			aws.NewConfig().WithMaxRetries(10).WithRegion("ap-southeast-2"),
		),
	)

	resp, _ := svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: &streamName,
		},
	)

	shardID := *resp.StreamDescription.Shards[0].ShardId

	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		StreamName:        aws.String(streamName),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	}

	it, _ := svc.GetShardIterator(params)

	records, _ := svc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: it.ShardIterator,
	})

	sequenceNumber := records.Records[0].SequenceNumber

	go func() {

		err := readShardToStream(
			streamName,
			shardID,
			es,
			errors,
			svc,
			5*time.Second,
			sequenceNumber,
			func(s string, sequenceID string) {
				if shardID != s {
					t.Error("The callback for the shard didn't match up")
				}
			})

		// should never get here, function above blocks infinitely
		if err != nil {
			t.Errorf("Error in calling readShardToStream: %v", err)
		}
	}()

	for i := 0; i < 10; i++ {
		<-es
	}

	if len(errors) > 0 {
		t.Errorf("Error channel reported errors: %s", <-errors)
	}
}
