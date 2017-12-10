## Kinesis ReadEventStream 

A helper function for reading in a kinesis stream into a channel. Does some
basic attempts at resilliency. This is pre-alpha, not ready for production.

```go

package main

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
  	"github.com/davidporter-id-au/kinesisreader"
)

func main() {

  	// make an event stream channel
	es := make(chan string, 10)

  	// make an error channel for handling errors
	errors := make(chan string, 10)

	go kinesisreader.ReadEventStream(os.Getenv("KINESIS_STREAM"), es, errors, 6*time.Second, aws.NewConfig().WithRegion("ap-southeast-2"))

	go func() {
		for {
			println(<-errors)
		}
	}()

	for {
		println(<-es)
	}
}
```
