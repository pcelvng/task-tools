package main

import (
	"fmt"
	"os"
	"syscall"
)

func ExampleRun() {
	err := run()

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleRunSig() {
	sigChan <- syscall.SIGINT
	err := run()

	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleRunNopProducer() {
	// disable file-topic
	os.Args = append(os.Args, "-file-topic='-'")
	err := run()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// <nil>
}

func ExampleLifetimeMaxWorkers() {
	// launcher returns err on new consumer
	os.Args = append(os.Args, "-lifetime-max-workers=1")

	err := run()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// <nil>
}

func ExampleDoneTopic() {
	// launcher returns err on new consumer
	os.Args = append(os.Args, "-done-topic=test-done")

	err := run()

	fmt.Println(err) // output: <nil>

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// <nil>
}

func ExampleRunNewLauncherErr() {
	// launcher returns err on new consumer
	os.Args = append(os.Args, "-in-bus=not-supported")

	err := run()

	fmt.Println(err) // output: new consumer: 'task bus 'not-supported' not supported'

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// new consumer: 'task bus 'not-supported' not supported'
}

func ExampleRunNewProducerErr() {
	// producer returns err on new producer
	os.Args = append(os.Args, "-out-bus=not-supported")

	err := run()

	fmt.Println(err) // output: new producer: 'task bus 'not-supported' not supported'

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// new producer: 'task bus 'not-supported' not supported'
}

func ExampleLoadAppOptionsTopicChannel() {
	// bad toml config path
	os.Args = append(os.Args, "-topic=test-topic")
	os.Args = append(os.Args, "-channel=test-channel")

	err := loadAppOptions()

	fmt.Println(err)                // output: <nil>
	fmt.Println(appOpt.Bus.Topic)   // output: test-topic
	fmt.Println(appOpt.Bus.Channel) // output: test-channel

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// <nil>
	// test-topic
	// test-channel
}

func ExampleRunLoadOptionsErr() {
	// bad toml config path
	os.Args = append(os.Args, "-config=none.toml")

	err := run()

	fmt.Println(err) // output: open none.toml: no such file or directory

	// cleanup
	os.Args = os.Args[:len(os.Args)-1]

	// Output:
	// open none.toml: no such file or directory
}
