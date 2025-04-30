package kq

import (
	"context"
	"github.com/chenleijava/go-queue/kq/internal"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
)

type (
	PushOption func(options *pushOptions)

	Pusher struct {
		topic    string
		producer kafkaWriter
		executor *executors.ChunkExecutor
	}

	kafkaWriter interface {
		Close() error
		WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	}

	pushOptions struct {
		// kafka.Writer options
		allowAutoTopicCreation bool
		balancer               kafka.Balancer

		// executors.ChunkExecutor options
		chunkSize     int
		flushInterval time.Duration

		// syncPush is used to enable sync push
		syncPush bool

		// Limit on how many attempts will be made to deliver a message.
		//
		// The default is to try at most 10 times.
		MaxAttempts int
		// WriteBackoffMin optionally sets the smallest amount of time the writer waits before
		// it attempts to write a batch of messages
		//
		// Default: 100ms
		WriteBackoffMin time.Duration
		// WriteBackoffMax optionally sets the maximum amount of time the writer waits before
		// it attempts to write a batch of messages
		//
		// Default: 1s
		WriteBackoffMax time.Duration

		// Number of acknowledges from partition replicas required before receiving
		// a response to a produce request, the following values are supported:
		//
		//  RequireNone (0)  fire-and-forget, do not wait for acknowledgements from the
		//  RequireOne  (1)  wait for the leader to acknowledge the writes
		//  RequireAll  (-1) wait for the full ISR to acknowledge the writes
		//
		// Defaults to RequireNone.
		RequiredAcks kafka.RequiredAcks
		Completion   func(messages []kafka.Message, err error)
	}
)

// NewPusher returns a Pusher with the given Kafka addresses and topic.
func NewPusher(addrs []string, topic string, opts ...PushOption) *Pusher {
	producer := &kafka.Writer{
		Addr:        kafka.TCP(addrs...),
		Topic:       topic,
		Balancer:    &kafka.LeastBytes{},
		Compression: kafka.Snappy,
	}

	var options pushOptions
	for _, opt := range opts {
		opt(&options)
	}

	// apply kafka.Writer options
	producer.AllowAutoTopicCreation = options.allowAutoTopicCreation

	if options.balancer != nil {
		producer.Balancer = options.balancer
	}

	if options.MaxAttempts != 0 {
		producer.MaxAttempts = options.MaxAttempts
	}

	if options.WriteBackoffMin != 0 {
		producer.WriteBackoffMin = options.WriteBackoffMin
	}

	if options.WriteBackoffMax != 0 {
		producer.WriteBackoffMax = options.WriteBackoffMax
	}

	if options.RequiredAcks != 0 {
		producer.RequiredAcks = options.RequiredAcks
	}

	if options.Completion != nil {
		producer.Completion = options.Completion
	}

	pusher := &Pusher{
		producer: producer,
		topic:    topic,
	}

	// if syncPush is true, return the pusher directly
	if options.syncPush {
		producer.BatchSize = 1
		return pusher
	}

	// apply ChunkExecutor options
	var chunkOpts []executors.ChunkOption
	if options.chunkSize > 0 {
		chunkOpts = append(chunkOpts, executors.WithChunkBytes(options.chunkSize))
	}
	if options.flushInterval > 0 {
		chunkOpts = append(chunkOpts, executors.WithFlushInterval(options.flushInterval))
	}

	pusher.executor = executors.NewChunkExecutor(func(tasks []interface{}) {
		chunk := make([]kafka.Message, len(tasks))
		for i := range tasks {
			chunk[i] = tasks[i].(kafka.Message)
		}
		if err := pusher.producer.WriteMessages(context.Background(), chunk...); err != nil {
			logx.Error(err)
		}
	}, chunkOpts...)

	return pusher
}

// Close closes the Pusher and releases any resources used by it.
func (p *Pusher) Close() error {
	if p.executor != nil {
		p.executor.Flush()
	}

	return p.producer.Close()
}

// Name returns the name of the Kafka topic that the Pusher is sending messages to.
func (p *Pusher) Name() string {
	return p.topic
}

// KPush sends a message to the Kafka topic.
func (p *Pusher) KPush(ctx context.Context, k, v string) error {
	msg := kafka.Message{
		Key:   []byte(k), // current timestamp
		Value: []byte(v),
	}
	if p.executor != nil {
		return p.executor.Add(msg, len(v))
	} else {
		return p.producer.WriteMessages(ctx, msg)
	}
}

// Push sends a message to the Kafka topic.
func (p *Pusher) Push(ctx context.Context, v string) error {
	return p.PushWithKey(ctx, strconv.FormatInt(time.Now().UnixNano(), 10), v)
}

// PushWithKey sends a message with the given key to the Kafka topic.
func (p *Pusher) PushWithKey(ctx context.Context, key, v string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(v),
	}

	// wrap message into message carrier
	mc := internal.NewMessageCarrier(internal.NewMessage(&msg))
	// inject trace context into message
	otel.GetTextMapPropagator().Inject(ctx, mc)

	if p.executor != nil {
		return p.executor.Add(msg, len(v))
	} else {
		return p.producer.WriteMessages(ctx, msg)
	}
}

// WithAllowAutoTopicCreation allows the Pusher to create the given topic if it does not exist.
func WithAllowAutoTopicCreation() PushOption {
	return func(options *pushOptions) {
		options.allowAutoTopicCreation = true
	}
}

// WithBalancer customizes the Pusher with the given balancer.
func WithBalancer(balancer kafka.Balancer) PushOption {
	return func(options *pushOptions) {
		options.balancer = balancer
	}
}

// WithChunkSize customizes the Pusher with the given chunk size.
func WithChunkSize(chunkSize int) PushOption {
	return func(options *pushOptions) {
		options.chunkSize = chunkSize
	}
}

// WithFlushInterval customizes the Pusher with the given flush interval.
func WithFlushInterval(interval time.Duration) PushOption {
	return func(options *pushOptions) {
		options.flushInterval = interval
	}
}

// WithSyncPush enables the Pusher to push messages synchronously.
func WithSyncPush() PushOption {
	return func(options *pushOptions) {
		options.syncPush = true
	}
}

// WithMaxAttempts
//
//	@Description:
//	@param MaxAttempts
//	@return PushOption
func WithMaxAttempts(maxAttempts int) PushOption {
	return func(options *pushOptions) {
		options.MaxAttempts = maxAttempts
	}
}

// WithWriteBackoffMin
//
//	@Description:
//
// WriteBackoffMin optionally sets the smallest amount of time the writer waits before
// it attempts to write a batch of messages
//
// Default: 100ms
//
//	@param writeBackoffMin
//	@return PushOption
func WithWriteBackoffMin(writeBackoffMin time.Duration) PushOption {
	return func(options *pushOptions) {
		options.WriteBackoffMin = writeBackoffMin
	}
}

// WithWriteBackoffMax
//
//	 @Description:
//	 WriteBackoffMax optionally sets the maximum amount of time the writer waits before
//		it attempts to write a batch of messages
//		Default: 1s
//	 @param writeBackoffMax
//	 @return PushOption
func WithWriteBackoffMax(writeBackoffMax time.Duration) PushOption {
	return func(options *pushOptions) {
		options.WriteBackoffMax = writeBackoffMax
	}
}

// WithRequiredAcks
//
//	@Description:
//	@param acks
//
// Number of acknowledges from partition replicas required before receiving
// a response to a produce request, the following values are supported:
//
//	 RequireNone (0)  fire-and-forget, do not wait for acknowledgements from the
//	 RequireOne  (1)  wait for the leader to acknowledge the writes
//	 RequireAll  (-1) wait for the full ISR to acknowledge the writes
//
//		@return PushOption
func WithRequiredAcks(acks kafka.RequiredAcks) PushOption {
	return func(options *pushOptions) {
		options.RequiredAcks = acks
	}
}

// WithCompletion
//
//	@Description:
//	@param completion
//	@return PushOption
func WithCompletion(completion func(messages []kafka.Message, err error)) PushOption {
	return func(options *pushOptions) {
		options.Completion = completion
	}
}
