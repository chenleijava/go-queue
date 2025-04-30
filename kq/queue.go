package kq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/chenleijava/go-queue/kq/internal"
	"io"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/sasl/plain"
	_ "github.com/segmentio/kafka-go/snappy"
	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/logc"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/queue"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zeromicro/go-zero/core/timex"
	"go.opentelemetry.io/otel"
)

const (
	defaultCommitInterval = time.Second //提交给 kafka broker 间隔时间，默认是 1s
	defaultMaxWait        = time.Second //从 kafka 批量获取数据时，等待新数据到来的最大时间
	defaultQueueCapacity  = 1000        //kafka 内部队列长度

	//
	defaultBatchSize          = 1000
	defaultBatchFlushInterval = time.Second
)

type (
	ConsumeHandle func(ctx context.Context, key, value string) error

	ConsumeErrorHandler func(ctx context.Context, err error, msgs ...kafka.Message)

	ConsumeHandler interface {
		Consume(ctx context.Context, key, value string) error
	}

	kafkaReader interface {
		FetchMessage(ctx context.Context) (kafka.Message, error)
		CommitMessages(ctx context.Context, msgs ...kafka.Message) error
		Close() error
	}

	BatchHandle func(ctx context.Context, items []kafka.Message) error

	queueOptions struct {
		commitInterval time.Duration
		queueCapacity  int
		maxWait        time.Duration
		metrics        *stat.Metrics
		errorHandler   ConsumeErrorHandler

		batchHandle        BatchHandle
		batchFlushInterval time.Duration // flush interval
		batchSize          int           // batch size

	}

	QueueOption func(*queueOptions)

	kafkaQueue struct {
		c                KqConf
		consumer         kafkaReader
		handler          ConsumeHandler
		channel          chan kafka.Message
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup

		batchHandle BatchHandle
		batch       *internal.BatchProcessor[kafka.Message]

		commitRunner *threading.StableRunner[kafka.Message, kafka.Message]
		metrics      *stat.Metrics
		errorHandler ConsumeErrorHandler //force commit message . the error handler record the message
	}

	kafkaQueues struct {
		queues []queue.MessageQueue
		group  *service.ServiceGroup
	}
)

func MustNewQueue(c KqConf, handler ConsumeHandler, queueOpts ...QueueOption) queue.MessageQueue {
	q, err := NewQueue(c, handler, queueOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return q
}

func NewQueue(c KqConf, handler ConsumeHandler, queueOpts ...QueueOption) (queue.MessageQueue, error) {
	var options queueOptions
	for _, opt := range queueOpts {
		opt(&options)
	}
	ensureQueueOptions(c, &options)

	if c.Conns < 1 {
		c.Conns = 1
	}
	q := kafkaQueues{
		group: service.NewServiceGroup(),
	}
	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newKafkaQueue(c, handler, options))
	}

	return q, nil
}

func newKafkaQueue(c KqConf, handler ConsumeHandler, options queueOptions) queue.MessageQueue {
	var offset int64
	if c.Offset == firstOffset {
		offset = kafka.FirstOffset
	} else {
		offset = kafka.LastOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes, // 10KB
		MaxBytes:       c.MaxBytes, // 10MB
		MaxWait:        options.maxWait,
		CommitInterval: options.commitInterval,
		QueueCapacity:  options.queueCapacity, // default 1000
	}

	if len(c.Username) > 0 && len(c.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: c.Username,
				Password: c.Password,
			},
		}
	}

	if len(c.CaFile) > 0 {
		caCert, err := os.ReadFile(c.CaFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		ok := caCertPool.AppendCertsFromPEM(caCert)
		if !ok {
			log.Fatal(err)
		}

		readerConfig.Dialer.TLS = &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	}

	consumer := kafka.NewReader(readerConfig)

	q := &kafkaQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          make(chan kafka.Message),
		producerRoutines: threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          options.metrics,
		errorHandler:     options.errorHandler,
	}

	//batch process the kafka message
	if options.batchHandle != nil {
		q.batch = internal.NewBatchProcessor[kafka.Message](options.batchSize, options.batchFlushInterval, q.consumerBatchProcess)
		q.batch.Start()
	}

	if c.CommitInOrder {
		q.commitRunner = threading.NewStableRunner(func(msg kafka.Message) kafka.Message {
			if err := q.consumeOne(context.Background(), string(msg.Key), string(msg.Value)); err != nil {
				if q.errorHandler != nil {
					q.errorHandler(context.Background(), err, msg)
				}
			}

			return msg
		})
	}

	return q
}

func (q *kafkaQueue) Start() {
	if q.c.CommitInOrder {
		go q.commitInOrder()
		if err := q.consume(func(msg kafka.Message) {
			if e := q.commitRunner.Push(msg); e != nil {
				logx.Error(e)
			}
		}); err != nil {
			logx.Error(err)
		}
	} else if q.batch != nil {
		// pull message form kafka
		q.startProducers()
		q.producerRoutines.Wait()
		logx.Infof("Consumer %s is closed", q.c.Name)
	} else {
		q.startConsumers() // consumers handle message
		q.startProducers()
		q.producerRoutines.Wait()
		close(q.channel)
		q.consumerRoutines.Wait()
		logx.Infof("Consumer %s is closed", q.c.Name)
	}
}

func (q *kafkaQueue) Stop() {
	q.consumer.Close()
	if q.batch != nil {
		q.batch.Stop()
	}
	logx.Close()
}

// consumeOne
//
//	@Description: call logic and handle message
//	@receiver q
//	@param ctx
//	@param key
//	@param val
//	@return error
func (q *kafkaQueue) consumeOne(ctx context.Context, key, val string) error {
	startTime := timex.Now()
	err := q.handler.Consume(ctx, key, val)
	q.metrics.Add(stat.Task{
		Duration: timex.Since(startTime),
	})
	return err
}

// startConsumers
//
//	@Description: start consumers , processors of the partitions
//	@receiver q
func (q *kafkaQueue) startConsumers() {
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			for msg := range q.channel {
				// wrap message into message carrier
				mc := internal.NewMessageCarrier(internal.NewMessage(&msg))
				// extract trace context from message
				ctx := otel.GetTextMapPropagator().Extract(context.Background(), mc)
				// remove deadline and error control
				ctx = contextx.ValueOnlyFrom(ctx)

				//logic hand message
				if err := q.consumeOne(ctx, string(msg.Key), string(msg.Value)); err != nil {
					if q.errorHandler != nil {
						q.errorHandler(ctx, err, msg)
					}
				}

				//commit message offset
				if err := q.consumer.CommitMessages(ctx, msg); err != nil {
					if q.errorHandler != nil {
						q.errorHandler(ctx, errors.New(fmt.Sprintf("commit failed, error: %v", err)), msg)
					}
				}

			}
		})
	}
}

// consumerBatchProcess
//
//	@Description: batch process message . if err==nil. the items be commited
//	@receiver q
//	@param items
//	@return error
func (q *kafkaQueue) consumerBatchProcess(items []kafka.Message) error {
	//TODO span
	ctx := context.Background()
	err := q.batchHandle(ctx, items)
	if err != nil {
		if q.errorHandler != nil {
			q.errorHandler(ctx, err, items...)
		}
		return err
	} else {
		if err = q.consumer.CommitMessages(ctx, items...); err != nil {
			if q.errorHandler != nil {
				q.errorHandler(ctx, errors.New(fmt.Sprintf("commit failed, error: %v", err)), items...)
			}
		}
		return err
	}
}

// startProducers
//
//	@Description:
//	@receiver q
func (q *kafkaQueue) startProducers() {
	//partitions: consumers*1.5
	for i := 0; i < q.c.Consumers; i++ {
		i := i
		if q.batch != nil {
			q.producerRoutines.Run(func() {
				if err := q.consume(func(msg kafka.Message) {
					q.batch.Add(msg)
				}); err != nil {
					logx.Infof("Consumer %s-%d is closed, error: %q", q.c.Name, i, err.Error())
					return
				}
			})
		} else {
			//pull message from kafka and cache the channel
			q.producerRoutines.Run(func() {
				if err := q.consume(func(msg kafka.Message) {
					q.channel <- msg
				}); err != nil {
					logx.Infof("Consumer %s-%d is closed, error: %q", q.c.Name, i, err.Error())
					return
				}
			})
		}
	}
}

// consume
//
//	@Description:
//	@receiver q
//	@param handle
//	@return error
func (q *kafkaQueue) consume(handle func(msg kafka.Message)) error {
	for {
		msg, err := q.consumer.FetchMessage(context.Background())
		// io.EOF means consumer closed
		// io.ErrClosedPipe means committing messages on the consumer,
		// kafka will refire the messages on uncommitted messages, ignore
		if err == io.EOF || errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		if err != nil {
			logx.Errorf("Error on reading message, %q", err.Error())
			continue
		}

		handle(msg)
	}
}

func (q *kafkaQueue) commitInOrder() {
	for {
		msg, err := q.commitRunner.Get()
		if err != nil {
			logx.Error(err)
			return
		}

		if err := q.consumer.CommitMessages(context.Background(), msg); err != nil {
			logx.Errorf("commit failed, error: %v", err)
		}
	}
}

func (q kafkaQueues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q kafkaQueues) Stop() {
	q.group.Stop()
}

func WithCommitInterval(interval time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.commitInterval = interval
	}
}

func WithQueueCapacity(queueCapacity int) QueueOption {
	return func(options *queueOptions) {
		options.queueCapacity = queueCapacity
	}
}

func WithHandle(handle ConsumeHandle) ConsumeHandler {
	return innerConsumeHandler{
		handle: handle,
	}
}

func WithMaxWait(wait time.Duration) QueueOption {
	return func(options *queueOptions) {
		options.maxWait = wait
	}
}

func WithMetrics(metrics *stat.Metrics) QueueOption {
	return func(options *queueOptions) {
		options.metrics = metrics
	}
}

// WithErrorHandler
//
//	@Description: go-queue handle message  ,if err!=nil  ,will call this func
//	@param errorHandler
//	@return QueueOption
func WithErrorHandler(errorHandler ConsumeErrorHandler) QueueOption {
	return func(options *queueOptions) {
		options.errorHandler = errorHandler
	}
}

// WithBatchHandle
//
//	@Description: batch handle
//	@param batchHandle
//	@return QueueOption
func WithBatchHandle(batchHandle BatchHandle) QueueOption {
	return func(options *queueOptions) {
		options.batchHandle = batchHandle
	}
}

// WithBatchFlushInterval
//
//	@Description:  batch flush of the windows
//	@param batchFlushInterval default 1s
//	@return BatchOption
func WithBatchFlushInterval(flushInterval string) QueueOption {
	return func(options *queueOptions) {
		f, err := time.ParseDuration(flushInterval)
		logx.Must(err)
		options.batchFlushInterval = f
	}
}

// WithBatchSize
//
//	@Description:
//	@param batchSize default 1000
//	@return BatchOption
func WithBatchSize(batchSize int) QueueOption {
	return func(options *queueOptions) {
		options.batchSize = batchSize
	}
}

type innerConsumeHandler struct {
	handle ConsumeHandle
}

func (ch innerConsumeHandler) Consume(ctx context.Context, k, v string) error {
	return ch.handle(ctx, k, v)
}

func ensureQueueOptions(c KqConf, options *queueOptions) {

	if options.batchFlushInterval == 0 {
		options.batchFlushInterval = defaultBatchFlushInterval
	}

	if options.batchSize == 0 {
		options.batchSize = defaultBatchSize
	}

	if options.commitInterval == 0 {
		options.commitInterval = defaultCommitInterval
	}
	if options.queueCapacity == 0 {
		options.queueCapacity = defaultQueueCapacity
	}
	if options.maxWait == 0 {
		options.maxWait = defaultMaxWait
	}
	if options.metrics == nil {
		options.metrics = stat.NewMetrics(c.Name)
	}
	if options.errorHandler == nil {
		options.errorHandler = func(ctx context.Context, err error, msgs ...kafka.Message) {
			logc.Errorf(ctx, "consume: %+v, error: %v", msgs, err)
		}
	}
}
