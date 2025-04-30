package internal

import (
	"go.uber.org/zap"
	"sync"
	"time"
)

// BatchProcessor 批处理模块
type BatchProcessor[T any] struct {
	queue         chan T                // 数据队列
	batchSize     int                   // 批量大小阈值
	flushInterval time.Duration         // 刷新时间间隔
	flushFunc     func(items []T) error // 外部传入的处理函数
	stop          chan struct{}         // 停止信号
	wg            sync.WaitGroup        // 等待组
	batch         []T                   // 当前批次数据
	mu            sync.Mutex            // 保护 batch 的并发访问
}

// NewBatchProcessor 创建新的批处理模块
func NewBatchProcessor[T any](batchSize int, flushInterval time.Duration, fc func(items []T) error) *BatchProcessor[T] {
	return &BatchProcessor[T]{
		queue:         make(chan T, batchSize*2), // 缓冲区稍大避免阻塞
		batchSize:     batchSize,
		flushInterval: flushInterval,
		flushFunc:     fc,
		stop:          make(chan struct{}),
		batch:         make([]T, 0, batchSize), // 预分配批次切片
	}
}

// Start 启动批处理模块
func (bp *BatchProcessor[T]) Start() {
	bp.wg.Add(1)
	go bp.process()
}

// Add 添加数据到队列
func (bp *BatchProcessor[T]) Add(item T) {
	select {
	case bp.queue <- item:
		// 成功写入队列
	default:
		// 队列满时，立即刷新当前批次数据
		bp.mu.Lock()
		if len(bp.batch) > 0 {
			bp.flush(bp.batch)
			bp.batch = bp.batch[:0] // 清空但保留容量
		}
		bp.mu.Unlock()
		// 再次尝试添加（队列可能已有空间）
		select {
		case bp.queue <- item:
		default:
			zap.S().Debugf("Queue still full after flush, dropping item: %v", item)
		}
	}
}

// Stop 停止批处理模块并处理剩余数据
func (bp *BatchProcessor[T]) Stop() {
	close(bp.stop)
	bp.wg.Wait()
}

// process 处理队列数据
func (bp *BatchProcessor[T]) process() {
	defer bp.wg.Done()

	ticker := time.NewTicker(bp.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case item := <-bp.queue:
			bp.mu.Lock()
			bp.batch = append(bp.batch, item)
			if len(bp.batch) >= bp.batchSize {
				bp.flush(bp.batch)
				bp.batch = bp.batch[:0]
			}
			bp.mu.Unlock()
		case <-ticker.C:
			bp.mu.Lock()
			if len(bp.batch) > 0 {
				bp.flush(bp.batch)
				bp.batch = bp.batch[:0]
			}
			bp.mu.Unlock()
		case <-bp.stop:
			bp.mu.Lock()
			if len(bp.batch) > 0 {
				bp.flush(bp.batch)
			}
			bp.mu.Unlock()
			return
		}
	}
}

// flush 调用外部函数处理批量数据
func (bp *BatchProcessor[T]) flush(batch []T) {
	if len(batch) == 0 {
		return
	}
	err := bp.flushFunc(batch)
	if err != nil {
		zap.S().Debugf("Flush failed: %v", err)
	} else {
		zap.S().Debugf("Flushed %d items", len(batch))
	}
}
