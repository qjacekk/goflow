package output

import (
	"fmt"
	"goflow/flow"
	"log"
	"path/filepath"
	"time"
)

// BucketWriter is a wrapper over a Writer which splits the output into multiple files
// organized into partitions and bucket structure like:
// .
// ├───partitionA
// │   ├───subPartitionB
// │   │   ├───subPartitinC         <- partition "partitionA/subPartitionB/subPartitionC"
// │   │   │   ├───file-0001.txt    <- this is a bucket "partitionA/subPartitionB/subPartitionC/file-0001.txt"
// │   │   │   ├───file-0002.txt    <- this is a bucket "partitionA/subPartitionB/subPartitionC/file-0002.txt"
// │   │   │   ├───file-0003.txt    etc.
//
// Each bucket has corresponding file and file writer.
// There can be more than one active bucket (i.e. opened file with its own writer) at the same time.
// Write(record ...) method of BucketWriter directs the record to approriate bucket based on a
// BucketAssiger(record) function.
// BucketAssigner() is responsible for generating a path for a bucket (including all partitions) based
// on the record content or flow.Context)
//

type Bucket[R any] struct {
	writer flow.Writer[R]
	path string
	recCnt int
	expirationTime time.Time
}
func (b *Bucket[R]) close() {
	log.Println("Closing bucket", b.path)
	b.writer.Close()
}
func (b *Bucket[R]) Write(record R, ctx *flow.Context) {
	b.writer.Write(record, ctx)
	b.recCnt++
}


type BucketWriter[R any] struct {
	buckets map[string]*Bucket[R] // partition -> Bucket
	checkTicker *time.Ticker
	globalBucketNo int
	basePath string
	prefix string
	suffix string
	writerFactory func(path string) flow.Writer[R]
	recordsPerFile int
	bucketDuration time.Duration
	

	GetPartition func(record R, ctx *flow.Context) string
	RolloverPolicy func(bucket *Bucket[R]) bool
}
func (b *BucketWriter[R]) Writer() flow.Writer[R] {
	return b
}

func (bw *BucketWriter[R]) getNewBucket(partition string) *Bucket[R]{
	bucketFile := fmt.Sprintf("%s-%d%s", bw.prefix, bw.globalBucketNo, bw.suffix)
	bucketPath := filepath.Join(bw.basePath, partition, bucketFile)
	log.Println("Crating new bucket", bucketPath)
	return &Bucket[R]{writer: bw.writerFactory(bucketPath), path: bucketPath, expirationTime: time.Now().Add(bw.bucketDuration)}
}


func (b *BucketWriter[R]) MaxRecordsRolloverPolicy(bucket *Bucket[R]) bool {
	if b.recordsPerFile > 0 && bucket.recCnt >= b.recordsPerFile {
		log.Printf("Rollover - bucket %s has reached %d records\n", bucket.path, bucket.recCnt)
		return true
	}
	return false
}
func (b *BucketWriter[R]) BucketDurationRolloverPolicy(bucket *Bucket[R]) bool {
	if b.bucketDuration > 0 {
		return bucket.expirationTime.After(time.Now())
	}
	return false
}
func defaultRolloverPolicy[R any](bucket *Bucket[R]) bool {
	return false // never rollout
}


func (bw *BucketWriter[R]) Write(record R, ctx *flow.Context) {
	partition := bw.GetPartition(record, ctx)
	bucket, ok := bw.buckets[partition]
	if !ok {
		bucket = bw.getNewBucket(partition)
		bw.buckets[partition] = bucket
		bw.globalBucketNo++
	}
	bucket.Write(record, ctx)
}
func (bw *BucketWriter[O]) Close() {
	log.Println("Closing BucketWriter")
	bw.checkTicker.Stop()
	for _, bucket := range bw.buckets {
		bucket.close()
	}
}

func (bw *BucketWriter[O]) checkBuckets(t time.Time) {
	for bucketId, bucket := range bw.buckets {
		if bw.RolloverPolicy(bucket) { // TODO: add t to RolloverPolicy signature
			bucket.close()
			delete(bw.buckets, bucketId)
		}
	}
}

func getDefaultPartition[R any](record R, ctx *flow.Context) string {
	return ""
}

func NewBucketWriter[R any](basePath string, prefix string, suffix string, recordsPerFile int, bucketDuration time.Duration,
			writerFactory func(path string) flow.Writer[R], checkInterval time.Duration) *BucketWriter[R] {	
	bw := BucketWriter[R]{
		basePath: basePath,
		prefix: prefix,
		suffix: suffix,
		writerFactory: writerFactory,
		recordsPerFile: recordsPerFile,
		bucketDuration: bucketDuration,

		buckets: make(map[string]*Bucket[R]),
		checkTicker: time.NewTicker(checkInterval),
		GetPartition: getDefaultPartition[R],
		RolloverPolicy: defaultRolloverPolicy[R],
	}
	go func() {
		for t := range bw.checkTicker.C {
			bw.checkBuckets(t)
		}
	}()
	return &bw
}
