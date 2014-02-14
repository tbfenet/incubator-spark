package org.apache.spark.rdd

import scala.concurrent.{Await, Future}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.collection.mutable
import org.apache.spark.rdd.RDDiterator._
import org.apache.spark.FutureAction

/**
 * Iterable whose iterator iterates over all elements of an RDD without fetching all partitions
 * to the driver process
 *
 * @param rdd RDD to iterate
 * @param prefetchPartitions    The number of partitions to prefetch.
 *                              If <1 will not prefetch.
 *                              partitions prefetched = min(prefetchPartitions, partitionBatchSize)
 * @param partitionBatchSize    How many partitions to fetch per job
 * @param timeOut How long to wait for each partition before failing.
 */
class RDDiterator[T: ClassTag](rdd: RDD[T], prefetchPartitions: Int, partitionBatchSize: Int,
                               timeOut: Duration)
  extends Iterator[T] {

  val batchSize = math.max(1,partitionBatchSize)
  var partitionsBatches: Iterator[Seq[Int]] = Range(0, rdd.partitions.size).grouped(batchSize)
  var pendingFetchesQueue = mutable.Queue.empty[Future[Seq[Seq[T]]]]
  //add prefetchPartitions prefetch
  0.until(math.max(0, prefetchPartitions / batchSize)).foreach(x=>enqueueDataFetch())

  var currentIterator: Iterator[T] = Iterator.empty
  @tailrec
  final def hasNext = {
    if (currentIterator.hasNext) {
      //Still values in the current partition
      true
    } else {
      //Move on to the next partition
      //Queue new prefetch of a partition
       enqueueDataFetch()
      if (pendingFetchesQueue.isEmpty) {
        //No more partitions
        currentIterator = Iterator.empty
        false
      } else {
        val future = pendingFetchesQueue.dequeue()
        currentIterator = Await.result(future, timeOut).flatMap(x => x).iterator
        //Next partition might be empty so check again.
        this.hasNext
      }
    }
  }
  def next() = {
    hasNext
    currentIterator.next()
  }

  def enqueueDataFetch() ={
    if (partitionsBatches.hasNext) {
      pendingFetchesQueue.enqueue(fetchData(partitionsBatches.next(), rdd))
    }
  }
}

object RDDiterator {
  private def fetchData[T: ClassTag](partitionIndexes: Seq[Int],
                                     rdd: RDD[T]): FutureAction[Seq[Seq[T]]] = {
    val results = new ArrayBuffer[Seq[T]]()
    rdd.context.submitJob[T, Array[T], Seq[Seq[T]]](rdd,
      x => x.toArray,
      partitionIndexes,
      (inx: Int, res: Array[T]) => results.append(res),
      results.toSeq)
  }
}