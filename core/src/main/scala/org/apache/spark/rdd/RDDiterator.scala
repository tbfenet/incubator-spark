package org.apache.spark.rdd

import scala.concurrent.{Await, Future}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.collection.mutable
import org.apache.spark.rdd.RDDiterator._

/**
 * Iterable whose iterator iterates over all elements of an RDD without fetching all partitions
 * to the driver process
 *
 * @param rdd RDD to iterate
 * @param prefetchPartitions    The number of partitions to prefetch.
 *                              If <1 will not prefetch.
 * @param timeOut How long to wait for each partition before failing.
 */
class RDDiterator[T: ClassTag](rdd: RDD[T], prefetchPartitions: Int, timeOut: Duration)
  extends Iterator[T] {
  var partitions = Range(0, rdd.partitions.size)
  var pendingFetches = mutable.Queue.empty[Future[Seq[T]]]
  pendingFetches.enqueue(partitions.take(prefetchPartitions).map(par => fetchData(par, rdd)): _*)
  partitions = partitions.drop(prefetchPartitions)
  var currentIterator: Iterator[T] = Iterator.empty
  @tailrec
  final def hasNext() = {
    if (currentIterator.hasNext) {
      //Still values in the current partition
      true
    } else {
      //Move on to the next partition

      //Queue new prefetch of a partition
      partitions.headOption.map {
        partitionNo =>
          pendingFetches.enqueue(fetchData(partitionNo, rdd))
      }
      partitions = partitions.drop(1)

      if (pendingFetches.isEmpty) {
        //No more partitions
        currentIterator = Iterator.empty
        false
      } else {
        val future = pendingFetches.dequeue
        currentIterator = Await.result(future, timeOut).iterator
        //Next partition might be empty so check again.
        this.hasNext()
      }
    }
  }
  def next() = {
    hasNext()
    currentIterator.next()
  }
}

object RDDiterator {
  private def fetchData[T: ClassTag](partitionIndex: Int, rdd: RDD[T]): Future[Seq[T]] = {
    val results = new ArrayBuffer[T]()
    rdd.context.submitJob[T, Array[T], Seq[T]](rdd,
      x => x.toArray,
      List(partitionIndex),
      (inx: Int, res: Array[T]) => results.appendAll(res),
      results.toSeq)
  }
}