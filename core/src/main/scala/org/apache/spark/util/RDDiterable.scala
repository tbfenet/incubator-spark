package org.apache.spark.util

import scala.collection.immutable.Queue
import scala.concurrent.{Await, Future}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.annotation.tailrec
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**Iterable whose iterator iterates over all elements of an RDD without fetching all partitions to the driver process
 *
 * @param rdd RDD to iterate
 * @param prefetchPartitions    The number of partitions to prefetch
 * @param timeOut How long to wait for each partition before failing.
 * @tparam T
 */
class RDDiterable[T: ClassTag](rdd: RDD[T], prefetchPartitions: Int, timeOut: Duration) extends Serializable with Iterable[T] {

  def iterator = new Iterator[T] {
    var partitions = Range(0, rdd.partitions.size)
    var pendingFetches = Queue.empty.enqueue(partitions.take(prefetchPartitions).map(par => fetchData(par)))
    partitions = partitions.drop(prefetchPartitions)
    var currentIterator: Iterator[T] = Iterator.empty
    @tailrec
    def hasNext() = {
      if (currentIterator.hasNext) {
        true
      } else {
        pendingFetches = partitions.headOption.map {
          partitionNo =>
            pendingFetches.enqueue(fetchData(partitionNo))
        }.getOrElse(pendingFetches)
        partitions = partitions.drop(1)

        if (pendingFetches.isEmpty) {
          currentIterator = Iterator.empty
          false
        } else {
          val (future, pendingFetchesN) = pendingFetches.dequeue
          pendingFetches = pendingFetchesN
          currentIterator = Await.result(future, timeOut).iterator
          this.hasNext()
        }
      }
    }
    def next() = {
      hasNext()
      currentIterator.next()
    }
  }
  private def fetchData(partitionIndex: Int): Future[Seq[T]] = {
    val results = new ArrayBuffer[T]()
    rdd.context.submitJob[T, Array[T], Seq[T]](rdd,
      x => x.toArray,
      List(partitionIndex),
      (inx: Int, res: Array[T]) => results.appendAll(res),
      results.toSeq)
  }
}