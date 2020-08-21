/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap, Semaphore, ThreadLocalRandom, TimeUnit}

import kafka.metrics.KafkaMetricsGroup
import kafka.server.FetchSession.CACHE_MAP
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchMetadata.{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, Time, Utils}

import scala.math.Ordered.orderingToOrdered
import scala.collection._

object FetchSession {
  type REQ_MAP = util.Map[TopicPartition, FetchRequest.PartitionData]
  type RESP_MAP = util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
  type CACHE_MAP = ImplicitLinkedHashCollection[CachedPartition]
  type RESP_MAP_ITER = util.Iterator[util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]]]

  val NUM_INCREMENTAL_FETCH_SESSISONS = "NumIncrementalFetchSessions"
  val NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED = "NumIncrementalFetchPartitionsCached"
  val INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC = "IncrementalFetchSessionEvictionsPerSec"
  val EVICTIONS = "evictions"

  def partitionsToLogString(partitions: util.Collection[TopicPartition], traceEnabled: Boolean): String = {
    if (traceEnabled) {
      "(" + Utils.join(partitions, ", ") + ")"
    } else {
      s"${partitions.size} partition(s)"
    }
  }
}

/**
  * A cached partition.
  *
  * The broker maintains a set of these objects for each incremental fetch session.
  * When an incremental fetch request is made, any partitions which are not explicitly
  * enumerated in the fetch request are loaded from the cache.  Similarly, when an
  * incremental fetch response is being prepared, any partitions that have not changed
  * are left out of the response.
  *
  * We store many of these objects, so it is important for them to be memory-efficient.
  * That is why we store topic and partition separately rather than storing a TopicPartition
  * object.  The TP object takes up more memory because it is a separate JVM object, and
  * because it stores the cached hash code in memory.
  *
  * Note that fetcherLogStartOffset is the LSO of the follower performing the fetch, whereas
  * localLogStartOffset is the log start offset of the partition on this broker.
  */
class CachedPartition(val topic: String,
                      val partition: Int,
                      var maxBytes: Int,
                      var fetchOffset: Long,
                      var highWatermark: Long,
                      var leaderEpoch: Optional[Integer],
                      var fetcherLogStartOffset: Long,
                      var localLogStartOffset: Long)
    extends ImplicitLinkedHashCollection.Element {

  var cachedNext: Int = ImplicitLinkedHashCollection.INVALID_INDEX
  var cachedPrev: Int = ImplicitLinkedHashCollection.INVALID_INDEX

  override def next: Int = cachedNext
  override def setNext(next: Int): Unit = this.cachedNext = next
  override def prev: Int = cachedPrev
  override def setPrev(prev: Int): Unit = this.cachedPrev = prev

  def this(topic: String, partition: Int) =
    this(topic, partition, -1, -1, -1, Optional.empty(), -1, -1)

  def this(part: TopicPartition) =
    this(part.topic, part.partition)

  def this(part: TopicPartition, reqData: FetchRequest.PartitionData) =
    this(part.topic, part.partition, reqData.maxBytes, reqData.fetchOffset, -1,
      reqData.currentLeaderEpoch, reqData.logStartOffset, -1)

  def this(part: TopicPartition, reqData: FetchRequest.PartitionData,
           respData: FetchResponse.PartitionData[Records]) =
    this(part.topic, part.partition, reqData.maxBytes, reqData.fetchOffset, respData.highWatermark,
      reqData.currentLeaderEpoch, reqData.logStartOffset, respData.logStartOffset)

  def reqData = new FetchRequest.PartitionData(fetchOffset, fetcherLogStartOffset, maxBytes, leaderEpoch)

  def updateRequestParams(reqData: FetchRequest.PartitionData): Unit = {
    // Update our cached request parameters.
    maxBytes = reqData.maxBytes
    fetchOffset = reqData.fetchOffset
    fetcherLogStartOffset = reqData.logStartOffset
    leaderEpoch = reqData.currentLeaderEpoch
  }

  /**
    * Determine whether or not the specified cached partition should be included in the FetchResponse we send back to
    * the fetcher and update it if requested.
    *
    * This function should be called while holding the appropriate session lock.
    *
    * @param respData partition data
    * @param updateResponseData if set to true, update this CachedPartition with new request and response data.
    * @return True if this partition should be included in the response; false if it can be omitted.
    */
  def maybeUpdateResponseData(respData: FetchResponse.PartitionData[Records], updateResponseData: Boolean): Boolean = {
    // Check the response data.
    var mustRespond = false
    if ((respData.records != null) && (respData.records.sizeInBytes > 0)) {
      // Partitions with new data are always included in the response.
      mustRespond = true
    }
    if (highWatermark != respData.highWatermark) {
      mustRespond = true
      if (updateResponseData)
        highWatermark = respData.highWatermark
    }
    if (localLogStartOffset != respData.logStartOffset) {
      mustRespond = true
      if (updateResponseData)
        localLogStartOffset = respData.logStartOffset
    }
    if (respData.preferredReadReplica.isPresent) {
      // If the broker computed a preferred read replica, we need to include it in the response
      mustRespond = true
    }
    if (respData.error.code != 0) {
      // Partitions with errors are always included in the response.
      // We also set the cached highWatermark to an invalid offset, -1.
      // This ensures that when the error goes away, we re-send the partition.
      if (updateResponseData)
        highWatermark = -1
      mustRespond = true
    }
    mustRespond
  }

  override def hashCode: Int = (31 * partition) + topic.hashCode

  def canEqual(that: Any) = that.isInstanceOf[CachedPartition]

  override def equals(that: Any): Boolean =
    that match {
      case that: CachedPartition =>
        this.eq(that) ||
          (that.canEqual(this) &&
            this.partition.equals(that.partition) &&
            this.topic.equals(that.topic))
      case _ => false
    }

  override def toString: String = synchronized {
    "CachedPartition(topic=" + topic +
      ", partition=" + partition +
      ", maxBytes=" + maxBytes +
      ", fetchOffset=" + fetchOffset +
      ", highWatermark=" + highWatermark +
      ", fetcherLogStartOffset=" + fetcherLogStartOffset +
      ", localLogStartOffset=" + localLogStartOffset  +
        ")"
  }
}

/**
  * The fetch session.
  *
  * Each fetch session is protected by its own lock, which must be taken before mutable
  * fields are read or modified.  This includes modification of the session partition map.
  *
  * @param id           The unique fetch session ID.
  * @param privileged   True if this session is privileged.  Sessions crated by followers
  *                     are privileged; sesssion created by consumers are not.
  * @param partitionMap The CachedPartitionMap.
  * @param creationMs   The time in milliseconds when this session was created.
  * @param lastUsedMs   The last used time in milliseconds.  This should only be updated by
  *                     FetchSessionCache#touch.
  * @param epoch        The fetch session sequence number.
  */
class FetchSession(val id: Int,
                   val privileged: Boolean,
                   val partitionMap: FetchSession.CACHE_MAP,
                   val creationMs: Long,
                   var lastUsedMs: Long,
                   var epoch: Int) {
  val lock = new ReentrantLock()

  // This is used by the FetchSessionCache to store the last known size of this session.
  // If this is -1, the Session is not in the cache.
  var cachedSize = -1

  def synchronize[X](action: => X): X = {
    lock.lock()
    try {
      action
    } finally {
      lock.unlock()
    }
  }

  def size: Int = synchronize {
    partitionMap.size
  }

  def isEmpty: Boolean = synchronize {
    partitionMap.isEmpty
  }

  def lastUsedKey: LastUsedKey = synchronize {
    LastUsedKey(lastUsedMs, id)
  }

  def evictableKey: EvictableKey = synchronize {
    EvictableKey(privileged, cachedSize, id)
  }

  def metadata: JFetchMetadata = synchronize { new JFetchMetadata(id, epoch) }

  def getFetchOffset(topicPartition: TopicPartition): Option[Long] = synchronize {
    Option(partitionMap.find(new CachedPartition(topicPartition))).map(_.fetchOffset)
  }

  type TL = util.ArrayList[TopicPartition]

  // Update the cached partition data based on the request.
  def update(fetchData: FetchSession.REQ_MAP,
             toForget: util.List[TopicPartition],
             reqMetadata: JFetchMetadata): (TL, TL, TL) = synchronize {
    val added = new TL
    val updated = new TL
    val removed = new TL
    fetchData.forEach { (topicPart, reqData) =>
      val newCachedPart = new CachedPartition(topicPart, reqData)
      val cachedPart = partitionMap.find(newCachedPart)
      if (cachedPart == null) {
        partitionMap.mustAdd(newCachedPart)
        added.add(topicPart)
      } else {
        cachedPart.updateRequestParams(reqData)
        updated.add(topicPart)
      }
    }
    toForget.forEach { p =>
      if (partitionMap.remove(new CachedPartition(p.topic, p.partition)))
        removed.add(p)
    }
    (added, updated, removed)
  }

  override def toString: String = synchronize {
    "FetchSession(id=" + id +
      ", privileged=" + privileged +
      ", partitionMap.size=" + partitionMap.size +
      ", creationMs=" + creationMs +
      ", lastUsedMs=" + lastUsedMs +
      ", epoch=" + epoch + ")"
  }
}

trait FetchContext extends Logging {
  /**
    * Get the fetch offset for a given partition.
    */
  def getFetchOffset(part: TopicPartition): Option[Long]

  /**
    * Apply a function to each partition in the fetch request.
    */
  def foreachPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit

  /**
    * Get the response size to be used for quota computation. Since we are returning an empty response in case of
    * throttling, we are not supposed to update the context until we know that we are not going to throttle.
    */
  def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int

  /**
    * Updates the fetch context with new partition information.  Generates response data.
    * The response data may require subsequent down-conversion.
    */
  def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse[Records]

  def partitionsToLogString(partitions: util.Collection[TopicPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)

  /**
    * Return an empty throttled response due to quota violation.
    */
  def getThrottledResponse(throttleTimeMs: Int): FetchResponse[Records] =
    new FetchResponse(Errors.NONE, new FetchSession.RESP_MAP, throttleTimeMs, INVALID_SESSION_ID)
}

/**
  * The fetch context for a fetch request that had a session error.
  */
class SessionErrorContext(val error: Errors,
                          val reqMetadata: JFetchMetadata) extends FetchContext {
  override def getFetchOffset(part: TopicPartition): Option[Long] = None

  override def foreachPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {}

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator)
  }

  // Because of the fetch session error, we don't know what partitions were supposed to be in this request.
  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse[Records] = {
    debug(s"Session error fetch context returning $error")
    new FetchResponse(error, new FetchSession.RESP_MAP, 0, INVALID_SESSION_ID)
  }
}

/**
  * The fetch context for a sessionless fetch request.
  *
  * @param fetchData          The partition data from the fetch request.
  */
class SessionlessFetchContext(val fetchData: util.Map[TopicPartition, FetchRequest.PartitionData]) extends FetchContext {
  override def getFetchOffset(part: TopicPartition): Option[Long] =
    Option(fetchData.get(part)).map(_.fetchOffset)

  override def foreachPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchData.forEach(fun(_, _))
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse[Records] = {
    debug(s"Sessionless fetch context returning ${partitionsToLogString(updates.keySet)}")
    new FetchResponse(Errors.NONE, updates, 0, INVALID_SESSION_ID)
  }
}

/**
  * The fetch context for a full fetch request.
  *
  * @param time               The clock to use.
  * @param cache              The fetch session cache.
  * @param reqMetadata        The request metadata.
  * @param fetchData          The partition data from the fetch request.
  * @param isFromFollower     True if this fetch request came from a follower.
  */
class FullFetchContext(private val time: Time,
                       private val cache: FetchSessionCache,
                       private val reqMetadata: JFetchMetadata,
                       private val fetchData: util.Map[TopicPartition, FetchRequest.PartitionData],
                       private val isFromFollower: Boolean) extends FetchContext {
  override def getFetchOffset(part: TopicPartition): Option[Long] =
    Option(fetchData.get(part)).map(_.fetchOffset)

  override def foreachPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    fetchData.forEach(fun(_, _))
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    FetchResponse.sizeOf(versionId, updates.entrySet.iterator)
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse[Records] = {
    def createNewSession: FetchSession.CACHE_MAP = {
      val cachedPartitions = new FetchSession.CACHE_MAP(updates.size)
      updates.forEach { (part, respData) =>
        val reqData = fetchData.get(part)
        cachedPartitions.mustAdd(new CachedPartition(part, reqData, respData))
      }
      cachedPartitions
    }
    val responseSessionId = cache.maybeCreateSession(time.milliseconds(), isFromFollower,
        updates.size, () => createNewSession)
    debug(s"Full fetch context with session id $responseSessionId returning " +
      s"${partitionsToLogString(updates.keySet)}")
    new FetchResponse(Errors.NONE, updates, 0, responseSessionId)
  }
}

/**
  * The fetch context for an incremental fetch request.
  *
  * @param time         The clock to use.
  * @param reqMetadata  The request metadata.
  * @param session      The incremental fetch request session.
  */
class IncrementalFetchContext(private val time: Time,
                              private val reqMetadata: JFetchMetadata,
                              private val session: FetchSession) extends FetchContext {

  override def getFetchOffset(tp: TopicPartition): Option[Long] = session.getFetchOffset(tp)

  override def foreachPartition(fun: (TopicPartition, FetchRequest.PartitionData) => Unit): Unit = {
    // Take the session lock and iterate over all the cached partitions.
    session.synchronize {
      session.partitionMap.forEach { part =>
        fun(new TopicPartition(part.topic, part.partition), part.reqData)
      }
    }
  }

  // Iterator that goes over the given partition map and selects partitions that need to be included in the response.
  // If updateFetchContextAndRemoveUnselected is set to true, the fetch context will be updated for the selected
  // partitions and also remove unselected ones as they are encountered.
  private class PartitionIterator(val iter: FetchSession.RESP_MAP_ITER,
                                  val updateFetchContextAndRemoveUnselected: Boolean)
    extends FetchSession.RESP_MAP_ITER {
    var nextElement: util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = null

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext) {
        val element = iter.next()
        val topicPart = element.getKey
        val respData = element.getValue
        val cachedPart = session.partitionMap.find(new CachedPartition(topicPart))
        val mustRespond = cachedPart.maybeUpdateResponseData(respData, updateFetchContextAndRemoveUnselected)
        if (mustRespond) {
          nextElement = element
          if (updateFetchContextAndRemoveUnselected) {
            session.partitionMap.remove(cachedPart)
            session.partitionMap.mustAdd(cachedPart)
          }
        } else {
          if (updateFetchContextAndRemoveUnselected) {
            iter.remove()
          }
        }
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = {
      if (!hasNext) throw new NoSuchElementException
      val element = nextElement
      nextElement = null
      element
    }

    override def remove() = throw new UnsupportedOperationException
  }

  override def getResponseSize(updates: FetchSession.RESP_MAP, versionId: Short): Int = {
    session.synchronize {
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        FetchResponse.sizeOf(versionId, (new FetchSession.RESP_MAP).entrySet.iterator)
      } else {
        // Pass the partition iterator which updates neither the fetch context nor the partition map.
        FetchResponse.sizeOf(versionId, new PartitionIterator(updates.entrySet.iterator, false))
      }
    }
  }

  override def updateAndGenerateResponseData(updates: FetchSession.RESP_MAP): FetchResponse[Records] = {
    session.synchronize {
      // Check to make sure that the session epoch didn't change in between
      // creating this fetch context and generating this response.
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        info(s"Incremental fetch session ${session.id} expected epoch $expectedEpoch, but " +
          s"got ${session.epoch}.  Possible duplicate request.")
        new FetchResponse(Errors.INVALID_FETCH_SESSION_EPOCH, new FetchSession.RESP_MAP, 0, session.id)
      } else {
        // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
        val partitionIter = new PartitionIterator(updates.entrySet.iterator, true)
        while (partitionIter.hasNext) {
          partitionIter.next()
        }
        debug(s"Incremental fetch context with session id ${session.id} returning " +
          s"${partitionsToLogString(updates.keySet)}")
        new FetchResponse(Errors.NONE, updates, 0, session.id)
      }
    }
  }

  override def getThrottledResponse(throttleTimeMs: Int): FetchResponse[Records] = {
    session.synchronize {
      // Check to make sure that the session epoch didn't change in between
      // creating this fetch context and generating this response.
      val expectedEpoch = JFetchMetadata.nextEpoch(reqMetadata.epoch)
      if (session.epoch != expectedEpoch) {
        info(s"Incremental fetch session ${session.id} expected epoch $expectedEpoch, but " +
          s"got ${session.epoch}.  Possible duplicate request.")
        new FetchResponse(Errors.INVALID_FETCH_SESSION_EPOCH, new FetchSession.RESP_MAP, throttleTimeMs, session.id)
      } else {
        new FetchResponse(Errors.NONE, new FetchSession.RESP_MAP, throttleTimeMs, session.id)
      }
    }
  }
}

case class LastUsedKey(lastUsedMs: Long, id: Int) extends Comparable[LastUsedKey] {
  override def compareTo(other: LastUsedKey): Int =
    (lastUsedMs, id) compare (other.lastUsedMs, other.id)
}

case class EvictableKey(privileged: Boolean, size: Int, id: Int) extends Comparable[EvictableKey] {
  override def compareTo(other: EvictableKey): Int =
    (privileged, size, id) compare (other.privileged, other.size, other.id)
}

/**
  * Caches fetch sessions.
  *
  * See tryEvict for an explanation of the cache eviction strategy.
  *
  * The FetchSessionCache is thread-safe because all of its methods are synchronized.
  * Note that individual fetch sessions have their own locks which are separate from the
  * FetchSessionCache lock.  In order to avoid deadlock, the FetchSessionCache lock
  * must never be acquired while an individual FetchSession lock is already held.
  *
  * @param maxEntries The maximum number of entries that can be in the cache.
  * @param evictionMs The minimum time that an entry must be unused in order to be evictable.
  */
class FetchSessionCache(private val maxEntries: Int,
                        private val evictionMs: Long) extends Logging with KafkaMetricsGroup {
  // This is only used for metrics, so its state changes don't need to be atomic wrt other members
  private val numPartitions = new AtomicLong()

  // A map of session ID to FetchSession.
  private val sessions = new ConcurrentHashMap[Int, FetchSession]

  // The number of free slots in sessions map.
  // If there are none then it will be necessary to try to evict another session
  private val freeSlots = new Semaphore(maxEntries)

  // Maps last used times to sessions.
  private val lastUsed = new ConcurrentSkipListMap[LastUsedKey, FetchSession]

  // A map containing sessions which can be evicted by both privileged and
  // unprivileged sessions.
  private val evictableByAll = new ConcurrentSkipListMap[EvictableKey, FetchSession]

  // A map containing sessions which can be evicted by privileged sessions.
  private val evictableByPrivileged = new ConcurrentSkipListMap[EvictableKey, FetchSession]

  // Set up metrics.
  removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_SESSISONS)
  newGauge(FetchSession.NUM_INCREMENTAL_FETCH_SESSISONS, () => FetchSessionCache.this.size)
  removeMetric(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED)
  newGauge(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED, () => FetchSessionCache.this.totalPartitions)
  removeMetric(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC)
  private[server] val evictionsMeter = newMeter(FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC,
    FetchSession.EVICTIONS, TimeUnit.SECONDS, Map.empty)


  /**
    * Get a session by session ID.
    *
    * @param sessionId  The session ID.
    * @return           The session, or None if no such session was found.
    */
  def get(sessionId: Int): Option[FetchSession] = {
    Option(sessions.get(sessionId))
  }

  /**
    * Get the number of entries currently in the fetch session cache.
    */
  def size: Int = {
    maxEntries - freeSlots.availablePermits()
  }

  /**
    * Get the total number of cached partitions.
    */
  def totalPartitions: Long = {
    numPartitions.get()
  }

  /**
    * Creates a new random session ID which will be positive.
    * The returned id is not guaranteed to be unique on this broker
    * (because there's no lock preventing another thread from generating the same id).
    * It's uniqueness can only be proven when it's added to sessions.
    *
    * @return   The new session ID.
    */
  def newSessionId(): Int = {
    var id = 0
    do {
      id = ThreadLocalRandom.current().nextInt(1, Int.MaxValue)
    } while (sessions.containsKey(id) || id == INVALID_SESSION_ID)
    id
  }

  /**
    * Try to create a new session.
    *
    * @param now                The current time in milliseconds.
    * @param privileged         True if the new entry we are trying to create is privileged.
    * @param size               The number of cached partitions in the new entry we are trying to create.
    * @param createPartitions   A callback function which creates the map of cached partitions.
    * @return                   If we created a session, the ID; INVALID_SESSION_ID otherwise.
    */
  def maybeCreateSession(now: Long,
                         privileged: Boolean,
                         size: Int,
                         createPartitions: () => FetchSession.CACHE_MAP): Int = {
    // If there is room, create a new session entry.
    if (freeSlots.tryAcquire() || tryEvict(privileged, EvictableKey(privileged, size, 0), now)) {
      val partitionMap = createPartitions()
      var session = createSession(now, privileged, partitionMap)
      // Note the session is locked before it's discoverable via shared state (sessions, lastUsed or evictable maps)
      session.lock.lock()
      while (sessions.putIfAbsent(session.id, session) != null) {
        // because computing a session id is not atomic with adding it to sessions
        // there's the possibility that two threads allocate the same id to different sessions
        // so we use the atomicity of putIfAbsent and recompute if there is a collision.
        session = createSession(now, privileged, partitionMap)
        session.lock.lock()
      }
      try {
        debug(s"Created fetch session ${session.toString}")
        touch(session, now)
      } finally {
        session.lock.unlock()
      }
      session.id
    } else {
      debug(s"No fetch session created for privileged=$privileged, size=$size.")
      INVALID_SESSION_ID
    }
  }

  private def createSession(now: Long, privileged: Boolean, partitionMap: CACHE_MAP) = {
    new FetchSession(newSessionId(), privileged, partitionMap,
      now, now, JFetchMetadata.nextEpoch(INITIAL_EPOCH))
  }

  /**
    * Try to evict an entry from the session cache.
    *
    * A proposed new element A may evict an existing element B if:
    * 1. A is privileged and B is not, or
    * 2. B is considered "stale" because it has been inactive for a long time, or
    * 3. A contains more partitions than B, and B is not recently created.
    *
    * @param privileged True if the new entry we would like to add is privileged.
    * @param key        The EvictableKey for the new entry we would like to add.
    * @param now        The current time in milliseconds.
    * @return           True if an entry was evicted; false otherwise.
    */
  def tryEvict(privileged: Boolean, key: EvictableKey, now: Long): Boolean = {
    // Try to evict an entry which is stale.
    var stop = false;
    while (!stop) {
      val it = lastUsed.entrySet().iterator()
      while (!stop && it.hasNext) {
        val lastUsedEntry = it.next()
        if (now - lastUsedEntry.getKey.lastUsedMs > evictionMs) {
          val session = lastUsedEntry.getValue
          if (session.lock.tryLock()) {
            try {
              if (sessions.containsKey(session.id)) {
                // We have to check sessions still contains this session, since another thread might have evicted it
                // it after get got the first entry and before we acquired the lock
                trace(s"Evicting stale FetchSession ${session.id}.")
                // Don't release the freeLocks semaphore, since successful eviction means a new session will be added
                remove(session, false)
                evictionsMeter.mark()
                return true
              }
            } finally {
              session.lock.unlock()
            }
          } // else try the next session in lastUsed, since one stale session is as evictable as another
        } else {
          // We got the end of the stale sessions, give up on staleness-based eviction.
          stop = true
        }
      }
      // There are not more sessions in lastUses, give up
      stop = true
    }

    // If there are no stale entries, check the first evictable entry.
    // If it is less valuable than our proposed entry, evict it.
    stop = false
    val map = if (privileged) evictableByPrivileged else evictableByAll
    while (!stop) {
      val evictableEntry = map.firstEntry()
      if (evictableEntry != null) {
        val session = evictableEntry.getValue
        if (session.lock.tryLock()) {
          try {
            if (key.compareTo(evictableEntry.getKey) < 0) {
              trace(s"Can't evict ${evictableEntry.getKey} with ${key.toString}")
              return false
            } else if (sessions.containsKey(session.id)) {
              // We have to check sessions still contains this session, since another thread might have evicted it
              // it after get got the first entry and before we acquired the lock
              trace(s"Evicting ${evictableEntry.getKey} with ${key.toString}.")
              // Don't release the freeLocks semaphore, since successful eviction means a new session will be added
              remove(session, false)
              evictionsMeter.mark()
              return true
            }
          } finally {
            session.lock.unlock()
          }
        }
        // else we can't lock session. Unlike stale eviction, the next entry in the map is more more valuable
        //  than this one, so try to get the first element again
      } else {
        stop = true
      }
    }
    trace("No evictable entries found.")
    false
  }

  def remove(sessionId: Int): Option[FetchSession] = {
    var sessionOpt = get(sessionId)
    while (sessionOpt.isDefined) {
      val session = sessionOpt.get
      if (session.lock.tryLock()) {
        try {
          remove(session)
        } finally {
          session.lock.unlock()
        }
        return sessionOpt
      }
      sessionOpt = get(sessionId)
    }
    None
  }

  /**
    * Remove an entry from the session cache.
    * The session's lock must be held by the caller.
    *
    * @param session      The session.
    * @param releaseSlot  Whether to decrement freeSlots, allowing other threads to create a session without eviction.
    *
    * @return             The removed session, or None if there was no such session.
    */
  def remove(session: FetchSession, releaseSlot: Boolean = true): Option[FetchSession] = {
    // session's lock must be held by the caller of this method
    val count = session.lock.getHoldCount
    if (count != 1) {
      throw new IllegalStateException("Unexpected count " + count)
    }

    if (lastUsed.remove(session.lastUsedKey) != session) {
      throw new IllegalStateException(s"Removing session ${session}")
    }
    val evictableKey = session.evictableKey
    evictableByAll.remove(evictableKey)
    evictableByPrivileged.remove(evictableKey)
    val removeResult = Option(sessions.remove(session.id))
    if (removeResult.isDefined) {
      numPartitions.addAndGet(-session.cachedSize)
    } else {
      throw new IllegalStateException(s"Removing session ${session}")
    }
    if (releaseSlot) {
      freeSlots.release()
    }
    removeResult
  }

  /**
    * Update a session's position in the lastUsed and evictable trees.
    * The session's lock must be held by the caller.
    *
    * @param session  The session.
    * @param now      The current time in milliseconds.
    */
  def touch(session: FetchSession, now: Long): Unit = {
    // session's lock must be held by the caller of this method
    val count = session.lock.getHoldCount
    if (count != 1) {
      throw new IllegalStateException("Unexpected count " + count)
    }
    // Update the lastUsed map.
    lastUsed.remove(session.lastUsedKey)
    session.lastUsedMs = now
    lastUsed.put(session.lastUsedKey, session)
    val oldSize = session.cachedSize

    val oldEvictableKey = session.evictableKey
    val oldSizeDecrement = if (oldSize != -1) oldSize else 0
    session.cachedSize = session.size
    val newEvictableKey = session.evictableKey
    if ((!session.privileged) || (now - session.creationMs > evictionMs)) {
      evictableByPrivileged.put(newEvictableKey, session)
    }
    if (oldEvictableKey != newEvictableKey) {
      evictableByPrivileged.remove(oldEvictableKey)
    }

    if (now - session.creationMs > evictionMs) {
      evictableByAll.put(newEvictableKey, session)
    }
    if (oldEvictableKey != newEvictableKey) {
      evictableByAll.remove(oldEvictableKey)
    }
    numPartitions.addAndGet(session.cachedSize - oldSizeDecrement)
  }
}

class FetchManager(private val time: Time,
                   private val cache: FetchSessionCache) extends Logging {
  def newContext(reqMetadata: JFetchMetadata,
                 fetchData: FetchSession.REQ_MAP,
                 toForget: util.List[TopicPartition],
                 isFollower: Boolean): FetchContext = {
    val context = if (reqMetadata.isFull) {
      var removedFetchSessionStr = ""
      if (reqMetadata.sessionId != INVALID_SESSION_ID) {
        // Any session specified in a FULL fetch request will be closed.
        if (cache.remove(reqMetadata.sessionId).isDefined) {
          removedFetchSessionStr = s" Removed fetch session ${reqMetadata.sessionId}."
        }
      }
      var suffix = ""
      val context = if (reqMetadata.epoch == FINAL_EPOCH) {
        // If the epoch is FINAL_EPOCH, don't try to create a new session.
        suffix = " Will not try to create a new session."
        new SessionlessFetchContext(fetchData)
      } else {
        new FullFetchContext(time, cache, reqMetadata, fetchData, isFollower)
      }
      debug(s"Created a new full FetchContext with ${partitionsToLogString(fetchData.keySet)}."+
        s"${removedFetchSessionStr}${suffix}")
      context
    } else {
      var sessionOpt: Option[FetchContext] = None
      while (sessionOpt.isEmpty) {
        sessionOpt = cache.get(reqMetadata.sessionId) match {
          case None =>
            debug(s"Session error for ${reqMetadata.sessionId}: no such session ID found.")
            Some(new SessionErrorContext(Errors.FETCH_SESSION_ID_NOT_FOUND, reqMetadata))
          case Some(session) =>
            if (session.epoch != reqMetadata.epoch) {
              debug(s"Session error for ${reqMetadata.sessionId}: expected epoch " +
                s"${session.epoch}, but got ${reqMetadata.epoch} instead.");
              Some(new SessionErrorContext(Errors.INVALID_FETCH_SESSION_EPOCH, reqMetadata))
            } else {
              if (session.lock.tryLock()) {
                try {
                  val (added, updated, removed) = session.update(fetchData, toForget, reqMetadata)
                  if (session.isEmpty) {
                    debug(s"Created a new sessionless FetchContext and closing session id ${session.id}, " +
                      s"epoch ${session.epoch}: after removing ${partitionsToLogString(removed)}, " +
                      s"there are no more partitions left.")
                    cache.remove(session)
                    Some(new SessionlessFetchContext(fetchData))
                  } else {
                    session.epoch = JFetchMetadata.nextEpoch(session.epoch)
                    debug(s"Created a new incremental FetchContext for session id ${session.id}, " +
                      s"epoch ${session.epoch}: added ${partitionsToLogString(added)}, " +
                      s"updated ${partitionsToLogString(updated)}, " +
                      s"removed ${partitionsToLogString(removed)}")
                    cache.touch(session, time.milliseconds())
                    Some(new IncrementalFetchContext(time, reqMetadata, session))
                  }
                } finally {
                  session.lock.unlock()
                }
              } else {
                // re-read from cache
                None
              }
            }
        }
      }// while
      sessionOpt.get
    }
    context
  }

  def partitionsToLogString(partitions: util.Collection[TopicPartition]): String =
    FetchSession.partitionsToLogString(partitions, isTraceEnabled)
}
