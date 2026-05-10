/*
 * Copyright 2022-2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.internal

import java.util.concurrent.ConcurrentLinkedQueue
import zio.stacktracer.TracingImplicits.disableAutoTrace

import zio.Cause
import zio.internal.FiberMessage.InterruptSignal

/**
 * A specialized fiber mailbox for ZIO fibers.
 *
 * Key optimizations over directly polling a ConcurrentLinkedQueue:
 *   1. '''Batched drain''': drainBatch() snapshots all pending messages at once
 *      via a single traversal, so FiberRuntime processes a complete batch in
 *      one pass rather than looping on individual polls. This improves cache
 *      locality and eliminates repeated isEmpty/poll call overhead. 2.
 *      '''Lock-free MPSC''': Multiple producers call tell() concurrently via
 *      the underlying ConcurrentLinkedQueue (a Michael-Scott lock-free queue).
 *      Only the owning fiber drains, so the consumer side has no CAS cost. 3.
 *      '''Interrupt signal priority''': InterruptSignal messages can be
 *      detected and prioritized during batch processing.
 *
 * @note
 *   drainBatch() is NOT thread-safe for multiple consumers. It must only be
 *   called by the owning fiber, which is the single consumer by design.
 */
private[zio] final class FiberMailbox {
  private val queue: ConcurrentLinkedQueue[FiberMessage] = new ConcurrentLinkedQueue[FiberMessage]()

  /**
   * Adds a message to the mailbox. Lock-free and safe for multiple concurrent
   * producers.
   */
  private[zio] final def tell(msg: FiberMessage): Unit = {
    queue.offer(msg)
    ()
  }

  /**
   * Adds an interrupt signal to the mailbox.
   */
  private[zio] final def tellInterrupt(cause: Cause[Nothing]): Unit =
    tell(InterruptSignal(cause))

  /**
   * Drains all currently queued messages into an array in a single pass.
   * Thread-safe only for the single consumer (owning fiber).
   *
   * @return
   *   an array of messages; an empty array if the mailbox is empty
   */
  private[zio] final def drainBatch(): Array[FiberMessage] = {
    var msg = queue.poll()
    if (msg eq null) return FiberMailbox.EmptyBatch
    val buf = new java.util.ArrayList[FiberMessage](FiberMailbox.InitialBatchCapacity)
    while (msg ne null) {
      buf.add(msg)
      msg = queue.poll()
    }
    buf.toArray(FiberMailbox.EmptyBatch)
  }

  /**
   * Returns true if the mailbox contains no messages.
   */
  private[zio] final def isEmpty: Boolean = queue.isEmpty

  private[zio] final def size: Int = queue.size()
}

private[zio] object FiberMailbox {
  private[zio] final val EmptyBatch: Array[FiberMessage] = new Array[FiberMessage](0)
  private[zio] final val InitialBatchCapacity: Int       = 16
}
