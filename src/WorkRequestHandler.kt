// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
@file:Suppress("ReplaceGetOrSet")

package org.jetbrains.bazel.jvm

import java.io.PrintStream
import java.util.function.BiConsumer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiFunction
import java.io.PrintWriter
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest
import java.io.IOException
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse
import com.sun.management.OperatingSystemMXBean
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.time.Instant
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.StringWriter
import java.lang.AutoCloseable
import java.lang.Error
import java.lang.Exception
import java.lang.RuntimeException
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import kotlin.system.exitProcess

/**
 * A helper class that handles [WorkRequests](https://bazel.build/docs/persistent-workers), including
 * [multiplex workers](https://bazel.build/docs/multiplex-worker).
 */
class WorkRequestHandler internal constructor(
  /**
   * The function to be called after each [WorkRequest] is read.
   */
  private val callback: WorkRequestCallback,
  /**
   * This worker's stderr.
   */
  private val stderr: PrintStream,
  val messageProcessor: WorkerMessageProcessor,
  cpuUsageBeforeGc: Duration,
  private val cancelCallback: BiConsumer<Int, Thread>?,
  idleTimeBeforeGc: Duration,
) : AutoCloseable {
  /**
   * Requests that are currently being processed. Visible for testing.
   */
  // visible for test
  internal val activeRequests: ConcurrentHashMap<Int, RequestInfo> = ConcurrentHashMap<Int, RequestInfo>()

  /**
   * A scheduler that runs garbage collection after a certain amount of CPU time has passed. In our
   * experience, explicit GC reclaims much more than implicit GC. This scheduler helps make sure
   * very busy workers don't grow ridiculously large.
   */
  private val gcScheduler = CpuTimeBasedGcScheduler(cpuUsageBeforeGc)

  /**
   * A scheduler that runs garbage collection after a certain amount of time without any activity.
   * In our experience, explicit GC reclaims much more than implicit GC. This scheduler helps make
   * sure workers don't hang on to excessive memory after they are done working.
   */
  private val idleGcScheduler = IdleGcScheduler(idleTimeBeforeGc)

  /**
   * If set, this worker will stop handling requests and shut itself down. This can happen if
   * something throws an [Error].
   */
  private val shutdownWorker = AtomicBoolean(false)

  //TestOnly
  internal constructor(
    callback: WorkRequestCallback,
    stderr: PrintStream,
    messageProcessor: WorkerMessageProcessor
  ) : this(
    callback = callback,
    stderr = stderr,
    messageProcessor = messageProcessor,
    cpuUsageBeforeGc = Duration.ZERO,
    cancelCallback = null,
    idleTimeBeforeGc = Duration.ZERO,
  )

  /**
   * Runs an infinite loop of reading [WorkRequest] from `in`, running the callback,
   * then writing the corresponding [WorkResponse] to `out`. If there is an error
   * reading or writing the requests or responses, it writes an error message on `err` and
   * returns. If `in` reaches EOF, it also returns.
   *
   * This function also wraps the system streams in a [WorkerIo] instance that prevents the
   * underlying tool from writing to [System.out] or reading from [System.in], which
   * would corrupt the worker protocol. When the while loop exits, the original system
   * streams will be swapped back into [System].
   */
  @Throws(IOException::class)
  fun processRequests() {
    // wrap the system streams into a WorkerIO instance to prevent unexpected reads and writes on stdin/stdout
    val workerIo = wrapStandardSystemStreams()
    try {
      while (!shutdownWorker.get()) {
        val request = messageProcessor.readWorkRequest()
        idleGcScheduler.markActivity(true)
        if (request == null) {
          break
        }
        if (request.cancel) {
          respondToCancelRequest(request)
        }
        else {
          startResponseThread(workerIo, request)
        }
      }
    }
    catch (e: IOException) {
      stderr.println("Error reading next WorkRequest: $e")
      e.printStackTrace(stderr)
    }
    finally {
      idleGcScheduler.stop()
      // TODO(b/220878242): Give the outstanding requests a chance to send a "shutdown" response,
      // but also try to kill stuck threads. For now, we just interrupt the remaining threads.
      // We considered doing System.exit here, but that is hard to test and would deny the callers
      // of this method a chance to clean up. Instead, we initiate the cleanup of our resources here
      // and the caller can decide whether to wait for an orderly shutdown or now.
      for (ri in activeRequests.values) {
        if (ri.thread.isAlive) {
          try {
            ri.thread.interrupt()
          }
          catch (_: RuntimeException) {
            // If we can't interrupt, we can't do much else.
          }
        }
      }

      try {
        // Unwrap the system streams placing the original streams back
        workerIo.close()
      }
      catch (e: Exception) {
        stderr.println(e.message)
      }
    }
  }

  /**
   * Starts a thread for the given request.
   */
  fun startResponseThread(workerIO: WorkerIo, request: WorkRequest) {
    val currentThread = Thread.currentThread()
    val threadName = if (request.requestId > 0) "multiplex-request-" + request.requestId else "singleplex-request"
    if (request.requestId == 0) {
      while (activeRequests.containsKey(request.requestId)) {
        // Previous singleplex requests can still be in activeRequests for a bit after the response has been sent.
        // We need to wait for them to vanish.
        try {
          Thread.sleep(1)
        }
        catch (_: InterruptedException) {
          Thread.currentThread().interrupt()
          return
        }
      }
    }

    val t = Thread(
      Runnable {
        val requestInfo = activeRequests.get(request.requestId)
        if (requestInfo == null) {
          // already canceled
          idleGcScheduler.markActivity(!activeRequests.isEmpty())
          return@Runnable
        }
        try {
          respondToRequest(workerIo = workerIO, request = request, requestInfo = requestInfo)
        }
        catch (e: IOException) {
          // IOExceptions here means a problem talking to the server, so we must shut down.
          if (!shutdownWorker.compareAndSet(false, true)) {
            stderr.println("Error communicating with server, shutting down worker.")
            e.printStackTrace(stderr)
            currentThread.interrupt()
          }
        }
        finally {
          activeRequests.remove(request.requestId)
          idleGcScheduler.markActivity(!activeRequests.isEmpty())
        }
      },
      threadName
    )
    t.setUncaughtExceptionHandler(
      Thread.UncaughtExceptionHandler { t1, e ->
        // Shut down the worker in case of severe issues. We don't handle RuntimeException here,
        // as those are not serious enough to merit shutting down the worker.
        if (e is Error && shutdownWorker.compareAndSet(false, true)) {
          stderr.println("Error thrown by worker thread, shutting down worker.")
          e.printStackTrace(stderr)
          currentThread.interrupt()
          idleGcScheduler.stop()
          exitProcess(1)
        }
      })
    val previous = activeRequests.putIfAbsent(request.requestId, RequestInfo(t))
    check(previous == null) { "Request still active: ${request.requestId}" }
    t.start()
  }

  /**
   * Handles and responds to the given [WorkRequest].
   *
   * @throws IOException if there is an error talking to the server. Errors from calling the [][.callback] are reported with exit code 1.
   */
  @Throws(IOException::class)
  // visible for tests
  internal fun respondToRequest(workerIo: WorkerIo, request: WorkRequest, requestInfo: RequestInfo) {
    var exitCode: Int
    val sw = StringWriter()
    PrintWriter(sw).use { pw ->
      try {
        exitCode = callback.apply(request, pw)
      }
      catch (_: InterruptedException) {
        exitCode = 1
      }
      catch (e: RuntimeException) {
        e.printStackTrace(pw)
        exitCode = 1
      }
      try {
        // read out the captured string for the final WorkResponse output
        val captured = workerIo.readCapturedAsUtf8String().trim()
        if (!captured.isEmpty()) {
          pw.write(captured)
        }
      }
      catch (e: IOException) {
        stderr.println(e.message)
      }
    }
    val builder = requestInfo.takeBuilder()
    if (builder != null) {
      builder.setRequestId(request.requestId)
      if (requestInfo.isCancelled()) {
        builder.setWasCancelled(true)
      }
      else {
        builder.setOutput(builder.getOutput() + sw).setExitCode(exitCode)
      }
      val response = builder.build()
      synchronized(this) {
        messageProcessor.writeWorkResponse(response)
      }
    }
    gcScheduler.maybePerformGc()
  }

  /**
   * Marks the given request as canceled and uses [.cancelCallback] to request cancellation.
   *
   * For simplicity, and to avoid blocking in [.cancelCallback], response to cancellation
   * is still handled by [.respondToRequest] once the canceled request aborts (or finishes).
   */
  fun respondToCancelRequest(request: WorkRequest) {
    // Theoretically, we could have gotten two singleplex requests, and we can't tell those apart.
    // However, that's a violation of the protocol, so we don't try to handle it (not least because handling it would be quite error-prone).
    val ri = activeRequests.get(request.requestId) ?: return
    if (cancelCallback == null) {
      ri.setCancelled()
      // This is either an error on the server side or a version mismatch between the server setup and the binary.
      // It's better to wait for the regular work to finish instead of breaking the build, but we should inform the user about the bad setup.
      ri.addOutput("Cancellation request received for worker request ${request.requestId}, " +
                     "but this worker does not support cancellation.\n")
    }
    else if (ri.thread.isAlive && !ri.isCancelled()) {
      ri.setCancelled()
      // Response will be sent from request thread once request handler returns.
      // We can ignore any exceptions in cancel callback since it's best effort.
      val t = Thread({ cancelCallback.accept(request.requestId, ri.thread) }, "cancel-request-${request.requestId}-callback")
      t.start()
    }
  }

  @Throws(IOException::class)
  override fun close() {
    messageProcessor.close()
  }
}

/**
 * Contains the logic for reading [WorkRequest]s and writing [WorkResponse]s.
 */
interface WorkerMessageProcessor {
  /**
   * Reads the next incoming request from this worker's stdin.
   */
  @Throws(IOException::class)
  fun readWorkRequest(): WorkRequest?

  /**
   * Writes the provided [WorkResponse] to this worker's stdout. This function is also responsible for flushing the stdout.
   */
  @Throws(IOException::class)
  fun writeWorkResponse(workResponse: WorkResponse)

  /**
   * Clean up.
   */
  @Throws(IOException::class)
  fun close()
}

/**
 * Holds information necessary to properly handle a request, especially for cancellation.
 */
internal class RequestInfo(
  /**
   * The thread handling the request.
   */
  @JvmField val thread: Thread,
) {
  /**
   * If true, we have received a cancel request for this request.
   */
  private val cancelled = AtomicBoolean(false)

  /**
   * The builder for the response to this request. Since only one response must be sent per
   * request, this builder must be accessed through takeBuilder(), which zeroes this field and
   * returns the builder.
   */
  private var responseBuilder: WorkResponse.Builder? = WorkResponse.newBuilder()

  /**
   * Sets whether this request has been canceled.
   */
  fun setCancelled() {
    cancelled.set(true)
  }

  /**
   * Returns true if this request has been canceled.
   */
  fun isCancelled(): Boolean = cancelled.get()

  /**
   * Returns the response builder. If called more than once on the same instance, later calls will return `null`.
   */
  @Synchronized
  fun takeBuilder(): WorkResponse.Builder? {
    val b = responseBuilder
    responseBuilder = null
    return b
  }

  /**
   * Adds `s` as output to when the response eventually gets built. Does nothing if the response has already been taken.
   * There is no guarantee that the response hasn't already been taken, making this call a no-op. This may be called multiple times.
   * No delimiters are added between strings from multiple calls.
   */
  @Synchronized
  fun addOutput(s: String) {
    responseBuilder?.let {
      it.setOutput(it.getOutput() + s)
    }
  }
}

/**
 * A wrapper class for the callback BiFunction
 */
class WorkRequestCallback(
  /**
   * Callback method for executing a single WorkRequest in a thread.
   * The first argument to `callback` is the WorkRequest, the second is where all error messages and other user-oriented
   * messages should be written to. The callback must return an exit code indicating success (zero) or failure (nonzero).
   */
  private val callback: BiFunction<WorkRequest, PrintWriter, Int>
) {
  @Throws(InterruptedException::class)
  fun apply(workRequest: WorkRequest, printWriter: PrintWriter): Int {
    val result = callback.apply(workRequest, printWriter)
    if (Thread.interrupted()) {
      throw InterruptedException("Work request interrupted: " + workRequest.requestId)
    }
    return result
  }
}

/**
 * Builder class for WorkRequestHandler. Required parameters are passed to the constructor.
 */
class WorkRequestHandlerBuilder
/**
 * Creates a `WorkRequestHandlerBuilder`.
 *
 * @param callback         WorkRequestCallback object with Callback method for executing a single
 * WorkRequest in a thread. The first argument to `callback` is the WorkRequest, the
 * second is where all error messages and other user-oriented messages should be written to.
 * The callback must return an exit code indicating success (zero) or failure (nonzero).
 * @param stderr           Stream that log messages should be written to, typically the process' stderr.
 * @param messageProcessor Object responsible for parsing `WorkRequest`s from the server
 * and writing `WorkResponses` to the server.
 */(
  private val callback: WorkRequestCallback,
  private val stderr: PrintStream,
  private val messageProcessor: WorkerMessageProcessor,
) {
  private var cpuUsageBeforeGc: Duration = Duration.ZERO
  private var cancelCallback: ((Int, Thread) -> Unit)? = null
  private var idleTimeBeforeGc: Duration = Duration.ZERO

  /**
   * Sets the minimum amount of CPU time between explicit garbage collection calls.
   * Pass `Duration.ZERO` to not do explicit garbage collection (the default).
   */
  @Suppress("unused")
  fun setCpuUsageBeforeGc(cpuUsageBeforeGc: Duration): WorkRequestHandlerBuilder {
    this.cpuUsageBeforeGc = cpuUsageBeforeGc
    return this
  }

  /**
   * Sets a callback will be called when a cancellation message has been received.
   * The callback will be called with the request ID and the thread executing the request.
   */
  fun setCancelCallback(cancelCallback: ((Int, Thread) -> Unit)?): WorkRequestHandlerBuilder {
    this.cancelCallback = cancelCallback
    return this
  }

  /**
   * Sets the time without any work that should elapse before forcing a GC.
   */
  @Suppress("unused")
  fun setIdleTimeBeforeGc(idleTimeBeforeGc: Duration): WorkRequestHandlerBuilder {
    this.idleTimeBeforeGc = idleTimeBeforeGc
    return this
  }

  /**
   * Returns a WorkRequestHandler instance with the values in this Builder.
   */
  fun build(): WorkRequestHandler {
    return WorkRequestHandler(
      callback = callback,
      stderr = stderr,
      messageProcessor = messageProcessor,
      cpuUsageBeforeGc = cpuUsageBeforeGc,
      cancelCallback = cancelCallback,
      idleTimeBeforeGc = idleTimeBeforeGc
    )
  }
}


/**
 * Schedules GC when the worker has been idle for a while
 */
private class IdleGcScheduler
/**
 * Creates a new scheduler.
 *
 * @param idleTimeBeforeGc The time from the last activity until attempting GC.
 */(
  /**
   * Minimum duration from the end of activity until we perform an idle GC.
   */
  private val idleTimeBeforeGc: Duration
) {
  private val executor = ScheduledThreadPoolExecutor(1)
  private var lastActivity: Instant = Instant.EPOCH
  private var lastGc: Instant = Instant.EPOCH
  private var futureGc: ScheduledFuture<*>? = null

  @Synchronized
  fun start() {
    if (!idleTimeBeforeGc.isZero) {
      futureGc = executor.schedule({ maybeDoGc() }, idleTimeBeforeGc.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Should be called whenever there is some sort of activity starting or ending. Better to call too often.
   */
  @Synchronized
  fun markActivity(anythingActive: Boolean) {
    lastActivity = Instant.now()
    futureGc?.let {
      it.cancel(false)
      futureGc = null
    }
    if (!anythingActive) {
      start()
    }
  }

  fun maybeDoGc() {
    if (lastGc.isBefore(lastActivity) && lastActivity.isBefore(Instant.now().minus(idleTimeBeforeGc))) {
      System.gc()
      lastGc = Instant.now()
    }
    else {
      start()
    }
  }

  @Synchronized
  fun stop() {
    futureGc?.let {
      it.cancel(false)
      futureGc = null
    }
    executor.shutdown()
  }
}


/**
 * Class that performs GC occasionally, based on how much CPU time has passed. This strikes a
 * compromise between blindly doing GC after e.g., every request, which takes too much CPU, and not
 * doing explicit GC at all, which causes poor garbage collection in some cases.
 */
private class CpuTimeBasedGcScheduler(
  /**
   * After this much CPU time has elapsed, we may force a GC run. Set to [Duration.ZERO] to
   * disable.
   */
  private val cpuUsageBeforeGc: Duration
) {
  companion object {
    /**
     * Used to get the CPU time used by this process.
     */
    private val bean = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
  }

  /**
   * The total process CPU time at the last GC run (or from the start of the worker).
   */
  private val cpuTimeAtLastGc = AtomicReference(cpuTime)

  val cpuTime: Duration
    get() = if (cpuUsageBeforeGc.isZero) Duration.ZERO else Duration.ofNanos(bean.processCpuTime)

  /**
   * Call occasionally to perform a GC if enough CPU time has been used.
   */
  fun maybePerformGc() {
    if (cpuUsageBeforeGc.isZero) {
      return
    }

    val currentCpuTime = cpuTime
    val lastCpuTime = cpuTimeAtLastGc.get()
    // do GC when enough CPU time has been used, but only if nobody else beat us to it
    if (currentCpuTime.minus(lastCpuTime) > cpuUsageBeforeGc && cpuTimeAtLastGc.compareAndSet(lastCpuTime, currentCpuTime)) {
      System.gc()
      // avoid counting GC CPU time against CPU time before the next GC
      cpuTimeAtLastGc.compareAndSet(currentCpuTime, this.cpuTime)
    }
  }
}

/**
 * A class that wraps the standard [System.in], [System.out], and [System.err]
 * with our own ByteArrayOutputStream that allows [WorkRequestHandler] to safely capture
 * outputs that can't be directly captured by the PrintStream associated with the work request.
 *
 * This is most useful when integrating JVM tools that write exceptions and logs directly to
 * [System.out] and [System.err], which would corrupt the persistent worker protocol.
 * We also redirect [System.in], just in case a tool should attempt to read it.
 *
 *
 * WorkerIO implements [AutoCloseable] and will swap the original streams back into
 * [System] once close has been called.
 */
class WorkerIo
/**
 * Creates a new [WorkerIo] that allows [WorkRequestHandler] to capture standard
 * output and error streams that can't be directly captured by the PrintStream associated with
 * the work request.
 */ /* visible for test */(
  /**
   * Returns the original input stream most commonly provided by [System.in]
   */
  val originalInputStream: InputStream?,
  /**
   * Returns the original output stream most commonly provided by [System.out]
   */
  val originalOutputStream: PrintStream?,
  /**
   * Returns the original error stream most commonly provided by [System.err]
   */
  val originalErrorStream: PrintStream?,
  private val capturedStream: ByteArrayOutputStream,
  private val restore: AutoCloseable
) : AutoCloseable {
  /**
   * Returns the captured outputs as a UTF-8 string
   */
  fun readCapturedAsUtf8String(): String {
    capturedStream.flush()
    val captureOutput = capturedStream.toString(StandardCharsets.UTF_8)
    capturedStream.reset()
    return captureOutput
  }

  override fun close() {
    restore.close()
  }
}

/**
 * Wraps the standard System streams and WorkerIO instance
 */
fun wrapStandardSystemStreams(): WorkerIo {
  // Save the original streams
  val originalInputStream = System.`in`
  val originalOutputStream = System.out
  val originalErrorStream = System.err

  // replace the original streams with our own instances
  val capturedStream = ByteArrayOutputStream()
  val outputBuffer = PrintStream(capturedStream, true)
  val byteArrayInputStream = ByteArray(0).inputStream()
  System.setIn(byteArrayInputStream)
  System.setOut(outputBuffer)
  System.setErr(outputBuffer)

  return WorkerIo(
    originalInputStream = originalInputStream,
    originalOutputStream = originalOutputStream,
    originalErrorStream = originalErrorStream,
    capturedStream = capturedStream,
    restore = AutoCloseable {
      System.setIn(originalInputStream)
      System.setOut(originalOutputStream)
      System.setErr(originalErrorStream)
      outputBuffer.close()
      byteArrayInputStream.close()
    })
}