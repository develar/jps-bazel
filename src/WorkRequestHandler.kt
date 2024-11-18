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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.io.PrintWriter
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest
import java.io.IOException
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.StringWriter
import java.io.Writer
import java.lang.AutoCloseable
import java.lang.Exception
import java.lang.RuntimeException
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import kotlin.system.exitProcess

/**
 * A helper class that handles [WorkRequests](https://bazel.build/docs/persistent-workers), including
 * [multiplex workers](https://bazel.build/docs/multiplex-worker).
 */
class WorkRequestHandler internal constructor(
  /**
   * The function to be called after each [WorkRequest] is read.
   */
  private val executor: (WorkRequest, Writer) -> Int,
  /**
   * This worker's stderr.
   */
  private val errorStream: PrintStream,
  private val messageProcessor: WorkerMessageProcessor,
  private val cancelHandler: ((Int) -> Unit)? = null,
) : AutoCloseable {
  /**
   * Requests that are currently being processed. Visible for testing.
   */
  // visible for test
  internal val activeRequests: ConcurrentHashMap<Int, RequestInfo> = ConcurrentHashMap<Int, RequestInfo>()

  private val threadPool = Executors.newCachedThreadPool()

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
      while (!threadPool.isShutdown) {
        val request = messageProcessor.readWorkRequest() ?: break
        if (request.cancel) {
          handleCancelRequest(request)
        }
        else {
          scheduleHandlingRequest(workerIO = workerIo, request = request)
        }
      }
    }
    catch (e: Throwable) {
      errorStream.println("error reading next request: $e")
      e.printStackTrace(errorStream)
    }
    finally {
      // TODO(b/220878242): Give the outstanding requests a chance to send a "shutdown" response,
      // but also try to kill stuck threads. For now, we just interrupt the remaining threads.
      // We considered doing System.exit here, but that is hard to test and would deny the callers
      // of this method a chance to clean up. Instead, we initiate the cleanup of our resources here
      // and the caller can decide whether to wait for an orderly shutdown or now.
      for (activeRequest in activeRequests.values) {
        activeRequest.setCancelled()
      }
      threadPool.shutdownNow()

      try {
        // Unwrap the system streams placing the original streams back
        workerIo.close()
      }
      catch (e: Exception) {
        errorStream.println(e.message)
      }
    }
  }

  /**
   * Starts a thread for the given request.
   */
  private fun scheduleHandlingRequest(workerIO: WorkerIo, request: WorkRequest) {
    if (request.requestId == 0) {
      while (activeRequests.containsKey(0)) {
        // Previous singleplex requests can still be in activeRequests for a bit after the response has been sent.
        // We need to wait for them to vanish.
        try {
          Thread.sleep(1)
        }
        catch (_: InterruptedException) {
          // reset interrupted status
          Thread.interrupted()
          return
        }
      }
    }

    val requestInfo = RequestInfo()
    val previous = activeRequests.putIfAbsent(request.requestId, requestInfo)
    require(previous == null) { "Request still active: ${request.requestId}" }
    threadPool.execute {
      if (requestInfo.isCancelled() || activeRequests.get(request.requestId) !== requestInfo) {
        return@execute
      }

      try {
        handleRequest(workerIo = workerIO, request = request, requestInfo = requestInfo)
      }
      catch (e: RuntimeException) {
        e.printStackTrace(errorStream)
      }
      catch (_: InterruptedException) {
        requestInfo.setCancelled()
        // reset interrupted status
        Thread.interrupted()
      }
      catch (e: Throwable) {
        // shutdown the worker in case of severe issues,
        // we don't handle RuntimeException here, as those are not serious enough to merit shutting down the worker.
        if (!threadPool.isShutdown) {
          errorStream.println("error thrown by worker thread, shutting down worker")
          e.printStackTrace(errorStream)
          requestInfo.setCancelled()
          threadPool.shutdownNow()
        }
      }
      finally {
        activeRequests.remove(request.requestId)
      }
    }
  }

  /**
   * Handles and responds to the given [WorkRequest].
   *
   * @throws IOException if there is an error talking to the server. Errors from calling the [][.callback] are reported with exit code 1.
   */
  // visible for tests
  internal fun handleRequest(workerIo: WorkerIo, request: WorkRequest, requestInfo: RequestInfo) {
    var exitCode = 1
    val stringWriter = StringWriter()
    stringWriter.use { writer ->
      try {
        exitCode = executor(request, writer)
      }
      catch (_: InterruptedException) {
        requestInfo.setCancelled()
      }
      catch (e: Error) {
        throw e
      }
      catch (e: Throwable) {
        PrintWriter(writer).use { e.printStackTrace(it) }
      }

      try {
        // read out the captured string for the final WorkResponse output
        val captured = workerIo.readCapturedAsUtf8String().trim()
        if (!captured.isEmpty()) {
          writer.write(captured)
        }
      }
      catch (e: Throwable) {
        errorStream.println(e.message)
      }
    }

    val responseBuilder = requestInfo.takeBuilder() ?: return
    responseBuilder.setRequestId(request.requestId)
    if (requestInfo.isCancelled()) {
      responseBuilder.setWasCancelled(true)
    }
    else {
      responseBuilder.setOutput(responseBuilder.getOutput() + stringWriter).setExitCode(exitCode)
    }
    val response = responseBuilder.build()
    synchronized(this) {
      messageProcessor.writeWorkResponse(response)
    }
  }

  /**
   * Marks the given request as canceled and uses [cancelHandler] to request cancellation.
   *
   * For simplicity, and to avoid blocking in [cancelHandler], response to cancellation
   * is still handled by [handleRequest] once the canceled request aborts (or finishes).
   */
  private fun handleCancelRequest(request: WorkRequest) {
    // Theoretically, we could have gotten two singleplex requests, and we can't tell those apart.
    // However, that's a violation of the protocol, so we don't try to handle it (not least because handling it would be quite error-prone).
    val requestToCancel = activeRequests.get(request.requestId) ?: return
    if (cancelHandler == null) {
      requestToCancel.setCancelled()
      // This is either an error on the server side or a version mismatch between the server setup and the binary.
      // It's better to wait for the regular work to finish instead of breaking the build, but we should inform the user about the bad setup.
      requestToCancel.addOutput("Cancellation request received for worker request ${request.requestId}, " +
                                  "but this worker does not support cancellation.\n")
    }
    else if (requestToCancel.cancelIfAlive()) {
      // Response will be sent from request thread once request handler returns.
      // We can ignore any exceptions in cancel callback since it's best effort.
      threadPool.execute {
        cancelHandler(request.requestId)
      }
    }
  }

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
internal class RequestInfo {
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

  fun cancelIfAlive(): Boolean = cancelled.compareAndSet(false, true)

  /**
   * Returns true if this request has been canceled.
   */
  fun isCancelled(): Boolean = cancelled.get()

  @JvmField var thread: Thread? = null

  fun cancel() {

  }

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
internal class WorkerIo
/**
 * Creates a new [WorkerIo] that allows [WorkRequestHandler] to capture standard
 * output and error streams that can't be directly captured by the PrintStream associated with
 * the work request.
 */ /* visible for test */(
  /**
   * Returns the original input stream most commonly provided by [System.in]
   */
  @JvmField val originalInputStream: InputStream?,
  /**
   * Returns the original output stream most commonly provided by [System.out]
   */
  @JvmField val originalOutputStream: PrintStream?,
  /**
   * Returns the original error stream most commonly provided by [System.err]
   */
  @JvmField val originalErrorStream: PrintStream?,
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
internal fun wrapStandardSystemStreams(): WorkerIo {
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