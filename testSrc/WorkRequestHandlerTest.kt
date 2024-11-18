// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
@file:Suppress("ReplaceGetOrSet")

package org.jetbrains.bazel.jvm

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Tests for the WorkRequestHandler
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class WorkRequestHandlerTest {
  companion object {
    private fun createTestWorkerIo(): WorkerIo {
      val captured = ByteArrayOutputStream()
      return WorkerIo(
        originalInputStream = System.`in`,
        originalOutputStream = System.out,
        originalErrorStream = System.err,
        capturedStream = captured,
        restore = captured
      )
    }
  }

  private val testWorkerIo = createTestWorkerIo()

  @AfterEach
  fun after() {
    testWorkerIo.close()
  }

  @Test
  fun normalWorkRequest() {
    val out = ByteArrayOutputStream()
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err -> 1 },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = ProtoWorkerMessageProcessor(stdin = ByteArrayInputStream(ByteArray(0)), stdout = out),
    )

    val args = listOf("--sources", "A.java")
    val request = WorkRequest.newBuilder().addAllArguments(args).build()
    handler.respondToRequest(workerIo = testWorkerIo, request = request, requestInfo = RequestInfo(Thread.currentThread()))

    val response = WorkResponse.parseDelimitedFrom(out.toByteArray().inputStream())
    assertThat(response.requestId).isEqualTo(0)
    assertThat(response.exitCode).isEqualTo(1)
    assertThat(response.getOutput()).isEmpty()
  }

  @Test
  fun multiplexWorkRequest() {
    val out = ByteArrayOutputStream()
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err -> 0 },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = ProtoWorkerMessageProcessor(ByteArray(0).inputStream(), out)
    )

    val args = listOf("--sources", "A.java")
    val request = WorkRequest.newBuilder().addAllArguments(args).setRequestId(42).build()
    handler.respondToRequest(workerIo = testWorkerIo, request = request, requestInfo = RequestInfo(Thread.currentThread()))

    val response = WorkResponse.parseDelimitedFrom(out.toByteArray().inputStream())
    assertThat(response.requestId).isEqualTo(42)
    assertThat(response.exitCode).isEqualTo(0)
    assertThat(response.getOutput()).isEmpty()
  }

  @Test
  fun multiplexWorkRequestStopsThreadsOnShutdown() {
    val src = PipedOutputStream()
    val dest = PipedInputStream()

    // Work request threads release this when they have started.
    val started = Semaphore(0)
    // Work request threads wait forever on this, so we can see how they react to closed stdin.
    val eternity = Semaphore(0)
    // Released when the work request handler thread has noticed the closed stdin and interrupted
    // the work request threads.
    val stopped = Semaphore(0)
    val workerThreads = ArrayList<Thread>()
    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest)
    ))
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err ->
        // Each call to this, runs in its own thread.
        try {
          synchronized(workerThreads) {
            workerThreads.add(Thread.currentThread())
          }
          started.release()
          eternity.acquire() // This blocks forever.
        }
        catch (e: InterruptedException) {
          throw AssertionError("Unhandled exception", e)
        }
        0
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor
    )

    val args = listOf("--sources", "A.java")
    val t = Thread({
                     try {
                       handler.processRequests()
                       stopped.release()
                     }
                     catch (e: IOException) {
                       throw AssertionError("Unhandled exception", e)
                     }
                   }, "Worker thread")
    t.start()
    val request1 = WorkRequest.newBuilder().addAllArguments(args).setRequestId(42).build()
    request1.writeDelimitedTo(src)
    val request2 = WorkRequest.newBuilder().addAllArguments(args).setRequestId(43).build()
    request2.writeDelimitedTo(src)
    src.flush()

    started.acquire(2)
    assertThat(workerThreads).hasSize(2)
    // now both request threads are started, closing the input to the "worker" should shut it down
    src.close()
    stopped.acquire()
    while (workerThreads.get(0).isAlive || workerThreads.get(1).isAlive) {
      Thread.sleep(1)
    }
    assertThat(workerThreads.get(0).isAlive).isFalse()
    assertThat(workerThreads.get(1).isAlive).isFalse()
  }

  @Test
  fun multiplexWorkRequestStopsWorkerOnException() {
    val src = PipedOutputStream()
    val dest = PipedInputStream()

    // work request threads release this when they have started
    val started = Semaphore(0)
    // one work request threads waits forever on this, so the second one can throw an exception
    val eternity = Semaphore(0)
    // released when the work request handler thread has been stopped after a worker thread died
    val stopped = Semaphore(0)
    val workerThreads = ArrayList<Thread>()
    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest)
    ))
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err ->
        // Each call to this, runs in its own thread.
        try {
          synchronized(workerThreads) {
            workerThreads.add(Thread.currentThread())
          }
          started.release()
          if (workerThreads.size < 2) {
            eternity.acquire() // This blocks forever.
          }
          else {
            throw Error("Intentional death!")
          }
        }
        catch (e: InterruptedException) {
          throw AssertionError("Unhandled exception", e)
        }
        0
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor
    )

    val args = listOf("--sources", "A.java")
    val t = Thread({
                     try {
                       handler.processRequests()
                       stopped.release()
                     }
                     catch (e: IOException) {
                       throw AssertionError("Unhandled exception", e)
                     }
                   }, "Worker thread")
    t.start()
    val request1 = WorkRequest.newBuilder().addAllArguments(args).setRequestId(42).build()
    request1.writeDelimitedTo(src)
    val request2 = WorkRequest.newBuilder().addAllArguments(args).setRequestId(43).build()
    request2.writeDelimitedTo(src)
    src.flush()

    started.acquire(2)
    assertThat<Thread?>(workerThreads).hasSize(2)
    stopped.acquire()
    while (workerThreads.get(0).isAlive || workerThreads.get(1).isAlive) {
      Thread.sleep(1)
    }
    assertThat(workerThreads.get(0).isAlive).isFalse()
    assertThat(workerThreads.get(1).isAlive).isFalse()
  }

  @Test
  fun testOutput() {
    val out = ByteArrayOutputStream()
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err ->
        err.appendLine("Failed!")
        1
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = ProtoWorkerMessageProcessor(ByteArray(0).inputStream(), out)
    )

    val args = listOf("--sources", "A.java")
    val request = WorkRequest.newBuilder().addAllArguments(args).build()
    handler.respondToRequest(testWorkerIo, request, RequestInfo(Thread.currentThread()))

    val response = WorkResponse.parseDelimitedFrom(out.toByteArray().inputStream())
    assertThat(response.requestId).isEqualTo(0)
    assertThat(response.exitCode).isEqualTo(1)
    assertThat(response.getOutput()).contains("Failed!")
  }

  @Test
  fun testException() {
    val out = ByteArrayOutputStream()
    val handler = WorkRequestHandler(
      executor = WorkRequestCallback { args, err ->
          throw RuntimeException("Exploded!")
        },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = ProtoWorkerMessageProcessor(ByteArray(0).inputStream(), out)
      )

    val args = listOf("--sources", "A.java")
    val request = WorkRequest.newBuilder().addAllArguments(args).build()
    handler.respondToRequest(workerIo = testWorkerIo, request = request, requestInfo = RequestInfo(Thread.currentThread()))

    val response = WorkResponse.parseDelimitedFrom(ByteArrayInputStream(out.toByteArray()))
    assertThat(response.requestId).isEqualTo(0)
    assertThat(response.exitCode).isEqualTo(1)
    assertThat(response.getOutput()).startsWith("java.lang.RuntimeException: Exploded!")
  }

  @Test
  fun cancelRequestExactlyOneResponseSent() {
    val handlerCalled = booleanArrayOf(false)
    val cancelCalled = booleanArrayOf(false)
    val src = PipedOutputStream()
    val dest = PipedInputStream()
    val done = Semaphore(0)
    val finish = Semaphore(0)
    val failures = ArrayList<String>()

    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest)
    ))
    val handler = WorkRequestHandlerBuilder(
      executor = WorkRequestCallback { args, err ->
          handlerCalled[0] = true
          err.appendLine("Such work! Much progress! Wow!")
          1
        },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor
      )
        .setCancelCallback { i, t -> cancelCalled[0] = true }
        .build()

    runRequestHandlerThread(done = done, handler = handler, finish = finish, failures = failures)
    WorkRequest.newBuilder().setRequestId(42).build().writeDelimitedTo(src)
    WorkRequest.newBuilder().setRequestId(42).setCancel(true).build().writeDelimitedTo(src)
    val response = WorkResponse.parseDelimitedFrom(dest)
    messageProcessor.stop()
    done.acquire()

    assertThat(handlerCalled[0] || cancelCalled[0]).isTrue()
    assertThat(response.requestId).isEqualTo(42)
    if (response.wasCancelled) {
      assertThat(response.getOutput()).isEmpty()
      assertThat(response.exitCode).isEqualTo(0)
    }
    else {
      assertThat(response.getOutput()).startsWith("Such work! Much progress! Wow!")
      assertThat(response.exitCode).isEqualTo(1)
    }

    // checks that nothing more was sent
    assertThat(dest.available()).isEqualTo(0)
    finish.release()

    // Checks that there weren't other unexpected failures.
    assertThat(failures).isEmpty()
  }

  @Test
  fun cancelRequestSendsResponseWhenDone() {
    val waitForCancel = Semaphore(0)
    val handlerCalled = Semaphore(0)
    val cancelCalled = Semaphore(0)
    val src = PipedOutputStream()
    val dest = PipedInputStream()
    val done = Semaphore(0)
    val requestDone = Semaphore(0)
    val finish = Semaphore(0)
    val failures = ArrayList<String>()

    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest)
    ))
    // We force the regular handling to not finish until after we have read the cancel response,
    // to avoid flakiness.
    val handler = WorkRequestHandlerBuilder(
      executor = WorkRequestCallback { args, err ->
        // this handler waits until the main thread has sent a cancel request
        handlerCalled.release(2)
        try {
          waitForCancel.acquire()
        }
        catch (e: InterruptedException) {
          failures.add("Unexpected interrupt waiting for cancel request")
          e.printStackTrace()
        }
        requestDone.release()
        0
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor,
    )
      .setCancelCallback { i, t -> cancelCalled.release() }
      .build()

    runRequestHandlerThread(done = done, handler = handler, finish = finish, failures = failures)
    WorkRequest.newBuilder().setRequestId(42).build().writeDelimitedTo(src)
    // make sure the handler is called before sending the cancel request, or we might process the cancellation entirely before that
    handlerCalled.acquire()
    WorkRequest.newBuilder().setRequestId(42).setCancel(true).build().writeDelimitedTo(src)
    cancelCalled.acquire()
    waitForCancel.release()
    // Give the other request a chance to process, so we can check that no other response is sent
    requestDone.acquire()
    messageProcessor.stop()
    done.acquire()

    val response = WorkResponse.parseDelimitedFrom(dest)
    assertThat(handlerCalled.availablePermits()).isEqualTo(1) // Released 2, one was acquired
    assertThat(cancelCalled.availablePermits()).isEqualTo(0)
    assertThat(response.requestId).isEqualTo(42)
    assertThat(response.getOutput()).isEmpty()
    assertThat(response.wasCancelled).isTrue()

    // Checks that nothing more was sent.
    assertThat(dest.available()).isEqualTo(0)
    src.close()
    finish.release()

    // checks that there weren't other unexpected failures
    assertThat(failures).isEmpty()
  }

  @Test
  fun cancelRequestNoDoubleCancelResponse() {
    val waitForCancel = Semaphore(0)
    val cancelCalled = Semaphore(0)
    val src = PipedOutputStream()
    val dest = PipedInputStream()
    val done = Semaphore(0)
    val requestsDone = Semaphore(0)
    val finish = Semaphore(0)
    val failures = ArrayList<String>()

    // we force the regular handling to not finish until after we have read the cancel response, to avoid flakiness.
    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest),
    ))
    val handler = WorkRequestHandlerBuilder(
      executor = WorkRequestCallback { args, err ->
        try {
          waitForCancel.acquire()
        }
        catch (e: InterruptedException) {
          failures.add("Unexpected interrupt waiting for cancel request")
          e.printStackTrace()
        }
        requestsDone.release()
        0
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor
    )
      .setCancelCallback { i, t -> cancelCalled.release() }
      .build()

    runRequestHandlerThread(done = done, handler = handler, finish = finish, failures = failures)
    WorkRequest.newBuilder().setRequestId(42).build().writeDelimitedTo(src)
    WorkRequest.newBuilder().setRequestId(42).setCancel(true).build().writeDelimitedTo(src)
    WorkRequest.newBuilder().setRequestId(42).setCancel(true).build().writeDelimitedTo(src)
    cancelCalled.acquire()
    waitForCancel.release()
    requestsDone.acquire()
    messageProcessor.stop()
    done.acquire()

    val response = WorkResponse.parseDelimitedFrom(dest)
    assertThat(cancelCalled.availablePermits()).isLessThan(2)
    assertThat(response.requestId).isEqualTo(42)
    assertThat(response.getOutput()).isEmpty()
    assertThat(response.wasCancelled).isTrue()

    // Checks that nothing more was sent.
    assertThat(dest.available()).isEqualTo(0)
    src.close()
    finish.release()

    // Checks that there weren't other unexpected failures.
    assertThat(failures).isEmpty()
  }

  @Test
  fun cancelRequestSendsNoResponseWhenAlreadySent() {
    val handlerCalled = Semaphore(0)
    val src = PipedOutputStream()
    val dest = PipedInputStream()
    val done = Semaphore(0)
    val finish = Semaphore(0)
    val failures = ArrayList<String>()

    // We force the cancel request to not happen until after we have read the normal response,
    // to avoid flakiness.
    val messageProcessor = StoppableWorkerMessageProcessor(ProtoWorkerMessageProcessor(
      stdin = PipedInputStream(src),
      stdout = PipedOutputStream(dest),
    ))
    val handler = WorkRequestHandlerBuilder(
      executor = WorkRequestCallback { args, err ->
        handlerCalled.release()
        err.appendLine("Such work! Much progress! Wow!")
        2
      },
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = messageProcessor
    )
      .setCancelCallback { i, t -> }
      .build()

    runRequestHandlerThread(done = done, handler = handler, finish = finish, failures = failures)
    WorkRequest.newBuilder().setRequestId(42).build().writeDelimitedTo(src)
    val response = WorkResponse.parseDelimitedFrom(dest)
    WorkRequest.newBuilder().setRequestId(42).setCancel(true).build().writeDelimitedTo(src)
    messageProcessor.stop()
    done.acquire()

    assertThat(response).isNotNull()

    assertThat(handlerCalled.availablePermits()).isEqualTo(1)
    assertThat(response.requestId).isEqualTo(42)
    assertThat(response.wasCancelled).isFalse()
    assertThat(response.exitCode).isEqualTo(2)
    assertThat(response.getOutput()).startsWith("Such work! Much progress! Wow!")

    // checks that nothing more was sent
    assertThat(dest.available()).isEqualTo(0)
    src.close()
    finish.release()

    // Checks that there weren't other unexpected failures.
    assertThat(failures).isEmpty()
  }

  @Test
  fun workRequestHandlerWithWorkRequestCallback() {
    val out = ByteArrayOutputStream()
    val callback = WorkRequestCallback { request, err -> request.argumentsCount }
    val handler = WorkRequestHandlerBuilder(
      executor = callback,
      errorStream = PrintStream(ByteArrayOutputStream()),
      messageProcessor = ProtoWorkerMessageProcessor(ByteArrayInputStream(ByteArray(0)), out)
    )
      .build()

    val args = listOf("--sources", "B.java")
    val request = WorkRequest.newBuilder().addAllArguments(args).build()
    handler.respondToRequest(workerIo = testWorkerIo, request = request, requestInfo = RequestInfo(Thread.currentThread()))

    val response = WorkResponse.parseDelimitedFrom(ByteArrayInputStream(out.toByteArray()))
    assertThat(response.requestId).isEqualTo(0)
    assertThat(response.exitCode).isEqualTo(2)
    assertThat(response.getOutput()).isEmpty()
  }

  private fun runRequestHandlerThread(
    done: Semaphore,
    handler: WorkRequestHandler,
    finish: Semaphore,
    failures: MutableList<String>
  ) {
    // This thread just makes sure the WorkRequestHandler does work asynchronously.
    Thread({
             try {
               handler.processRequests()
               while (!handler.activeRequests.isEmpty()) {
                 Thread.sleep(1)
               }
             }
             catch (e: IOException) {
               failures.add("Unexpected I/O error talking to worker thread")
               e.printStackTrace()
             }
             catch (_: InterruptedException) {
               // Getting interrupted while waiting for requests to finish is OK.
             }
             try {
               done.release()
               finish.acquire()
             }
             catch (_: InterruptedException) {
               // Getting interrupted at the end is OK.
             }
           }, "worker thread")
      .start()
  }

  @Test
  fun workerIODoesWrapSystemStreams() {
    // Save the original streams
    val originalInputStream = System.`in`
    val originalOutputStream = System.out
    val originalErrorStream = System.err

    // Swap in the test streams to assert against
    val byteArrayInputStream = ByteArray(0).inputStream()
    System.setIn(byteArrayInputStream)
    val outputBuffer = PrintStream(ByteArrayOutputStream(), true)
    System.setOut(outputBuffer)
    System.setErr(outputBuffer)

    try {
      outputBuffer.use {
        byteArrayInputStream.use {
          wrapStandardSystemStreams().use { io ->
            // Assert that the WorkerIO returns the correct wrapped streams and the new System instance
            // has been swapped out with the wrapped one
            assertThat(io.originalInputStream).isSameAs(byteArrayInputStream)
            assertThat(System.`in`).isNotSameAs(byteArrayInputStream)

            assertThat(io.originalOutputStream).isSameAs(outputBuffer)
            assertThat(System.out).isNotSameAs(outputBuffer)

            assertThat(io.originalErrorStream).isSameAs(outputBuffer)
            assertThat(System.err).isNotSameAs(outputBuffer)
          }
        }
      }
    }
    finally {
      // swap back in the original streams
      System.setIn(originalInputStream)
      System.setOut(originalOutputStream)
      System.setErr(originalErrorStream)
    }
  }

  @Test
  fun workerIODoesCaptureStandardOutAndErrorStreams() {
    wrapStandardSystemStreams().use { io ->
      // Assert that nothing has been captured in the new instance
      assertThat(io.readCapturedAsUtf8String()).isEmpty()

      // Assert that the standard out/error stream redirect to our own streams
      print("This is a standard out message!")
      System.err.print("This is a standard error message!")
      assertThat(io.readCapturedAsUtf8String()).isEqualTo("This is a standard out message!This is a standard error message!")

      // Assert that readCapturedAsUtf8String calls reset on the captured stream after a read
      assertThat(io.readCapturedAsUtf8String()).isEmpty()

      print("out 1")
      System.err.print("err 1")
      print("out 2")
      System.err.print("err 2")
      assertThat(io.readCapturedAsUtf8String()).isEqualTo("out 1err 1out 2err 2")
      assertThat(io.readCapturedAsUtf8String()).isEmpty()
    }
  }
}

/**
 * A wrapper around a WorkerMessageProcessor that can be stopped by calling `#stop()`.
 */
private class StoppableWorkerMessageProcessor(private val delegate: WorkerMessageProcessor) : WorkerMessageProcessor {
  private val stop = AtomicBoolean(false)
  private var readerThread: Thread? = null

  override fun readWorkRequest(): WorkRequest? {
    readerThread = Thread.currentThread()
    if (stop.get()) {
      return null
    }

    try {
      return delegate.readWorkRequest()
    }
    catch (e: InterruptedIOException) {
      // being interrupted is only an error if we didn't ask for it
      if (stop.get()) {
        return null
      }
      else {
        throw e
      }
    }
  }

  override fun writeWorkResponse(workResponse: WorkResponse) {
    delegate.writeWorkResponse(workResponse)
  }

  override fun close() {
    delegate.close()
  }

  fun stop() {
    stop.set(true)
    readerThread?.interrupt()
  }
}