package org.jetbrains.bazel.jvm

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse
import java.io.InputStream
import java.io.OutputStream

class ProtoWorkerMessageProcessor(
  private val stdin: InputStream,
  private val stdout: OutputStream
) : WorkerMessageProcessor {
  override fun readWorkRequest(): WorkRequest? = WorkRequest.parseDelimitedFrom(stdin)

  override fun writeWorkResponse(workResponse: WorkResponse) {
    try {
      workResponse.writeDelimitedTo(stdout)
    }
    finally {
      stdout.flush()
    }
  }

  override fun close() {
  }
}