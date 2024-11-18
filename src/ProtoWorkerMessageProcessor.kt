package org.jetbrains.jps.bazel;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class ProtoWorkerMessageProcessor implements WorkRequestHandler.WorkerMessageProcessor {
  /**
   * This worker's stdin.
   */
  private final InputStream stdin;

  /**
   * This worker's stdout. Only {@link WorkRequest}s should be written here.
   */
  private final OutputStream stdout;

  /**
   * Constructs a {@link WorkRequestHandler} that reads and writes Protocol Buffers.
   */
  public ProtoWorkerMessageProcessor(InputStream stdin, OutputStream stdout) {
    this.stdin = stdin;
    this.stdout = stdout;
  }

  @Override
  public WorkRequest readWorkRequest() throws IOException {
    return WorkRequest.parseDelimitedFrom(stdin);
  }

  @Override
  public void writeWorkResponse(WorkResponse workResponse) throws IOException {
    try {
      workResponse.writeDelimitedTo(stdout);
    }
    finally {
      stdout.flush();
    }
  }

  @Override
  public void close() {
  }
}