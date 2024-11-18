package org.jetbrains.bazel.jvm

object JpsBazelBuilder {
  @JvmStatic
  fun main(args: Array<String>) {
    WorkRequestHandlerBuilder(
      callback = WorkRequestCallback { workRequest, printWriter -> 0 },
      stderr = System.err,
      messageProcessor = ProtoWorkerMessageProcessor(stdin = System.`in`, stdout = System.out)
    )
      .build()
      .processRequests()
  }
}