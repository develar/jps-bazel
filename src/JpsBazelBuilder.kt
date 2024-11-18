package org.jetbrains.bazel.jvm

object JpsBazelBuilder {
  @JvmStatic
  fun main(args: Array<String>) {
    WorkRequestHandlerBuilder(
      executor = WorkRequestCallback { workRequest, printWriter -> 0 },
      errorStream = System.err,
      messageProcessor = ProtoWorkerMessageProcessor(stdin = System.`in`, stdout = System.out)
    )
      .build()
      .processRequests()
  }
}