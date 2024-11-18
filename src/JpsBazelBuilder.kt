package org.jetbrains.bazel.jvm

object JpsBazelBuilder {
  @JvmStatic
  fun main(args: Array<String>) {
    WorkRequestHandler(
      executor = { workRequest, printWriter -> 0 },
      errorStream = System.err,
      messageProcessor = ProtoWorkerMessageProcessor(stdin = System.`in`, stdout = System.out)
    )
      .processRequests()
  }
}