package org.jetbrains.bazel.jvm

object JpsBazelBuilder {
  @JvmStatic
  fun main(args: Array<String>) {
    WorkRequestHandler({ workRequest, printWriter -> 0 })
      .processRequests()
  }
}