load("@rules_java//java:defs.bzl", "JavaInfo", "java_common")

def _jps_lib_impl(ctx):
    files = [ctx.outputs.jar]
    output_jar = ctx.actions.declare_file(ctx.label.name + ".jar")
    ctx.actions.run(
        mnemonic = "JpsCompile",
        inputs = ctx.files.srcs,
        outputs = [output_jar],
        executable = ctx.executable._compiler,
    )
    return [
        DefaultInfo(
            files = depset(files),
        ),
        # todo deps, runtime_deps, exports
        JavaInfo(
            output_jar = output_jar,
            compile_jar = output_jar,
        ),
    ]

jps_lib = rule(
    doc = """This rule compiles and links Kotlin and Java sources into a .jar file using JPS.""",
    implementation = _jps_lib_impl,
    outputs = dict(
        jar = "%{name}.jar",
    ),
    attrs = {
        "module_name": attr.string(
            doc = """The name of the module, if not provided the module name is derived from the label. --e.g.,
      `//some/package/path:label_name` is translated to `some_package_path-label_name`.""",
        ),
        "srcs": attr.label_list(
            doc = """The list of source files that are processed to create the target, this can contain both Java and Kotlin files.
      Java analysis occurs first so Kotlin classes may depend on Java classes in the same compilation unit.""",
            default = [],
            allow_files = [".kt", ".java"],
            mandatory = True,
        ),
        "deps": attr.label_list(
            doc = """A list of dependencies of this rule. See general comments about `deps` at
      [Attributes common to all build rules](https://docs.bazel.build/versions/master/be/common-definitions.html#common-attributes).""",
            providers = [
                [JavaInfo],
            ],
            allow_files = False,
        ),
        # see https://bazel.build/extending/rules#private_attributes_and_implicit_dependencies about implicit dependencies
        "_compiler": attr.label(
            default = Label(":jps_builder"),
            executable = True,
            allow_files = True,
            cfg = "exec",
        ),
    },
    provides = [JavaInfo],
)
