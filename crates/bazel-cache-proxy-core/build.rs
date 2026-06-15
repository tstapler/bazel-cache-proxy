fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Tell prost where to find the semver package in our module tree.
        // The generated code for remote_execution.proto references semver via
        // super::super::super::semver (3 levels up from build.bazel.remote.execution.v2).
        // We expose semver at crate::proto::semver so we map the prost package path there.
        .extern_path(".build.bazel.semver", "crate::proto::semver")
        .compile_protos(
            &[
                "proto/build/bazel/remote/execution/v2/remote_execution.proto",
                "proto/google/bytestream/bytestream.proto",
                "proto/build/bazel/semver/semver.proto",
            ],
            &[proto_dir],
        )?;

    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
