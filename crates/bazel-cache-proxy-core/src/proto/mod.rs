/// Semver types — defined manually since we use extern_path to tell prost
/// where to find them (avoiding super::super::super:: path issues).
pub mod semver {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct SemVer {
        #[prost(int32, tag = "1")]
        pub major: i32,
        #[prost(int32, tag = "2")]
        pub minor: i32,
        #[prost(int32, tag = "3")]
        pub patch: i32,
        #[prost(string, tag = "4")]
        pub prerelease: ::prost::alloc::string::String,
    }
}

pub mod reapi {
    tonic::include_proto!("build.bazel.remote.execution.v2");
}

pub mod bytestream {
    tonic::include_proto!("google.bytestream");
}
