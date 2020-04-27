fn main() {
    // TODO make this work
    // pkg_config::Config::new()
    //     .atleast_version("1.0")
    //     .probe("crypto")
    //     .unwrap();

    let x448_src = [
        "include/x448/src/arch_32/f_impl.c",
        "include/x448/src/curve448.c",
        "include/x448/src/curve448_tables.c",
        "include/x448/src/f_generic.c",
        "include/x448/src/scalar.c",
    ];
    let mut x448_builder = cc::Build::new();
    let x448_build = x448_builder
        .files(x448_src.iter())
        .include("include/x448/include")
        .include("include/x448/src")
        .flag("-Wno-unused-parameter");
    x448_build.compile("x448");

    tonic_build::compile_protos("protobuf/directory.proto").expect("Failed to generate gRPC");
}
