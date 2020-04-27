fn main() {
    // TODO make this work
    // pkg_config::Config::new()
    //     .atleast_version("1.0")
    //     .probe("crypto")
    //     .unwrap();

    let x448_src = [
        "include/x448/arch_32/f_impl.c",
        "include/x448/curve448.c",
        "include/x448/curve448_tables.c",
        "include/x448/eddsa.c",
        "include/x448/f_generic.c",
        "include/x448/scalar.c",
    ];
    let mut x448_builder = cc::Build::new();
    let x448_build = x448_builder
        .files(x448_src.iter())
        .include("include/x448")
        .flag("-Wno-unused-parameter");
    x448_build.compile("x448");

    println!("cargo:rustc-link-lib=crypto");

    tonic_build::compile_protos("protobuf/directory.proto").expect("Failed to generate gRPC");
}
