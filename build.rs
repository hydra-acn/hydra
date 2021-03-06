fn main() {
    // build static x448 lib
    let dst = cmake::build("include/x448");
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=x448");

    // build static threefish lib
    let dst = cmake::build("include/threefish");
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=threefish");

    // build static fakerand lib
    let dst = cmake::build("include/fakerand");
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=fakerand");

    // compile protobuf files
    tonic_build::compile_protos("protobuf/directory.proto")
        .expect("Failed to generate directory gRPC");
    tonic_build::compile_protos("protobuf/mix.proto").expect("Failed to generate mix gRPC");
    tonic_build::compile_protos("protobuf/rendezvous.proto")
        .expect("Failed to generate rendezvous gRPC");
}
