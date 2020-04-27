fn main() {
    // build static x448 lib
    let dst = cmake::build("include/x448");
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=x448");

    // compile protobuf files
    tonic_build::compile_protos("protobuf/directory.proto").expect("Failed to generate gRPC");
}
