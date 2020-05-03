pub mod crypto;
pub mod directory;
pub mod error;

pub mod tonic_directory {
    tonic::include_proto!("directory");
}

