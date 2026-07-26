use std::io::Result;

fn main() -> Result<()> {
    let proto_files = ["proto/raw_record.proto", "proto/wal_event.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    prost_build::compile_protos(&proto_files, &["proto"])?;

    println!("cargo:rerun-if-changed=proto/agent_lifecycle.proto");
    tonic_build::compile_protos("proto/agent_lifecycle.proto")?;

    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}
