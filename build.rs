use std::fs::read_dir;

const PROTO_DIR: &str = "./src/protobuf";

fn main() {
    let proto_files: Vec<_> = read_dir(PROTO_DIR)
        .expect("not found")
        .filter_map(|file| {
            let file = file.ok()?;
            if file.file_name().to_str()?.ends_with(".proto") {
                Some(file.path())
            } else {
                None
            }
        })
        .collect();

    tonic_build::configure()
        .build_server(true)
        .out_dir(PROTO_DIR)
        .compile_protos(&proto_files, &["."])
        .unwrap_or_else(|err| panic!("protobuf compile error: {err}"));
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.to_str().unwrap());
    }
    println!("cargo:rerun-if-changed=build.rs");

    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=migrations");
}
