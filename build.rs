// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        .out_dir("./src/protobuf")
        .compile(&proto_files, &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.to_str().unwrap());
    }
    println!("cargo:rerun-if-changed=build.rs");
}
