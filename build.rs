fn main() {
    println!("cargo:rerun-if-changed=proto/event.proto");
    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(&["."]);

    tonic_build::configure()
        .compile_with_config(
            prost_build,
            &["proto/event.proto", "proto/vector.proto"],
            &["proto/"],
        )
        .unwrap();

    built::write_built_file().expect("Failed to acquire build-time information");
}
