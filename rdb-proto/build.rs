fn main() {
  tonic_build::compile_protos("src/proto/rdbrpc.proto").unwrap();
}
