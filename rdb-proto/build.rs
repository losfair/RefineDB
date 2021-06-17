fn main() {
  let proto_root = "src/protos";
  println!("cargo:rerun-if-changed={}/rdbrpc.proto", proto_root);
  protoc_grpcio::compile_grpc_protos(&["rdbrpc.proto"], &[proto_root], &proto_root, None)
    .expect("Failed to compile gRPC definitions!");
}
