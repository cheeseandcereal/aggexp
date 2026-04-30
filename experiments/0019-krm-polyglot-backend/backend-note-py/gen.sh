#!/usr/bin/env bash
# Regenerate python gRPC bindings from ../proto/backend.proto.
#
# Output lands under aggexp/krm/v1/ so `from aggexp.krm.v1 import
# backend_pb2` works. grpc_tools.protoc reads the proto's
# `package aggexp.krm.v1;` and emits the corresponding directory
# structure when --python_out=. is rooted at the package.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}"

PROTO_DIR="${HERE}/../proto"

mkdir -p aggexp/krm/v1
touch aggexp/__init__.py aggexp/krm/__init__.py aggexp/krm/v1/__init__.py

python -m grpc_tools.protoc \
  --proto_path="${PROTO_DIR}" \
  --python_out=aggexp/krm/v1 \
  --grpc_python_out=aggexp/krm/v1 \
  "${PROTO_DIR}/backend.proto"

# grpc_tools.protoc's generated files import each other as plain
# top-level modules ("import backend_pb2 as ..."), which only works
# when run from the output dir. Rewrite to absolute package imports
# so the generated modules can be imported from anywhere on sys.path.
if [ -f aggexp/krm/v1/backend_pb2_grpc.py ]; then
  sed -i 's/^import backend_pb2 as /from aggexp.krm.v1 import backend_pb2 as /' \
    aggexp/krm/v1/backend_pb2_grpc.py
fi

echo "generated: aggexp/krm/v1/backend_pb2.py aggexp/krm/v1/backend_pb2_grpc.py"
