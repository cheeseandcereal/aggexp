# syntax=docker/dockerfile:1.7
#
# Build context: repo root (so we can COPY the shared proto/).
# Single-stage is fine for a lab — compactness > image-size purity.
FROM python:3.12-slim

WORKDIR /app

# System packages: grpcio-tools needs nothing extra on slim for the
# pinned wheels (manylinux); ship without build-essential.
COPY experiments/0019-krm-polyglot-backend/backend-note-py/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the shared proto and the backend source.
COPY experiments/0019-krm-polyglot-backend/proto/ /app/proto/
COPY experiments/0019-krm-polyglot-backend/backend-note-py/main.py /app/main.py
COPY experiments/0019-krm-polyglot-backend/backend-note-py/gen.sh /app/gen.sh

# Generate bindings at build time. The gen.sh script writes under
# aggexp/krm/v1/ next to main.py so `from aggexp.krm.v1 import
# backend_pb2` works.
RUN mkdir -p /app/aggexp/krm/v1 \
 && touch /app/aggexp/__init__.py /app/aggexp/krm/__init__.py /app/aggexp/krm/v1/__init__.py \
 && python -m grpc_tools.protoc \
      --proto_path=/app/proto \
      --python_out=/app/aggexp/krm/v1 \
      --grpc_python_out=/app/aggexp/krm/v1 \
      /app/proto/backend.proto \
 && sed -i 's/^import backend_pb2 as /from aggexp.krm.v1 import backend_pb2 as /' \
      /app/aggexp/krm/v1/backend_pb2_grpc.py

EXPOSE 9090
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/main.py", "--addr=0.0.0.0:9090"]
