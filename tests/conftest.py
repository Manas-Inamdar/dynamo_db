import os

# Silence gRPC's C-core logging (e.g. GOAWAY notices when test nodes stop).
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
