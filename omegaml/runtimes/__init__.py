from joblib import register_parallel_backend

from .backends.loky import OmegaRuntimeBackend
from .daskruntime import OmegaRuntimeDask
from .jobproxy import OmegaJobProxy
from .modelproxy import OmegaModelProxy
from .runtime import OmegaRuntime

register_parallel_backend('omegaml', OmegaRuntimeBackend)
