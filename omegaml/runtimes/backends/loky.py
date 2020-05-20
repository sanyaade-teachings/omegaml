from joblib.parallel import BACKENDS
from tqdm import tqdm

LokyBackend = BACKENDS['loky']


class OmegaRuntimeBackend(LokyBackend):
    """
    omega custom parallel backend to print progress

    TODO: extend for celery dispatching
    """

    def __init__(self, *args, **kwargs):
        self._progress = ConsoleBackendProgress(self)
        self._job_count = kwargs.pop('n_tasks', None)
        super().__init__(*args, **kwargs)

    def _orig_print_progress(self, *args):
        pass

    def start_call(self):
        self._progress = tqdm(total=self._job_count, unit='tasks')
        self._orig_print_progress = self.parallel.print_progress
        self.parallel.print_progress = self.update_progress

    def update_progress(self):
        self._progress.update(1)

    def stop_call(self):
        self._progress.close()

    def terminate(self):
        self._progress.close()
        super().terminate()


class ConsoleBackendProgress:
    # model tqdm using default backend progress printing
    def __init__(self, backend):
        self.backend = backend

    def update(self, *args):
        self.backend._orig_print_progress()

    def close(self):
        self.backend._orig_print_progress()



