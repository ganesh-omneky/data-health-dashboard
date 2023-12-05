from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, wait
from datetime import datetime
from typing import Iterable, List, Optional

from src.logging import get_logger

logger = get_logger(__name__)


class AsyncWorkUnit:
    def run(self):
        raise NotImplementedError

    def __len__(self):
        return -1


class CustomUnit(AsyncWorkUnit):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.func(*self.args, **self.kwargs)


class AsyncAPIManager:
    MAX_NUM_THREADS = 16

    def __init__(self, max_num_threads: int = MAX_NUM_THREADS):
        self.work_units: List[AsyncWorkUnit] = []
        self.max_nthreads = max_num_threads

    def add_work_unit(self, work_unit: AsyncWorkUnit):
        self.work_units.append(work_unit)

    def reset(self):
        self.work_units = []

    def run(
        self,
        work_queue: Optional[Iterable[AsyncWorkUnit]] = None,
        nthreads: Optional[int] = None,
    ) -> Iterable:
        if not work_queue:
            work_queue = self.work_units
        futures: List[Future] = []

        time_start = datetime.now()
        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            for work_unit in work_queue:
                future = executor.submit(work_unit.run)
                futures.append(future)

        # Wait for all the futures to complete
        n_futures = len(futures)

        completed = []
        error = []
        pending = []

        while len(completed) + len(error) < n_futures:
            completed = []
            error = []
            pending = []

            for future in futures:
                if future.done():
                    completed.append(future)
                elif future.running():
                    pending.append(future)
                else:
                    error.append(future)

            logger.info(
                f"Completed: {len(completed)}, Pending: {len(pending)}, Error: {len(error)}, Total: {n_futures},"
                f"Time elapsed: {datetime.now() - time_start}"
            )
            wait(pending, return_when="FIRST_COMPLETED")

        logger.info("All futures completed")
        logger.info(f"Completed: {len(completed)}, Error: {len(error)}")

        # Get the results
        results = []
        for future in futures:
            try:
                result = future.result()
            except CancelledError:
                logger.error("Future was unexpectedly cancelled")
                result = None
            except Exception as e:
                logger.error(f"Exception occurred: {e}", exc_info=True)
                raise e
            results.append(result)
        return results
