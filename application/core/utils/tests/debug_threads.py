import time
import contextlib
import concurrent.futures
from application.core.utils.threads import (
    ExitThread
)


def main():
    start_time = time.perf_counter()
    t1 = ExitThread(time.sleep, (10,), debug=True)
    t1.start()
    time.sleep(1)
    t1.exit()
    with contextlib.suppress(concurrent.futures.CancelledError):
        print(t1.result_future.result())
    end_time = time.perf_counter()
    print(f"time cost {end_time - start_time:0.2f}")


if __name__ == '__main__':
    main()
