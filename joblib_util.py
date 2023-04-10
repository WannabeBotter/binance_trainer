import joblib
import contextlib
from tqdm.auto import tqdm

# joblibの並列処理のプログレスバーを表示するためのユーティリティ関数
# https://blog.ysk.im/x/joblib-with-progress-bar
@contextlib.contextmanager
def tqdm_joblib(total: int = None, **kwargs):

    pbar = tqdm(total = total, miniters = 1, smoothing = 0, **kwargs)

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            pbar.update(n = self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback

    try:
        yield pbar
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        pbar.close()
