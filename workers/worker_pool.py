from concurrent.futures import ThreadPoolExecutor

class WorkerThreadPool:
    def __init__(self, max_worker=None):
        # By default ThreadPoolExecutor will allocate the number of maximum available
        # threads for processing
        self.executor = ThreadPoolExecutor(max_workers=max_worker)

    def process_tasks(self, task_fn, *args, **kwargs):
        post_process_fn = None
        if 'post_process_fn' in kwargs:
            post_process_fn = kwargs.pop('post_process_fn')

        if task_fn and callable(task_fn):
            try:
                task = self.executor.submit(task_fn, *args, **kwargs)
                if post_process_fn and callable(post_process_fn):
                    task.add_done_callback(post_process_fn)
                return task
            except Exception as worker_exception:
                print('Exception while processing tasks in worker thread')
                return worker_exception
        else:
            raise Exception('first parameter to process_tasks must be a callable function')

DEFAULT_WORKER_THREAD = WorkerThreadPool()
