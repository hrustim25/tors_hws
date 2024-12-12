import threading

class Timer:
    def __init__(self):
        self.timer = threading.Timer(0.0, None)
        self.lock = threading.Lock()

    def restart_timer(self, timeout: float, callback, *args, **kwargs):
        with self.lock:
            self.timer.cancel()
            self.timer = threading.Timer(timeout, callback, args, kwargs)
            self.timer.start()

    def cancel_timer(self):
        self.timer.cancel()
