# trace_capture.py
from collections import deque
from dataclasses import dataclass
import time

@dataclass
class TraceLine:
    ts_ms: int
    direction: str   # "-->" or "<--"
    text: str        # raw JSON line

class RpcTape:
    def __init__(self, maxlen: int = 2000):
        self._buf = deque(maxlen=maxlen)

    def add(self, direction: str, text: str):
        self._buf.append(TraceLine(
            ts_ms=int(time.time() * 1000),
            direction=direction,
            text=text.rstrip("\r\n"),
        ))

    def snapshot(self):
        return [t.__dict__ for t in list(self._buf)]

    def clear(self):
        self._buf.clear()