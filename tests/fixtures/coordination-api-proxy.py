"""Owned SSH transport fault injector; all operations execute on the real test daemon."""

import json
import os
from pathlib import Path
import socket
import sys

root = Path(os.environ["TEST_ROOT"])
line = sys.stdin.buffer.readline()
request = json.loads(line)
mode = (root / "transport-mode").read_text() if (root / "transport-mode").exists() else ""
method = request["method"]
if mode == "prewrite-refusal" and method == "agent.coordination.list":
    print(json.dumps({"id": request["id"], "error": {"code": "unknown_method", "message": "owned old-server fixture"}}))
    sys.exit(0)
if mode == "hidden-outcome" and method == "agent.coordination.result":
    print(json.dumps({"id": request["id"], "result": {"type": "agent_coordination_result", "outcome": None}}))
    sys.exit(0)

stream = socket.socket(socket.AF_UNIX)
stream.settimeout(30)
stream.connect((root / "target-socket").read_text())
stream.sendall(line)
lost = mode in ("lost-reply", "hidden-outcome") and method == "agent.coordination.execute"
if lost:
    # The submitted request remains connected until the real target finishes, while
    # its caller loses the transport before the deferred PTY completion is available.
    if os.fork():
        os._exit(0)
    for descriptor in (0, 1, 2):
        os.close(descriptor)
    with stream.makefile("rb") as reply:
        reply.readline()
    os._exit(0)
with stream.makefile("rb") as reply:
    sys.stdout.buffer.write(reply.readline())
    sys.stdout.buffer.flush()
