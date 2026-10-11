"""python provision.py [hook_url]   (hook_server.py already running)
default hook_url = http://host.docker.internal:8099/gate   <- Floci runs in a container; it cannot reach your host as 'localhost'."""
import sys

import grid

hook = sys.argv[1] if len(sys.argv) > 1 else "http://host.docker.internal:8099/gate"
print("topic endpoint:", grid.create_topic())
grid.create_subscription(hook)
print(f"subscription '{grid.SUB}' -> {hook}   (filter: {grid.INBOX_SUBJECT_PREFIX}, BlobCreated)")
