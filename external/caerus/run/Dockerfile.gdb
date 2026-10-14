# Debug tooling only: caerus-run:aaf0eb6 (pinned, as released + THROUPUT) plus gdb.
FROM caerus-run:aaf0eb6
RUN apt-get update && apt-get install -y --no-install-recommends gdb && rm -rf /var/lib/apt/lists/*
