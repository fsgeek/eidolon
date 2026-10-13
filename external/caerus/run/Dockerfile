# Measurement image: caerus:aaf0eb6 + throughput logging (-DTHROUPUT, code
# already present behind the macro) + iproute2 for tc netem. No other changes.
FROM caerus:aaf0eb6
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*
RUN sed -i 's/^CXXFLAGS := -O2 -DNDEBUG -DNDEBUG -MD/CXXFLAGS := -O2 -DNDEBUG -DNDEBUG -DTHROUPUT -MD/' src/Makefile \
 && grep -q DTHROUPUT src/Makefile \
 && bash -o pipefail -c 'cd src && make clean >/dev/null && make -j32 2>&1 | tail -2'
