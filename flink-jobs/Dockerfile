FROM flink:1.18-scala_2.12

# Install Python and PyFlink. On Linux, prebuilt wheels exist for grpcio-tools,
# so this avoids the source-build failure seen when installing apache-flink
# directly on Windows.
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*