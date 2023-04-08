mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_LIBRARIES=0 -DENABLE_EMBEDDED_COMPILER=0 ..
ninja -j`nproc` clickhouse-server clickhouse-client
