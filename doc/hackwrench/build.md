# How to Build Hackwrench

We build and run Hackwrench with AWS EC2 instances. 
For simplicity, we have prepared a public AWS image with all environments and code prepared.
The image is available at `xxx`.

## Step 1: Install the Environments

### Install [cap'n proto](https://capnproto.org/)
```bash
curl -O https://capnproto.org/capnproto-c++-0.9.1.tar.gz
tar zxf capnproto-c++-0.9.1.tar.gz
cd capnproto-c++-0.9.1
./configure
make -j6 check
sudo make install
```

### Install [cppzmq](https://github.com/zeromq/cppzmq)
```bash
# install zeromq
git clone https://github.com/zeromq/libzmq.git
cd libzmq
git reset --hard 81a8211e
mkdir build && cd build
cmake ..
sudo make -j4 install

# install cppzmq
git clone https://github.com/zeromq/cppzmq.git
cd cppzmq
git checkout v4.7.0
mkdir build && cd build
cmake .. -DCPPZMQ_BUILD_TESTS=OFF
sudo make -j4 install
```

### Install [jemalloc](https://github.com/jemalloc)
```
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
./autogen.sh
make dist
make -j4
sudo make install
```

## Step 2: Compile Hackwrench

In the `root` directory, run the following commands. Then the binaries of Hackwrench are compiled.

```bash
./scripts/gen_capnp_files.sh
mkdir build && cd build
cmake ..
make -j4
```

<!-- ## Step 3: Recognize the Binaries

After then, we have successfully built Hackwrench binaries. The descriptions of binaries are listed as following:

| Binary | description  |
|--------|----------|
| `hackwrench` | Hackwrench.  |
| `hackwrench_lat` | Hackwrench with detailed latency report for each type of transaction.  |
| `hackwrench_occ` | The OCC system which corresponds to the baseline. |
| `hackwrench_batch_abort` | Hackwrench without timestamp server, repair, and fast path optimization.  |
| `hackwrench_ts` | Hackwrench without repair and fast path optimization.  |
| `hackwrench_repair` | Hackwrench without fast path optimization.  |


Note: the binary information is not complete and will be completed soon. -->