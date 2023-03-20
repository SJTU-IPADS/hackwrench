declare -a arr1=(
    # "tpcc-scalability"
    # "hw-batch-tpcc-tput-lat"
    # "hw-vs-occ-tpcc-tput-lat"
    # "ycsb10-cache-effect"
    # "tpcc-ratio"
    # "ycsb10-batch-effect-1"
    # "ycsb10-batch-effect-6"
    # "ycsb10-cache-effect-repair"
    # "ycsb10-contention"
    # "tpcc-abort-vs-repair"
    # "fdb-tpcc-tput-lat"
    # "hw-vs-tpcc-tput-hist"
)

declare -a arr2=(
    # "tpcc-scalability"
    # "tpcc-abort-vs-repair"
    # "hw-vs-tpcc-tput-hist"
    # "hw-vs-coco&calvin"
    # "hw-vs-coco-tput-hist"
    # "ycsb10-cache-effect"
    # "ycsb10-contention"
)

cd v1
for name in "${arr1[@]}"
do
    echo "plotting ${name}..."
    gnuplot ${name}.plt
    epstopdf out/${name}.eps
done
# mv ./out/*.pdf ./pdf

cd ../v2
for name in "${arr2[@]}"
do
    echo "plotting ${name}..."
    gnuplot ${name}.plt
    epstopdf ./out/${name}.eps
done
# mv ./out/*.pdf ./pdf


# "hw-vs-tpcc-tput-hist"
# "micro-contention-coco-0.1"
# "micro-contention-coco-0.99"
# "micro-contention-sundial-0.1"
# "micro-contention-sundial-0.99"
# "micro-batch-effect"
# "hw-vs-tpcc-tput-hist"
# "micro-contention-coco-0.1"
# "micro-contention-coco-0.99"
# "micro-contention-sundial-0.1"
# "micro-contention-sundial-0.99"
# "breakdown-calvin"
# "breakdown-coco"
# "breakdown-sundial"
# "breakdown-hw"
# "breakdown-hw-noreplica"
# "tpcc-scalability"
declare -a arr3=(
    "hw-vs-tpcc-tput-hist-fdb"
    # "hw-vs-tpcc-tput-hist-half"
    # "tpcc-abort-vs-repair"
    # "total-vs-partial"
    # "hw-vs-coco&calvin"
    # "hw-vs-sundial-tpcc"
    # "tpcc-scalability-noinvertkey"
    # "micro-contention-coco"
    # "micro-batch-effect-half"
    # "micro-contention-sundial"
    # "micro-cache-effect"
    # "network-message"
    # "breakdown-coco&calvin&hw"
    # "breakdown-sundial&hw-noreplica"
    # "ts-bottleneck"
)

cd ../v3
for name in "${arr3[@]}"
do
    echo "plotting ${name}..."
    gnuplot ${name}.plt
    epstopdf ./out/${name}.eps
done
mv ./out/*.pdf ./pdf
cd ..