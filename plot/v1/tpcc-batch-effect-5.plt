load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 14" eps color dashed size 3,1.7
set output "./out/tpcc-batch-effect-5.eps"

set key autotitle columnhead
set key tmargin center horizontal Left reverse samplen 2

set style fill pattern border 1

set xlabel "Throughput (txn/s)"
set ylabel "Latency (ms)"

# set xrange [0:160000]
# set yrange [0:12]
set xtics format "%.0s%c"

plot "./data/tpcc-batch-effect-5.data" u 1:($2/1000000)   w lp ls 1 lw 3 pt 5  ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 3:($4/1000000)   w lp ls 2 lw 3 pt 7  ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 5:($6/1000000)   w lp ls 3 lw 3 pt 9  ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 7:($8/1000000)   w lp ls 4 lw 3 pt 11 ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 9:($10/1000000)  w lp ls 5 lw 3 pt 13 ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 11:($12/1000000) w lp ls 6 lw 3 pt 14 ps 1.2,\
     "./data/tpcc-batch-effect-5.data" u 13:($14/1000000) w lp ls 7 lw 3 pt 15 ps 1.2