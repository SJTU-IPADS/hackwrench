load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1.4 linewidth 2 enhanced
set output "./out/ycsb10-batch-effect-1.eps"

set border linewidth 0.5

set key autotitle columnhead
set key top left Left reverse samplen 1

set xlabel "Throughput (txn/s)"
set ylabel "Latency (ms)"

set grid ytics
# set xrange [0:]
set yrange [0:80]
set xtics format "%.0s%c"

plot "./data/ycsb10-batch-effect-1.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 5  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 7  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 9  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 11 ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 15 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 13:($14/1000000) w lp ls 7 lw 1.5 pt 15 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 15:($16/1000000)  w lp ls 8 lw 1.5 pt 16 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 17:($18/1000000) w lp ls 9 lw 1.5 pt 17 ps 1.2