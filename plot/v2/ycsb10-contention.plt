load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1 linewidth 2 enhanced
set output "./out/ycsb10-contention.eps"

set border linewidth 0.5

bm = 0.23
tm = 0.96
lm = 0.1
rm = 0.99  # figure height = (0.96-0.23) * 1in = 0.73in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
set key top right Right samplen 1

set grid ytics
set ylabel "Throughput (txn/s)" offset 1
set yrange [0:]

set xlabel "Zipfian parameter {/Symbol q}" offset 0,0.4
set ytics format "%.0s%c"
set xtics 0.1
set xrange [0.05:1.05]

plot "./data/ycsb10-contention.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "./data/ycsb10-contention.data" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "./data/ycsb10-contention.data" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2

