load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 20" eps color dashed size 4.5,3
set output "./out/fdb-tpcc-tput-lat.eps"

set multiplot layout 1, 2

set key autotitle columnhead
set key tmargin center horizontal Left reverse samplen 2

set style fill pattern border 1

################################################################################
# plot 1
################################################################################

set tmargin at screen 0.90
set bmargin at screen 0.15
set lmargin at screen 0.13
set rmargin at screen 0.96

set xlabel "FDB Throughput (txn/s)"
set ylabel "Latency (ms)"

# set yrange [0:25]
# set xrange [0:140000]
# set xtics 40000

plot "./data/tpcc-tput-lat.data" u 19:20 w lp ls 1 lw 8 pt 1  ps 2,\
"" u 21:22 w lp ls 2 lw 8 pt 3  ps 2,\
"" u 23:24 w lp ls 3 lw 8 pt 5  ps 2