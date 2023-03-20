load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 14" eps color dashed size 3,1.7
set output "./out/tpcc-ratio.eps"

set key autotitle columnhead
set key tmargin center horizontal Left reverse samplen 2

set style fill pattern border 1

set xlabel "#DB"
set ylabel "Throughput (txn/s)"

# set yrange [0:320000]
set ytics format "%.0s%c"
# set xrange [1:16]

plot "./data/tpcc-ratio.data" u 1:($2) w lp ls 2 lw 3 pt 5 ps 1.1,\
     "./data/tpcc-ratio.data" u 1:($3) w lp ls 1 lw 3 pt 7 ps 1.2,\
     "./data/tpcc-ratio.data" u 1:($4) w lp ls 3 lw 3 pt 13 ps 1.4