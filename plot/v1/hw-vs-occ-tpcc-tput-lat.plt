load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 20" eps color dashed size 4.5,3
set output "./out/hw-vs-occ-tpcc-tput-lat.eps"

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

set xlabel "Hackwrench / OCC Throughput (txn/s)"
set ylabel "Latency (ms)"

# set yrange [0:25]
# set xrange [0:140000]
set xtics format "%.0s%c"
# set xtics 40000

plot "./data/tpcc-tput-lat.data" u 7:($8/1000000) w lp ls 1 lw 8 pt 1  ps 2,\
     "" u 9:($10/1000000) w lp ls 2 lw 8 pt 3  ps 2,\
     "" u 11:($12/1000000) w lp ls 3 lw 8 pt 5  ps 2,\
     "" u 13:($14/1000000) w lp ls 4 lw 8 pt 7  ps 2,\
     "" u 15:($16/1000000) w lp ls 5 lw 8 pt 9  ps 2,\
     "" u 17:($18/1000000) w lp ls 6 lw 8 pt 11  ps 2,\
