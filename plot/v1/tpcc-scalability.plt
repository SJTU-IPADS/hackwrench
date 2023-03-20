load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1 linewidth 2 enhanced
set output "./out/tpcc-scalability.eps"

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
set key top left Left reverse samplen 1

set ylabel "Throughput (txn/s)" offset 1
set grid ytics
set ytics format "%.0s%c"
set ytics 150000
set yrange [0:970000]

set xlabel "Number of database nodes" offset 0,0.4
set xtics 1, 2
set xrange [0.5:15.5]

plot "./data/tpcc-scalability.data" u 1:($2) w lp ls 1 lw 1.5 pt 3 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 9 ps 1.2