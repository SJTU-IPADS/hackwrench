load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1.7 linewidth 2 enhanced
set output "./out/ycsb10-batch-effect-1+6.eps"

set border linewidth 0.5

# to let labels have background but no border
set style textbox opaque noborder

# layout <rows>, <cols>
set multiplot layout 2, 1

bm = 0.13
tm = 0.99
lm = 0.1
rm = 0.99
gap = 0.04  # each subplot's height = 0.41 * 1.7in = 0.697in

# sc=subcaption
sc_xoff = 0.03
sc_yoff = -0.06

###############
# Upper figure
###############

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm + (tm-bm-gap)/2 + gap
set tmargin at screen tm

set label "(a) Zipfian parameter {/Symbol q} = 0.1" at screen lm+sc_xoff, screen tm+sc_yoff left font "Times New Roman Bold" front boxed

set key autotitle columnhead
set key center left Left reverse samplen 1 maxrows 3

set grid ytics
set ytics autofreq
set yrange [0:73]

set xtics ("" 50000, "" 100000, "" 150000, "" 200000,"" 250000, "" 300000)
set xrange [50000:315000]

plot "./data/ycsb10-batch-effect-1.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 5  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 7  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 9  ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 11 ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "./data/ycsb10-batch-effect-1.data" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 15 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 13:($14/1000000) w lp ls 7 lw 1.5 pt 15 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 15:($16/1000000)  w lp ls 8 lw 1.5 pt 16 ps 1.2,\
#      "./data/ycsb10-batch-effect-1.data" u 17:($18/1000000) w lp ls 9 lw 1.5 pt 17 ps 1.2

###############
# Lower figure
###############

set bmargin at screen bm
set tmargin at screen bm + (tm-bm-gap)/2

set label "(b) Zipfian parameter {/Symbol q} = 0.6" at screen lm+sc_xoff, screen bm + (tm-bm-gap)/2 + sc_yoff left font "Times New Roman Bold" front boxed

unset key

set ylabel "Latency (ms)" offset 0,4
set ytics 3
set yrange [0:21]

set xlabel "Throughput (txn/s)" offset 0,0.4
# set xtics ("?" 50000, "?" 100000, "?" 150000, "?" 200000,"" 250000, "" 300000)
set xtics format "%.0s%c"
set xtics 50000

plot "./data/ycsb10-batch-effect-6.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 5  ps 1.2,\
     "./data/ycsb10-batch-effect-6.data" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 7  ps 1.2,\
     "./data/ycsb10-batch-effect-6.data" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 9  ps 1.2,\
     "./data/ycsb10-batch-effect-6.data" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 11 ps 1.2,\
     "./data/ycsb10-batch-effect-6.data" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "./data/ycsb10-batch-effect-6.data" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 15 ps 1.2
