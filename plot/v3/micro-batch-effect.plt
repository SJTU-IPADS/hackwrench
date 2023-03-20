load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1.7 linewidth 2 enhanced
set output "./out/micro-batch-effect.eps"

set border linewidth 0.5

# to let labels have background but no border
set style textbox opaque noborder

# layout <rows>, <cols>
set multiplot layout 2, 1

bm = 0.13
tm = 0.99
lm = 0.1
rm = 0.97
gap = 0.08  # each subplot's height = 0.41 * 1.7in = 0.697in

# sc=subcaption
sc_xoff = 0.01
sc_yoff = -0.06

###############
# Upper figure
###############

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm + (tm-bm-gap)/2 + gap
set tmargin at screen tm

set label "(a) Low Contention" at screen lm+sc_xoff, screen tm+sc_yoff left font "Times New Roman Bold" front boxed

unset key

set grid ytics
set ytics autofreq
set yrange [-2:42]
set ytics 10

set xtics ("0.2M" 200000, "0.4M" 400000, "0.6M" 600000, "0.8M" 800000, "1M" 1000000, "1.2M" 1200000)
# set xtics format "%.0s%c"
set xrange [0:1220000]

plot "./data/micro-batch-effect-0.1.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 3  ps 1.2,\
     "" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 5  ps 1.2,\
     "" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 7  ps 1.2,\
     "" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 9 ps 1.2,\
     "" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 11 ps 1.2,\
     "" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 13 ps 1.2,\
     "" u 13:($14/1000000) w lp ls 8 lw 1.5 pt 15 ps 1.2,\

###############
# Lower figure
###############

set bmargin at screen bm
set tmargin at screen bm + (tm-bm-gap)/2

set label "(b) High Contention" at screen lm+sc_xoff, screen bm + (tm-bm-gap)/2 + sc_yoff left font "Times New Roman Bold" front boxed

set key autotitle columnhead
set key center left Left reverse samplen 1 maxrows 4

set ylabel "Latency (ms)" offset 0,4
set ytics 20
set yrange [-3:75]

set xtics ("0.1M" 100000, "0.2M" 200000, "0.3M" 300000, "0.4M" 400000, "0.5M" 500000, "0.6M" 600000, "0.7M" 700000, "0.8M" 800000, "0.9M" 900000, "1M" 1000000)
set xlabel "Txns/s" offset 0,0.4
set xrange [0:620000]

plot "./data/micro-batch-effect-0.99.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 3  ps 1.2,\
     "" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 5  ps 1.2,\
     "" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 7  ps 1.2,\
     "" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 9 ps 1.2,\
     "" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 11 ps 1.2,\
     "" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 13 ps 1.2,\
     "" u 13:($14/1000000) w lp ls 8 lw 1.5 pt 15 ps 1.2,\
