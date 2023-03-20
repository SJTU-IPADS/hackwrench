load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,0.8 linewidth 1.5 enhanced
set output "./out/micro-batch-effect-half.eps"

set border linewidth 0.5

# to let labels have background but no border
set style textbox opaque noborder

# layout <rows>, <cols>
set multiplot layout 1, 2

bm = 0.27
tm = 0.88
lm = 0.07
rm = 0.97
gap = 0.025 # each subplot's height = 0.41 * 1.7in = 0.697in

# sc=subcaption
sc_xoff = 0.01
sc_yoff = -0.06

###############
# Left figure
###############

set lmargin at screen lm
set rmargin at screen lm + (rm - lm) / 2 - gap
set bmargin at screen bm
set tmargin at screen tm

set label "(a) Low Contention" at screen lm+sc_xoff+0.2, screen tm+sc_yoff left font "Times New Roman Bold" front boxed

set key autotitle columnhead at screen 0, 0.98 vertical left Left top maxrows 1 samplen 0.5 font ",12" reverse
set xlabel "Tput (Txns/s)" offset 17,0.6

set grid ytics
set ytics autofreq
set yrange [-2:49]
set ytics 10

set xtics ("0.2M" 200000, "0.4M" 400000, "0.6M" 600000, "0.8M" 800000, "1M" 1000000)
# set xtics format "%.0s%c"
set xrange [0:1120000]
set ylabel "Latency (ms)" offset 1.7

plot "./data/micro-batch-effect-0.1.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 3  ps 1.2,\
     "" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 5  ps 1.2,\
     "" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 7  ps 1.2,\
     "" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 9 ps 1.2,\
     "" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 11 ps 1.2,\
     "" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 13 ps 1.2,\
     "" u 13:($14/1000000) w lp ls 8 lw 1.5 pt 15 ps 1.2,\

###############
# Right figure
###############

set lmargin at screen lm + (rm - lm) / 2 + gap
set rmargin at screen rm

set label "(b) High Contention" at screen lm + (rm - lm) / 2 + gap + sc_xoff - 0.005, screen tm + sc_yoff left font "Times New Roman Bold" front boxed

# set key autotitle columnhead
# set key center left Left reverse samplen 1 maxrows 4
unset key
unset ylabel
unset xlabel
set ytics 20
set yrange [-3:75]

set xtics ("0.1M" 100000, "0.2M" 200000, "0.3M" 300000, "0.4M" 400000, "0.5M" 500000, "0.6M" 600000, "0.7M" 700000, "0.8M" 800000, "0.9M" 900000, "1M" 1000000)
set xrange [0:610000]

plot "./data/micro-batch-effect-0.99.data" u 1:($2/1000000)   w lp ls 1 lw 1.5 pt 3  ps 1.2,\
     "" u 3:($4/1000000)   w lp ls 2 lw 1.5 pt 5  ps 1.2,\
     "" u 5:($6/1000000)   w lp ls 3 lw 1.5 pt 7  ps 1.2,\
     "" u 7:($8/1000000)   w lp ls 4 lw 1.5 pt 9 ps 1.2,\
     "" u 9:($10/1000000)  w lp ls 5 lw 1.5 pt 11 ps 1.2,\
     "" u 11:($12/1000000) w lp ls 6 lw 1.5 pt 13 ps 1.2,\
     "" u 13:($14/1000000) w lp ls 8 lw 1.5 pt 15 ps 1.2,\
