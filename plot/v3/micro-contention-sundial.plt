load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,0.9 linewidth 2 enhanced
set output "./out/micro-contention-sundial.eps"

set multiplot layout 1,2

set border linewidth 0.5

gap = 0.045
bm = 0.22
tm = 0.79
lm = 0.10
rm = 0.97  # figure height = (0.96-0.23) * 1in = 0.73in

# sc=subcaption
# sc_xoff = 0.16
# sc_yoff = -0.05

sc_xoff = 0.1
sc_yoff = -0.32

# left plot
set lmargin at screen lm
set rmargin at screen lm + (rm - lm) / 2 - gap
set bmargin at screen bm
set tmargin at screen tm
# set key autotitle columnhead horizontal at 100,850000
set key autotitle columnhead at screen 0.18, 0.99 vertical left top maxrows 2 samplen 2 reverse Left
# set key right samplen 2

set grid ytics
set ylabel "Tput (Txns/s)" offset 0.8
set yrange [0:1300000]

set label "(a) Low Contention" at screen lm+sc_xoff+0.05, screen tm+sc_yoff left font "Times New Roman Bold" front
set xlabel "Distributed Transaction Percentage {\%}" offset 16,0.6

set ytics ("0.4M" 400000, "0.8M" 800000, "1.2M" 1200000, "1.6M" 1600000)
set xtics 10
set xrange [9:]

plot "./data/micro-contention-sundial-0.1.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 8 ps 1.2,\
     # "" u 1:($6) w lp ls 5 lw 1.5 pt 13 ps 1.2,\


# right plot
set lmargin at screen lm + (rm - lm) / 2 + gap
set rmargin at screen rm
set yrange [0:600000]
set ytics ("0.2M" 200000, "0.4M" 400000, "0.6M" 600000)
unset key
set xlabel " " offset 8,0
unset ylabel

set label "(b) High Contention" at screen lm + (rm - lm) / 2 + gap + sc_xoff, screen 0.7 left font "Times New Roman Bold" front

plot "./data/micro-contention-sundial-0.99.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 8 ps 1.2,\
     # "" u 1:($6) w lp ls 5 lw 1.5 pt 13 ps 1.2,\

unset multiplot