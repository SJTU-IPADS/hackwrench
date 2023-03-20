load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,0.9 linewidth 2 enhanced
set output "./out/micro-contention-coco.eps"

set multiplot layout 1,2

set border linewidth 0.5

gap = 0.045
bm = 0.22
tm = 0.79
lm = 0.10
rm = 0.97  # figure height = (0.96-0.23) * 1in = 0.73in

# sc=subcaption
sc_xoff = 0.1
sc_yoff = -0.1

# left plot
set lmargin at screen lm
set rmargin at screen lm + (rm - lm) / 2 - gap
set bmargin at screen bm
set tmargin at screen tm

# set key autotitle columnhead at screen 0, 0.98 left top maxrows 2 samplen 1
set key autotitle columnhead at screen 1, 0.99 maxrows 2 samplen 1 reverse Left
set key width -3 font ", 11"
# set key right samplen 2

set grid ytics
set ylabel "Tput (Txns/s)" offset 0.8
set yrange [0:1800000]

set label "(a) Low Contention" at screen lm+sc_xoff, screen tm+sc_yoff left font "Times New Roman Bold" front
set xlabel "Distributed Transaction Percentage {\%}" offset 16,0.6

set ytics ("0.4M" 400000, "0.8M" 800000, "1.2M" 1200000, "1.6M" 1600000)
set xtics 10
set xrange [9:]


plot "./data/micro-contention-coco-0.1.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 8 ps 1.2,\
     "" u 1:($6) w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "" u 1:($7) w lp ls 6 lw 1.5 pt 11 ps 1.2,\
     "" u 1:($8) w lp ls 8 lw 1.5 pt 15 ps 1.2,\

## right plot
set lmargin at screen lm + (rm - lm) / 2 + gap
set rmargin at screen rm
set yrange [0:800000]
set ytics ("0.2M" 200000, "0.4M" 400000, "0.6M" 600000, "0.8M" 800000)

unset key
set xlabel " " offset 8,0
unset ylabel
set label "(b) High Contention" at screen lm + (rm - lm) / 2 + gap + sc_xoff, screen tm + sc_yoff left font "Times New Roman Bold" front

plot "./data/micro-contention-coco-0.99.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 8 ps 1.2,\
     "" u 1:($6) w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "" u 1:($7) w lp ls 6 lw 1.5 pt 11 ps 1.2,\
     "" u 1:($8) w lp ls 8 lw 1.5 pt 15 ps 1.2,\

unset multiplot