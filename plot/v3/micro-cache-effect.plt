load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 1.67,0.8 linewidth 2 enhanced
set output "./out/micro-cache-effect.eps"

set border linewidth 0.5

bm = 0.23
tm = 0.94
lm = 0.2
rm = 0.95  # figure height = (0.96-0.23) * 1in = 0.73in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
set key top left Left reverse samplen 1 at 0.1, 690000 font ",11"

set ylabel "Tput (Txns/s)" offset 1
set ytics 200000
set ytics format "%.0s%c"
set yrange [0:]

set xlabel "Cache Miss Rate (%)" offset 0,0.8
set xrange [-3:103]
set logscale x

plot "./data/micro-cache-effect.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 6 lw 1.5 pt 11 ps 1.2,\