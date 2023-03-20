load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 14" eps color dashed size 3,1.7
set output "./out/hw-vs-tpcc-drop.eps"

set key autotitle columnhead
set key tmargin center horizontal Left reverse samplen 2

set style fill pattern border 1

set xlabel "Cache Hit Ratio (%)"
set ylabel "Transaction Repair Ratio (%)"

# set logscale y 2

plot "./data/hw-vs-tpcc-tput-hist.data" u 1:($3) w lp ls 1 lw 3 pt 1 ps 1.2,\
"" u 1:($5) w lp ls 2 lw 3 pt 3 ps 1.2,\
"" u 1:($7) w lp ls 3 lw 3 pt 5 ps 1.2