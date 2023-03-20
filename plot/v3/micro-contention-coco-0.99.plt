load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1 linewidth 2 enhanced
set output "./out/micro-contention-coco-0.99.eps"

set border linewidth 0.5

bm = 0.23
tm = 0.96
lm = 0.1
rm = 0.97  # figure height = (0.96-0.23) * 1in = 0.73in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead horizontal at 100,750000
# set key right samplen 2

set grid ytics
set ylabel "Txns/s" offset 1
set yrange [0:800000]

set xlabel "Distributed Transaction Percentage {\%}" offset 0,0.4
set ytics ("0.2M" 200000, "0.4M" 400000, "0.6M" 600000, "0.8M" 800000)
set xtics 10
set xrange [9:]

plot "./data/micro-contention-coco-0.99.data" u 1:($2) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
     "" u 1:($3) w lp ls 2 lw 1.5 pt 7 ps 1.2,\
     "" u 1:($4) w lp ls 3 lw 1.5 pt 9 ps 1.2,\
     "" u 1:($5) w lp ls 4 lw 1.5 pt 11 ps 1.2,\
     "" u 1:($6) w lp ls 5 lw 1.5 pt 13 ps 1.2,\
     "" u 1:($7) w lp ls 6 lw 1.5 pt 15 ps 1.2,\
