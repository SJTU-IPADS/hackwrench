load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 1.67,0.8 linewidth 2 enhanced
set output "./out/ts-bottleneck.eps"

set border linewidth 0.5

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# To allow gaps between boxes
set boxwidth 1.7 relative

# to let labels have background but no border
set style textbox opaque noborder

bm = 0.27
tm = 0.94
lm = 0.17
rm = 0.99  # figure height = (0.96-0.23) * 1in = 0.73in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
# set key top right invert vertical Right samplen 1 font "Helvetica, 10"
unset key
set ylabel "Tput (Txns/s)" offset 0.68
set grid ytics
set ytics ("2M" 2000000, "4M" 4000000, "6M" 6000000, "8M" 8000000, "10M" 10000000, "12M" 12000000)
# set ytics format "%.0s%c"
set yrange [0:12000000]
set xrange [-0.5:6.5]
set xtics ("1" 0, "5" 1, "10" 2, "20" 3, "40" 4, "80" 5, "160" 6)
set xlabel "Batch Size" offset 0,0.5
# set xtics offset 0,0.3

plot "./data/ts-bottleneck.data" using 2 with histogram fill pattern 3 linestyle 5
     
