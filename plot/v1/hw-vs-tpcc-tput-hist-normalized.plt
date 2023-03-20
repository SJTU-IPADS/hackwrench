load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 14" eps color size 3, 1.2 linewidth 2 enhanced
set output "./out/hw-vs-tpcc-tput-hist-normalized.eps"

set border linewidth 0.5
set grid ytics

set key autotitle columnhead
set key tmargin center horizontal Left reverse samplen 2

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# gap <gapsize>: the empty space between clusters is equivalent to the width of
#         <gapsize> boxes (bars)
set style histogram clustered gap 1.0

# To allow gaps between boxes
set boxwidth 0.8 relative

# set logscale y 10
set ylabel "Normalized Throughput"
set xlabel "Remote Access Posibility (\%)"
set yrange [0:1.1]
# set ytics format "%.0s%c"

plot "./data/hw-vs-tpcc-tput-hist.data" using (1-$3):xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using (1-$5):xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using (1-$7):xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using (1-$9):xtic(1) with histogram fill pattern 7 linestyle 5