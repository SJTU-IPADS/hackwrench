load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 14" eps color size 3, 1.8 linewidth 2 enhanced
set output "./out/tpcc-replication-effect.eps"

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


set ylabel "Peak throughput (txn/s)"
set ytics format "%.0s%c"
set yrange[0:]

plot "./data/tpcc-replication-effect.data" using 2:xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using 3:xtic(1) with histogram fill pattern 2 linestyle 2