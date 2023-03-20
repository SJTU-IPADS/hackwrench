load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 1.67,0.7 linewidth 2 enhanced
set output "./out/breakdown-coco.eps"

set border linewidth 0.5

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# gap <gapsize>: the empty space between clusters is equivalent to the width of
#         <gapsize> boxes (bars)
set style histogram rowstacked gap 2.0


# To allow gaps between boxes
set boxwidth 0.6 relative

# to let labels have background but no border
set style textbox opaque noborder

bm = 0.14
tm = 0.93
lm = 0.16
rm = 0.99  # figure height = (0.95-0.14) * 0.9in = 0.729in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
set key top right vertical Right samplen 1 font "Helvetica, 10"

set grid ytics
# set ytics ("20%" 20, "40%" 40000, "60" 60000)
set ytics format "%g%%"
set yrange [0:100]
set xrange [-0.5:2.5]
set xtics offset 0,0.3


set boxwidth 0.4

plot "./data/breakdown-coco.data" using 2:xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using 3:xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using 4:xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using 5:xtic(1) with histogram fill pattern 7 linestyle 4,\
     "" using 6:xtic(1) with histogram fill pattern 9 linestyle 5,\
     "" using 7:xtic(1) with histogram fill pattern 10 linestyle 6,\
     
