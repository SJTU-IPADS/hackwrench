load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,0.7 linewidth 2 enhanced
set output "./out/total-vs-partial.eps"

set border linewidth 0.5

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# gap <gapsize>: the empty space between clusters is equivalent to the width of
#         <gapsize> boxes (bars)
set style histogram clustered gap 1.0

# To allow gaps between boxes
set boxwidth 0.8 relative

# to let labels have background but no border
set style textbox opaque noborder

bm = 0.25
tm = 0.98
lm = 0.1
rm = 0.99  # figure height = (0.95-0.14) * 0.9in = 0.729in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
# set key top right horizontal Left reverse samplen 1 font "Helvetica, 10"
set key vertical samplen 1 font "Helvetica, 10" maxrows 2
# unset key

set ylabel "Tput (Txns/s)" offset 1
set xlabel "Remote access probability (\%)" offset 0,1
set grid ytics
# set ytics 100000
# set ytics format "%.0s%c"
set ytics ("0" 0, "100k" 100000, "200k" 200000, "300k" 300000, "400k" 400000) offset 0.5
set xtics offset 0,0.5
set yrange [0:530000]
set xrange [-0.6:4.6]

serie_num=3.0
plot "./data/total-vs-partial.data" using 2:xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using 3:xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using 4:xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using 5:xtic(1) with histogram fill pattern 7 linestyle 4,\
     
