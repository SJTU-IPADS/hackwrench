load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,0.9 linewidth 2 enhanced
set output "./out/tpcc-abort-vs-repair.eps"

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

bm = 0.14
tm = 0.95
lm = 0.1
rm = 0.99  # figure height = (0.95-0.14) * 0.9in = 0.729in

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
set key top left Left reverse samplen 1

set ylabel "Throughput (txn/s)" offset 1
set grid ytics
set ytics format "%.0s%c"

set xrange [-0.6:3.6]

serie_num=4.0
plot "./data/tpcc-abort-vs-repair.data" using 2:xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using ($0-0.5+1/(serie_num+1)):2:($2<10000?sprintf("%.1f",$2):"") notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using 3:xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using ($0-0.5+2/(serie_num+1)):3:($3<10000?sprintf("%.1f",$3):"") notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using 4:xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using ($0-0.5+3/(serie_num+1)):4:($4<10000?sprintf("%.1f",$4):"") notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using 5:xtic(1) with histogram fill pattern 7 linestyle 5,\
     "" using ($0-0.5+4/(serie_num+1)):5:($5<10000?sprintf("%.1f",$5):"") notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90
