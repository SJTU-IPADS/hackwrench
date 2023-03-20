load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.5, 1.0 linewidth 2 enhanced
set output "./out/hw-vs-tpcc-tput-hist-fdb.eps"

set border linewidth 0.5

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# gap <gapsize>: the empty space between clusters is equivalent to the width of
#         <gapsize> boxes (bars)
set style histogram clustered gap 1.0

# To allow gaps between boxes
set boxwidth 0.8 relative

# layout <rows>, <cols>
set multiplot layout 2, 1

lm = 0.1
rm = 0.993
bm = 0.19
tm = 0.98
gap = 0.02
size = 0.77
# y1 = 0.0; y2 = 52000; y3 = 230000; y4 = 390000

#############
# Lower haft
#############

set border 1+2+4+8

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen tm

set key autotitle columnhead
unset key

set ylabel "Tput (Txns/s)" offset 1
set grid ytics
set ytics format "%.0s%c"
set ytics ("0" 0, "80k" 80000, "160k" 160000, "240k" 240000, "320k" 320000, "400k" 400000)
set yrange [0:440000]

set xlabel "Percentage of Remote NewOrder Transactions (\%)" offset 0,0.9
set xtics nomirror offset 0, 0.4
set xrange [-0.6:5.6]

# to let labels have background but no border
set style textbox opaque noborder

set key right top horizontal Left reverse samplen 2

serie_num=4.0
sep_width=0.3
plot "./data/hw-vs-tpcc-tput-hist-fdb.data" \
        using ($2):xtic(1) with histogram fill pattern 3 linestyle 1 ,\
     "" using ($5):xtic(1) with histogram fill pattern 3 linestyle 5,\
#      "" using ($0-0.5+1/(serie_num+1)):2:(sprintf("%.1fk",$2/1000)) notitle with labels left font "Helvetica, 10" boxed offset -1,-2.5 rotate by 90,\
#      "" using ($0-0.5+4/(serie_num+1)):5:(sprintf("%.0f",$5)) notitle with labels left font "Helvetica, 10" boxed offset -1.4,0.2 rotate by 90,\


#############
# Upper haft
#############

