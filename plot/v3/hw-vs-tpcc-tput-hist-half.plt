load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.5, 0.9 linewidth 2 enhanced
set output "./out/hw-vs-tpcc-tput-hist-half.eps"

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
set yrange [0:489000]

set xlabel "Remote Access Probability (\%) | Multi-warehouse NewOrder Percentage (\%)" offset 0,0.9
set xtics nomirror offset 0, 0.4
set xrange [-0.6:5.6]

# to let labels have background but no border
set style textbox opaque noborder

set key at screen 1.05, 0.98 right top horizontal Left reverse samplen 2

serie_num=4.0
sep_width=0.3
plot "./data/hw-vs-tpcc-tput-hist-half.data" \
        using ($2):xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using ($3):xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using ($4):xtic(1) with histogram fill pattern 6 linestyle 6,\
     "" using ($5):xtic(1) with histogram fill pattern 7 linestyle 5,\
     "" using ($0-0.5+1/(serie_num+1)):2:(sprintf("%.1fk",$2/1000)) notitle with labels left font "Helvetica, 10" boxed offset -2,-2.5 rotate by 90,\
     "" using ($0-0.5+2/(serie_num+1)):3:(sprintf("%.1fk",$3/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($0-0.5+3/(serie_num+1)):4:(sprintf("%.1fk",$4/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($0-0.5+4/(serie_num+1)):5:(sprintf("%.0f",$5)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\


#############
# Upper haft
#############

