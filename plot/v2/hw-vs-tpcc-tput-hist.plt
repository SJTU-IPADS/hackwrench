load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 7, 1.2 linewidth 2 enhanced
set output "./out/hw-vs-tpcc-tput-hist.eps"

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

lm = 0.05
rm = 0.993
bm = 0.19
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
#set tmargin at screen bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) )

set key autotitle columnhead
unset key

set ylabel "Throughput (txn/s)" offset 1
set grid ytics
set ytics format "%.0s%c"
set ytics 40000
set yrange [0:390000]

set xlabel "Remote access probability (\%) | multi-warehouse NewOrder percentage (\%)" offset 0,0.4
set xtics nomirror
set xrange [-0.6:9.6]

# to let labels have background but no border
set style textbox opaque noborder

set key right top horizontal Left reverse samplen 2

serie_num=4.0
sep_width=0.3
plot "./data/hw-vs-tpcc-tput-hist.data" \
        using ($2):xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using ($3):xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using ($4):xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using ($5):xtic(1) with histogram fill pattern 7 linestyle 5,\
     "" using ($0-0.5+1/(serie_num+1)):2:(sprintf("%.1fk",$2/1000)) notitle with labels left font "Helvetica, 10" boxed offset -2,-1.5 rotate by 90,\
     "" using ($0-0.5+2/(serie_num+1)):3:(sprintf("%.1fk",$3/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($0-0.5+3/(serie_num+1)):4:(sprintf("%.1fk",$4/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($0-0.5+4/(serie_num+1)):5:(sprintf("%.1f",$5)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\


#############
# Upper haft
#############

