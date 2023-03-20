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
y1 = 0.0; y2 = 52000; y3 = 230000; y4 = 390000

#############
# Lower haft
#############

set border 1+2+8

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) )

set key autotitle columnhead
unset key

set ylabel "Throughput (txn/s)" offset -0.6,4
set grid ytics
set ytics format "%.0s%c"
set ytics 20000
set yrange [y1:y2]

set xlabel "Remote access probability (\%) | multi-warehouse NewOrder probability (\%)" offset 0,0.4
set xtics nomirror
set xrange [-0.6:10.6]

# to let labels have background but no border
set style textbox opaque noborder

serie_num=3.0
sep_width=0.3
plot "./data/hw-vs-tpcc-tput-hist.data" using ($2):xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using ($0-0.5+1/(serie_num+1)-sep_width/2):(y2):($2>y2?sep_width:0):(0) with vectors nohead lc black,\
     "" using ($3):xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using ($0-0.5+2/(serie_num+1)):3:(sprintf("%.1fk",$3/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($4):xtic(1) with histogram fill pattern 6 linestyle 3,\
     "" using ($0-0.5+3/(serie_num+1)):4:4 notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\

#############
# Upper haft
#############

set border 2+4+8

set bmargin at screen bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + gap
set tmargin at screen bm + size + gap

set key right top horizontal Left reverse samplen 2

unset ylabel
set yrange [y3:y4]

unset xtics
unset xlabel

# tilt = gap / 4.0
tilt = 0
set arrow from screen lm - gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) - tilt \
            to screen lm + gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + tilt nohead
set arrow from screen lm - gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) - tilt + gap \
            to screen lm + gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + tilt + gap nohead
set arrow from screen rm - gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) - tilt \
            to screen rm + gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + tilt nohead
set arrow from screen rm - gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) - tilt + gap \
            to screen rm + gap / 4.0, bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + tilt + gap nohead

plot "./data/hw-vs-tpcc-tput-hist.data" using ($2):xtic(1) with histogram fill pattern 1 linestyle 1 ,\
     "" using ($0-0.5+1/(serie_num+1)-sep_width/2):(y3):($2>y3?sep_width:0):(0) with vectors nohead lc black,\
     "" using ($0-0.5+1/(serie_num+1)):2:($0==0?"":sprintf("%.1fk",$2/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($0-0.5+1/(serie_num+1)+0.2):($2-48000):($0!=0?"":sprintf("%.1fk",$2/1000)) notitle with labels left font "Helvetica, 10" boxed offset 0,0.2 rotate by 90,\
     "" using ($3):xtic(1) with histogram fill pattern 2 linestyle 2,\
     "" using ($4):xtic(1) with histogram fill pattern 6 linestyle 3
