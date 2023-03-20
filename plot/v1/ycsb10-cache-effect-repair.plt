load '../color.pal'
set encoding utf8

set terminal postscript "Helvetica, 12" eps color size 3.34,1.4 linewidth 2 enhanced
set output "./out/ycsb10-cache-effect-repair.eps"

bm = 0.19
lm = 0.15
rm = 0.97
gap = 0.02
size = 0.7
y1 = 0.0; y2 = 0.026; y3 = 0.082; y4 = 0.085

set border linewidth 0.5

set key autotitle columnhead
set key top left Left reverse samplen 1

# To make boxes surrounded by a solid line
set style fill border linecolor rgb 'black'

# gap <gapsize>: the empty space between clusters is equivalent to the width of
#         <gapsize> boxes (bars)
set style histogram clustered gap 1.0

# To allow gaps between boxes
set boxwidth 0.8 relative

set grid ytics
set xlabel "Cache hit rate (%)"
set ylabel "Transaction repair rate (%)"

set multiplot

#############
# Lower haft
#############
unset key

set lmargin at screen lm
set rmargin at screen rm
set bmargin at screen bm
set tmargin at screen bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) )

set xtics nomirror
set ytics 0.005
set yrange [y1:y2]
set border 1+2+8

# to let labels have background but no border, i guess
set style textbox opaque noborder

serie_num=2.0
sep_width=0.4
plot "./data/ycsb10-cache-effect.data" u ($3):xtic(1) with histogram fill pattern 1 linestyle 1,\
     "" using ($0-0.5+1/(serie_num+1)):3:($3<0.001?sprintf("%.5f",$3):"") notitle with labels font "Helvetica, 10" boxed offset 0,1.7 rotate by 90,\
     "" u ($5) with histogram fill pattern 2 linestyle 2,\
     "" using ($0-0.5+2/(serie_num+1)-sep_width/2):(y2):($5>y2?sep_width:0):(0) with vectors nohead lc black,\

#############
# Upper haft
#############
set key

set bmargin at screen bm + size * (abs(y2-y1) / (abs(y2-y1) + abs(y4-y3) ) ) + gap
set tmargin at screen bm + size + gap

unset xtics
unset xlabel
unset ylabel
set yrange [y3:y4]
set border 2+4+8

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

plot "./data/ycsb10-cache-effect.data" u ($3):xtic(1) with histogram fill pattern 1 linestyle 1,\
     "" u ($5) with histogram fill pattern 2 linestyle 2,\
     "" using ($0-0.5+2/(serie_num+1)-sep_width/2):(y3):($5>y3?sep_width:0):(0) with vectors nohead lc black,\

# plot "./data/ycsb10-cache-effect.data" u 1:($3) w lp ls 1 lw 1.5 pt 5 ps 1.2,\
#      "" u 1:($5) w lp ls 2 lw 1.5 pt 7 ps 1.2