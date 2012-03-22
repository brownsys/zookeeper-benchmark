set xlabel "Time since start"
set ylabel "CREATE ops per second"
plot "pre/CREATE.dat" title 'Pre' with lines, "post/CREATE.dat" title 'Post' with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14 
set output "create.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "DELETE ops per second"
plot "pre/DELETE.dat" title 'Pre' with lines, "post/DELETE.dat" title 'Post' with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "delete.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "READ ops per second"
plot "pre/READ.dat" title 'Pre' with lines, "post/READ.dat" title 'Post' with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "read.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "SETSINGLE ops per second"
plot "pre/SETSINGLE.dat" title 'Pre' with lines, "post/SETSINGLE.dat" title 'Post' with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "setsingle.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "SETMUTI ops per second"
plot "pre/SETMUTI.dat" title 'Pre' with lines, "post/SETMUTI.dat" title 'Post' with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "setmuti.ps"
replot
set term pop
