set xlabel "Time since start"
set ylabel "CREATE ops per second"
plot "CREATE.dat" with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14 
set output "create.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "DELETE ops per second"
plot "DELETE.dat" with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "delete.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "READ ops per second"
plot "READ.dat" with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "read.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "SETSINGLE ops per second"
plot "SETSINGLE.dat" with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "setsingle.ps"
replot
set term pop

set xlabel "Time since start"
set ylabel "SETMUTI ops per second"
plot "SETMUTI.dat" with lines

set size 1.0, 0.6
set terminal postscript portrait enhanced color dashed lw 1 "Helvetica" 14
set output "setmuti.ps"
replot
set term pop
