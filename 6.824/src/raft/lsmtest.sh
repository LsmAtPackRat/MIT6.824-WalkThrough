#!/bin/bash
i=1
while (( $i<=10 ))
do
    go test -run TestFigure8Unreliable2C
    #go test -run 2A
    let "i++"
done
