#!/bin/bash
i=1
while (( $i<=30 ))
do
    echo "Test Loop : $i"
    #go test -run TestFigure8Unreliable2C
    #go test -run 2A
    go test -run 2B
    #go test -run TestPersist12C
    #go test
    echo ' '
    let "i++"
done
