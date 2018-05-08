#!/bin/bash
i=1
while (( $i<=50 ))
do
    echo "Test Loop : $i"
    #go test -run 2A
    #go test -run 2B
    #go test -run TestBasicAgree2B
    #go test -run TestFailAgree2B
    #go test -run TestPersist12C 2>log$i
    go test -run TestPersist12C
    #go test -run TestFigure8Unreliable2C
    #go test
    echo ' '
    let "i++"
done
