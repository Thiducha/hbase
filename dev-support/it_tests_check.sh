#!/bin/bash

F1=`find -name it_tests_blockmachine_wrapper  -type f -user root -perm oug+r-w+s+x | wc -l`
F2=`find -name it_tests_unblockmachine_wrapper  -type f -user root -perm oug+r-w+s+x | wc -l`

if [ $F1 -eq 1 ]  && [ $F2 -eq 1 ]; then
 exit 0
else
 exit 1
fi

