#!/bin/bash

gcc it_tests_blockmachine_wrapper.c -o it_tests_blockmachine_wrapper
gcc it_tests_unblockmachine_wrapper.c -o it_tests_unblockmachine_wrapper

sudo chown root it_tests_blockmachine_wrapper
sudo chown root it_tests_unblockmachine_wrapper

sudo chmod oug+r-w+s+x it_tests_blockmachine_wrapper
sudo chmod oug+r-w+s+x it_tests_unblockmachine_wrapper
