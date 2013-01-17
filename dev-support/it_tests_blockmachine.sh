#!/bin/bash

/sbin/iptables -A INPUT -p all -s $1 -j DROP
#/sbin/iptables -A OUTPUT -p all -d $1 -j DROP
