#!/bin/bash

if [ "$#" -ne 3 ]
then
	echo 'usage: ./launcher.sh <VM> <NUM_NODES> <INTRO_PORT>'	
	exit
fi

declare -i cur_port=0
declare -i cur_name=0
cur_port=$(( $1 * 1000 ))
cur_name=$(( $1 * 100  ))
# echo $cur_port

for i in $(seq 0 $2); do
	if [ "$i" == "0" ]
	then
		./node NODE$cur_name $1 $cur_port $3 1 &
	else
		./node NODE$cur_name $1 $cur_port $3 &
	fi 
	cur_port=$(( $cur_port + 1 ))
	cur_name=$(( $cur_name + 1 ))
done