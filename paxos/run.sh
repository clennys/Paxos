#!/usr/bin/env bash

projdir="$1"
conf="$(pwd)/paxos.conf"
n="$2"
value=$(echo "0.15 * $2" | bc -l)  # Use bc for floating-point calculation


if [[ x$projdir == "x" || x$n == "x" ]]; then
    echo "Usage: $0 <project dir> <number of values per proposer>"
    exit 1
fi

pkill -f "$conf" # kill processes that have the config file in its cmdline

cd $projdir

../generate.sh $n >../prop1
../generate.sh $n >../prop2

echo "starting acceptors..."

./acceptor.sh 1 "$conf" &
./acceptor.sh 2 "$conf" &
./acceptor.sh 3 "$conf" &

sleep 1
echo "starting learners..."

./learner.sh 1 "$conf" >../learn1 &
./learner.sh 2 "$conf" >../learn2 &

# ./learner.sh 1 "$conf" &
# ./learner.sh 2 "$conf" &
sleep 1

echo "starting proposers..."

./proposer.sh 1 "$conf" &
./proposer.sh 2 "$conf" &

echo "waiting to start clients"
sleep 10
echo "starting clients..."

./client.sh 1 "$conf" <../prop1 &
./client.sh 2 "$conf" <../prop2 &

sleep $value

pkill -f "$conf"
wait

cd ..
