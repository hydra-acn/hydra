#!/usr/bin/env bash

deploy_local() {
    if [ $# -lt 2 ]; then
        echo "Usage: ./deploy.sh local <n> <localAddr> [options]"
        exit -1
    fi

    n=$1
    localhost=$2
    shift
    shift

    echo -n "Killing last session first ..."
    tmux kill-session -t $session > /dev/null 2>&1
    echo " Done"

    tmux new-session -d -s $session
    echo -n "Starting directory server on port $dirport ..."
    tmux new-window -d -t "=${session}" -n directory
    tmux send-keys -t "=${session}:=directory" "target/$mode/directory_service 0.0.0.0 $dirkey $dircrt -k $numrounds -d $rounddur -w $roundwait" Enter
    sleep 1
    echo " Done"
    echo "Starting $n mixes .."
    for i in `seq 1 $n`; do
        port=`echo $dirport + $i | bc`
        echo -n "-> Starting mix on port $port ..."
        tmux new-window -d -t "=${session}" -n mix-$i
        tmux send-keys -t "=${session}:=mix-$i" "target/$mode/mix $localhost:$port $threads -d $dirdom -p $dirport -c $cacrt $x25519" Enter
        echo " Done"
    done
    # Kill the "default" tmux window
    tmux kill-window -t "=${session}:0"
}

deploy_testbed() {
    if [ $# -lt 1 ]; then
        echo "Usage: ./deploy.sh testbed <n> [options]"
        exit -1
    fi
    n=$1
    shift

    # setup directory service on this host (kill old session before)
    tmux kill-session -t $session > /dev/null 2>&1
    tmux new-session -d -s $session
    echo -n "Starting directory server on port $dirport ..."
    tmux send-keys -t "=${session}:" "target/$mode/directory_service 0.0.0.0 $dirkey $dircrt -k $numrounds -d $rounddur -w $roundwait" Enter
    sleep 1
    echo " Done"

    # setup mixes
    port=9000
    echo "Starting $n mixes .."
    for i in `seq -w 01 $n`; do
        mix=mix-$i
        id=`echo $i | sed 's/^0*//'`
        addr=10.0.0.$id
        echo "Starting mix on host $mix, address $addr"
        # kill last tmux session and prepare testbed directory
        ssh $mix tmux kill-session -t $session > /dev/null 2>&1
        ssh $mix mkdir -p .testbed
        # copy binary and ca file
        scp target/$mode/mix $mix:.testbed/ > /dev/null
        scp $cacrt $mix:.testbed/ > /dev/null
        # start tmux session
        ssh $mix tmux new-session -d -s $session
        # start the engine :)
        # TODO avoid SPACE?
        ssh $mix tmux send-keys -t "=${session}:" ".testbed/mix Space $addr:$port Space $threads Space -d Space $dirdom Space -p Space $dirport Space -c Space $cacrt Space $x25519" Enter
    done
}

if [ $# -lt 1 ]; then
    echo "Missing subcommand (local|testbed)!"
    exit -1
fi

subcmd=$1
shift;

# copy arguments
args=$@

# global argument parsing (independent of subcmd)
session="hydra-testbed"
dirdom="hydra-swp.prakinf.tu-ilmenau.de"
dirport=9000
dirkey=".testbed/directory.key"
dircrt=".testbed/directory.crt"
cacrt=".testbed/ca.pem"
numrounds=8
rounddur=7
roundwait=13
x25519=""
threads=4
mode="release"
build=1

while [ -n "$1" ]; do
    case "$1" in
        --debug) mode="debug" ;;
        --dirdom) dirdom=$2; shift ;;
        --cache) build=0 ;;
        -k|--comm-rounds) numrounds=$2; shift ;;
        -d|--round-duration) rounddur=$2; shift ;;
        -w|--round-wait) roundwait=$2; shift ;;
        --x25519) x25519="--x25519" ;;
        -t|--threads) threads=$2; shift ;;
        -*) echo "Unknown option $1"; exit -1;;
    esac
    # shift once in any case
    shift
done

# compile for all subcmds (if not disabled)
if [ $build -eq 1 ]; then
    echo "Building in $mode mode ..."
    if [ $mode == "debug" ]; then
        cargo build
    else
        cargo build --$mode
    fi

    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit -1
    fi
fi

# call subcmd
if [ "$subcmd" == "local" ]; then
    deploy_local $args
elif [ "$subcmd" == "testbed" ]; then
    deploy_testbed $args
else
    echo "Unknown subcommand: $subcmd"
fi

