#!/usr/bin/env bash

deploy_local() {
    if [ $# -lt 2 ]; then
        echo "Usage: ./deploy.sh local <n> <localAddr> [options]"
        exit -1
    fi

    n=$1
    if [ $n -eq 1 ]; then
        simple_flag="--simple"
    else
        simple_flag=""
    fi
    localhost=$2
    dirport=9000
    dirkey="testbed/directory.key"
    dircrt="testbed/directory.crt"
    cacrt="testbed/ca.pem"
    phasedur=120
    mode="debug"

    shift
    shift
    while [ -n "$1" ]; do
        case "$1" in
            --release) mode="release" ;;
            *) echo "Unknown option $1"; exit -1 ;;
        esac
        shift
    done

    session="hydra-testbed"
    echo -n "Killing last session first ..."
    tmux kill-session -t $session > /dev/null 2>&1
    echo " Done"

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

    tmux new-session -d -s $session
    echo -n "Starting directory server on port $dirport ..."
    tmux new-window -d -t "=${session}" -n directory
    tmux send-keys -t "=${session}:=directory" "target/$mode/directory_service 0.0.0.0 $dirkey $dircrt -d $phasedur" Enter
    sleep 1
    echo " Done"
    echo "Starting $n mixes .."
    for i in `seq 1 $n`; do
        port=`echo $dirport + $i | bc`
        echo -n "-> Starting mix on port $port ..."
        tmux new-window -d -t "=${session}" -n mix-$i
        tmux send-keys -t "=${session}:=mix-$i" "target/$mode/mix $localhost:$port -p $dirport -c $cacrt $simple_flag" Enter
        echo " Done"
    done
    # Kill the "default" tmux window
    tmux kill-window -t "=${session}:0"
}

if [ $# -lt 1 ]; then
    echo "Missing subcommand (local)!"
    exit -1
fi

subcmd=$1
shift;

if [ "$subcmd" == "local" ]; then
    deploy_local $@
else
    echo "Unknown subcommand: $subcmd"
fi

