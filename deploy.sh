#!/usr/bin/env bash

deploy_local() {
    if [ $# -lt 2 ]; then
        echo "Usage: ./deploy.sh local <n> <localAddr>"
        exit -1
    fi

    session="hydra-testbed"
    echo -n "Killing last session first ..."
    tmux kill-session -t $session > /dev/null 2>&1
    echo " Done"

    mode="debug"
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

    n=$1
    localhost=$2
    dirport=9000
    phasedur=120
    tmux new-session -d -s $session
    echo -n "Starting directory server on port $dirport ..."
    tmux new-window -d -t "=${session}" -n directory
    tmux send-keys -t "=${session}:=directory" "target/$mode/directory_service $localhost -d $phasedur" Enter
    sleep 1
    echo " Done"
    echo "Starting $n mixes .."
    for i in `seq 1 $n`; do
        port=`echo $dirport + $i | bc`
        echo -n "-> Starting mix on port $port ..."
        tmux new-window -d -t "=${session}" -n mix-$i
        tmux send-keys -t "=${session}:=mix-$i" "target/$mode/mix $localhost:$port -d $localhost:$dirport" Enter
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

