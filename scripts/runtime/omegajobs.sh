#!/bin/bash
## package
##
## Run jupyterhub or jupyterhub-singleuser
##
## Options:
##    --singleuser     Run jupyterhub-singleuser
##    --ip=VALUE       ip address
##    --port=PORT      port
##    --installonly    install only then exit
##    --label          runtime label
##    --debug          debug jupyterhub and notebook
##
##    @script.name [option]
# script setup to parse options
script_dir=$(dirname "$0")
script_dir=$(realpath $script_dir)
source $script_dir/easyoptions || exit
# set defaults
ip=${ip:-0.0.0.0}
port=${port:-5000}
omegaml_dir=$(python -W ignore -c  "import omegaml; print(omegaml.__path__[0])")
runtimelabel=${label:-$(hostname)}
if [[ ! -z $debug ]]; then
  jydebug="--debug"
fi
# setup environment
# TODO env vars should come from runtime/worker configmap
export PYTHONPATH="/app/pylib/user:/app/pylib/base"
export PYTHONUSERBASE="/app/pylib/user"
export C_FORCE_ROOT=1
export CELERY_Q=$runtimelabel
if [[ ! -f $HOME/.jupyter/.omegaml ]]; then
    mkdir -p $HOME/.jupyter
    cp $omegaml_dir/notebook/jupyter/* $HOME/.jupyter/
fi
cd $HOME/.jupyter
nohup honcho -d /app start worker >> worker.log 2>&1 &
jupyterhub-singleuser --ip $ip --port $port $jydebug
