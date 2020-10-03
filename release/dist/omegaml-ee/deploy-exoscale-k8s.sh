#!/usr/bin/env bash
## package
##
## Deploy to kubernetes cluster
##    @script.name [option]
##
##    --full-client      open all ports including mongodb, rabbitmq
##    --clean            delete all services, pods, configmaps, secretsbefore, DBs before redeploy
##
# script setup to parse options
script_dir=$(dirname "$0")
script_dir=$(realpath $script_dir)
source $script_dir/easyoptions || exit
source $script_dir/omutils || exit

CLIENT_CONFIG=$HOME/.omegaml/
METALB_CONFIG=./exocloud/metalb-configmap.yml
SSH_KEY=$HOME/.ssh/id_rke-k8s-key-exoscale

# build args for deploy-rancher-k8s
DEPLOY_ARGS=""
if [[ ! -z $clean ]]; then
    DEPLOY_ARGS="$DEPLOY_ARGS --clean"
fi

$script_dir/deploy-rancher-k8s.sh --metalb $METALB_CONFIG --config $CLIENT_CONFIG --sshkey $SSH_KEY $DEPLOY_ARGS