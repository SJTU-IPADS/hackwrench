#!/bin/bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CODE_DIR=$(dirname $SCRIPTPATH)

for token in ${2}; do
    # echo ${token}
    ssh -F ${1}/ssh_config ${token} 'rm -rf ~/logs/*'
    rsync -e "ssh -o StrictHostKeyChecking=no -F ${1}/ssh_config" -aqzP ${1}/* ${token}:~/ --exclude={'logs'} &
done

# echo "send files to ${i} nodes..."

wait
