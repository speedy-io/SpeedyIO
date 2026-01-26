#!/bin/bash

for i in {2..32}; do
    host="node$i"
    echo ">>> Copying configs to $host"

    scp -r *.sh "$host:~/"

    scp -r \
        ~/.ssh/id_ed25519* \
        ~/.ssh/id_rsa* \
        ~/.ssh/config \
        "$host:~/.ssh"

    scp -r \
        ~/.tmux.conf \
        ~/.bashrc \
        ~/.vim \
        ~/.vimrc \
        "$host:~/"
done
