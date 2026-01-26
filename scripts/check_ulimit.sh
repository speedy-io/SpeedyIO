#!/bin/bash

limit=300000

check_ulimit() {
        current_limit=$(ulimit -n)

        # Check if the current limit is less than 10000
        if [ "$current_limit" -lt $limit ]; then
                echo "The current ulimit is less than 10000: $current_limit"
                echo "Rocksdb makes a lot of files; experiment might not run to completion"
                increase_ulimit
        else
                echo "The current ulimit is $current_limit, which is sufficient."
        fi
}

increase_ulimit(){
        echo "This permanently increases limit on #files opened at once [ulimit -n] (required by rocksdb)"

        echo "This will require sudo access and a reboot to take effect"
        echo "Do you want to proceed? press Enter to proceed or Ctrl-C to abort"

        read u


        echo "$USER soft nofile $limit" | sudo tee -a /etc/security/limits.conf
        echo "$USER hard nofile $limit" | sudo tee -a /etc/security/limits.conf

        echo "Now Reboot this machine. ulimit -n will show $limit after reboot"
}

check_ulimit
