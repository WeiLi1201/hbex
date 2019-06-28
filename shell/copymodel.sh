#!/bin/bash

/usr/bin/expect <<-EOF
spawn scp -r root@45.77.179.204:/root/train/model /root/hbex
expect {
"*password" { send "?6Us7HeTv1%Mjzw?\r" }
}
expect eof
exit 0
EOF