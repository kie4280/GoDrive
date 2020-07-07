#!/bin/bash --init-file
echo "start debug server"
if ps -A|grep dlv; then
    echo "exit" | dlv connect :2345
fi
cd cmd
dlv debug --headless --listen=:2345 --api-version=2

exit
