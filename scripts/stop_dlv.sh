#!/bin/bash
echo "stopping debug server"
if ps -a| grep  -E "^.*[0-9]+ dlv$"; then
    echo "exit" | dlv connect :2345
    sleep 1
    killall dlv
fi