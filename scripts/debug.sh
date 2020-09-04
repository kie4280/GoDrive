#!/bin/bash --init-file
echo "start debug server"
if ps -A|grep dlv; then
    echo "exit" | dlv connect :2345
fi
dlv debug ./cmd --headless --listen=:2345 --api-version=2
EXIT_CODE=$?
if [[ $EXIT_CODE -ne 0 ]]; then
echo "error code: $EXIT_CODE";
fi
exit $EXIT_CODE
