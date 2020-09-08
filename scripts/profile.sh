#!/bin/bash

go tool pprof -top http://localhost:6060/debug/pprof/heap
