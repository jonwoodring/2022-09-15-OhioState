#!/bin/bash

cd slides
python -m http.server 8080 &
exec $BROWSER http://localhost:8080/index.html &
