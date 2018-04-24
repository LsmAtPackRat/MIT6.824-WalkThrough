#!/bin/bash
find . -type f -name "mrtmp.test*" -print0 | xargs -0 rm -f
