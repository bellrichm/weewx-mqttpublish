#! /bin/bash
#
#    Copyright (c) 2025 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#

./devtools/functests.sh

while inotifywait -e modify devtools/watchfunctests.sh devtools/functests.sh bin/user/mqttpublish.py bin/user/tests/func
do
    ./devtools/functests.sh $WEEWX $PY_VERSION
done