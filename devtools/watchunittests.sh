#! /bin/bash
#
#    Copyright (c) 2025 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#

./devtools/unittests.sh

while inotifywait -e modify devtools/watchunittests.sh devtools/unittests.sh bin/user/mqttpublish.py bin/user/tests/unit
do
    ./devtools/unittests.sh $WEEWX $PY_VERSION
done