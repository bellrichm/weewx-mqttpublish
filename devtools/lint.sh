#! /bin/bash
#
#    Copyright (c) 2025 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#
source ./devtools/python_versions.sh

export PYENV_VERSION=$weewx_default_python_version
PYTHONPATH=bin:../weewx/src python -m pylint bin/user/tests/unit/*.py
PYTHONPATH=bin:../weewx/src python -m pylint bin/user/tests/func/*.py
PYTHONPATH=bin:../weewx/src python -m pylint bin/user/mqttpublish.py
