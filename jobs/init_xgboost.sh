#!/bin/bash
set -euxo pipefail

if [ -x /opt/conda/default/bin/python ]; then
  PYTHON_BIN=/opt/conda/default/bin/python
else
  PYTHON_BIN=python3
fi

WHEELHOUSE_DIR=/tmp/data228-wheelhouse
mkdir -p "${WHEELHOUSE_DIR}"
gsutil -m cp "gs://sparkforgroup-data228-taxi/jobs/wheelhouse/"'*.whl' "${WHEELHOUSE_DIR}/"

"${PYTHON_BIN}" -m pip install --no-index --no-deps "${WHEELHOUSE_DIR}"/xgboost-2.1.4-*.whl

"${PYTHON_BIN}" - <<'PY'
import importlib.util

missing = [
    package
    for package in ("xgboost", "pandas", "pyarrow")
    if importlib.util.find_spec(package) is None
]
if missing:
    raise RuntimeError("Missing Python package(s): {}".format(", ".join(missing)))
PY
