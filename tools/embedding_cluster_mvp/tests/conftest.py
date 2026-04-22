"""Pytest 配置：让 ``tools/embedding_cluster_mvp/pipeline_v2`` 可以被导入。

运行方式：

.. code-block:: bash

    cd tools/embedding_cluster_mvp
    python -m pytest tests/

或从仓库根：

.. code-block:: bash

    python -m pytest tools/embedding_cluster_mvp/tests/
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MVP_ROOT = _HERE.parent  # tools/embedding_cluster_mvp
if str(_MVP_ROOT) not in sys.path:
    sys.path.insert(0, str(_MVP_ROOT))
