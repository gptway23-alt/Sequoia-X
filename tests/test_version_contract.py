"""源码文件必须作为同一版本整体发布。"""

import main
from sequoia_x.data.engine import ENGINE_API_VERSION


def test_main_and_engine_api_versions_match() -> None:
    assert ENGINE_API_VERSION == main.REQUIRED_ENGINE_API_VERSION == 3
