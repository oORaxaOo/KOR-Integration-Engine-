import importlib.util
import sys
from pathlib import Path


def test_root_plugin_entrypoint_exposes_register_from_src_package():
    repository_root = Path(__file__).resolve().parents[1]
    source_root = str(repository_root / "src")
    if source_root in sys.path:
        sys.path.remove(source_root)

    module_name = "kor_kie_root_plugin_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        repository_root / "__init__.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
        assert source_root in sys.path
        assert callable(module.register)
        assert module.register.__module__ == "kie.hermes_plugin"
    finally:
        if source_root in sys.path:
            sys.path.remove(source_root)
        sys.modules.pop(module_name, None)
