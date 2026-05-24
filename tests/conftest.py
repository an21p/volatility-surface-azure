import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  # so `import utils` works


def load_module(relpath, name):
    """Load a single module file directly, bypassing package __init__."""
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(ROOT, relpath))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
