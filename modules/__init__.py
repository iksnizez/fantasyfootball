from pathlib import Path
import sys
import importlib

# Ensure the modules folder itself is in sys.path
MODULES_PATH = Path(__file__).parent
if str(MODULES_PATH) not in sys.path:
    sys.path.append(str(MODULES_PATH))

# Dynamically import all .py files in this folder (except __init__.py)
for py_file in MODULES_PATH.glob("*.py"):
    if py_file.name != "__init__.py":
        module_name = py_file.stem
        importlib.import_module(f".{module_name}", package=__name__)