# config.py
from pathlib import Path
import sys

def get_project_root():
    """
    Returns the project root regardless of whether we're in a script,
    an interactive shell, or Jupyter Notebook.
    """
    # Case 1: Running from a file on disk
    if '__file__' in globals():
        return Path(__file__).resolve().parent.parent

    # Case 2: Interactive mode or Jupyter
    if hasattr(sys, 'ps1') or sys.argv[0].endswith(('ipython', 'jupyter-notebook', 'jupyter-lab')):
        return Path.cwd()

    # Fallback: assume current working directory
    return Path.cwd()

PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUT_DIR = PROJECT_ROOT / 'output'

# Example usage
if __name__ == '__main__':
    print("Project root:", PROJECT_ROOT)
    print("Data dir:", DATA_DIR)
    print("Output dir:", OUTPUT_DIR)
