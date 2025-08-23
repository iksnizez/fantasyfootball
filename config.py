# config.py
from pathlib import Path
from dotenv import load_dotenv
import os, sys

def get_project_root():
    """
    Returns the project root regardless of whether we're in a script,
    an interactive shell, or Jupyter Notebook.
    """
    try:
        # Running from a file on disk
        return Path(__file__).resolve().parent   # the number of parents should be how many levels below the root the config is saved
    except NameError:
        # Interactive mode (IPython, Jupyter, or plain Python shell)
        return Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()

PROJECT_ROOT = get_project_root()

# Load environment variables without overwriting existing ones
ENV_PATH = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=ENV_PATH, override=False)
ESPN_COOKIES_SWID = os.environ.get('ESPN_COOKIES_SWID')
ESPN_COOKIES_S2 = os.environ.get('ESPN_COOKIES_S2')
ESPN_HEADERS_NAME=os.environ.get('ESPN_HEADERS_NAME')
ESPN_HEADERS=os.environ.get('ESPN_HEADERS')
ESPN_LEAGUE_ID = os.environ.get('ESPN_LEAGUE_ID')
PYMYSQL_NFL = os.environ.get('PYMYSQL_NFL')

# config data 
DATA_DIR = PROJECT_ROOT / 'data'
BROWSER_DIR = PROJECT_ROOT / 'browsers'
MODULES_DIR = PROJECT_ROOT / 'modules'

# add modules to the temp system path so notebooks and scripts can easily import
if MODULES_DIR not in sys.path:
    sys.path.append(MODULES_DIR)



