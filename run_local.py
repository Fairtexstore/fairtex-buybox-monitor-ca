from dotenv import load_dotenv
load_dotenv()

import sys
sys.path.insert(0, '.')
from src.monitor import main
main()