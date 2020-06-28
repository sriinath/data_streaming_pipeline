import os
import sys
my_path = os.getcwd()
sys.path.insert(0, os.path.join(my_path, '..'))

from processes.default_processor import default_processor
from processes.internal_processor import internal_processor

if __name__ == "__main__":
    print('Starting first process')
    default_processor()
    print('starting second process')
    internal_processor()
