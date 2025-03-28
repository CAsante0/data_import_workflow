import pytest
import json
import sys
import os 

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..\\..\\")))
from util.parse_config import ParseConfig


def test_config_shape():
    """
    Check whether the configuration file and data type config file have the correct shape
    """

    pass



def test_missing_config():
    """
    Checks whether the configuration file path is correctly referenced
    """
    pass



def test_malformed_config():
    """
    Check whether the configuration file is valid
    """
    pass



