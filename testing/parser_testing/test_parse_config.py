import pytest
import json
import sys
import os 

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..\\..\\")))
from util.parse_config import ParseConfig



def set_griddap_config():
    return ParseConfig(config_path = "./case1/erddap-tabledap-config.yaml", validation_config_path = "./case1/variable.yaml")


def test_griddap_config_parsing():
    """
    Check whether the configuration file and data type config file have the correct shape
    """
    pc_obj = set_griddap_config()
    assert isinstance(pc_obj.parameter_args_dict, dict)
    assert isinstance(pc_obj.parameter_types_dict, dict)


def test_config_file():
    """
    Note: this must be updated with every new config version
    Checks whether the configuration file and the associated validation config  has been correctly defined
    """
    pc_obj = set_griddap_config().parameter_args_dict
    pc_val_obj = set_griddap_config().parameter_types_dict

    assert "server" in pc_obj and "server" in pc_val_obj
    assert "url" in pc_obj['server'] and "url" in pc_val_obj['server']
    assert "datasets" in pc_obj and "datasets" in pc_val_obj
    assert "name" in pc_obj["datasets"] and "name" in pc_val_obj["datasets"]
    assert "query" in pc_obj and "query" in pc_val_obj
    assert "output_format" in pc_obj['query'] and "output_format" in pc_obj["query"]



def test_malformed_config():
    """
    Test the validate_config_params function
    """

    parse_config = set_griddap_config()

    assert parse_config.validate_config_params() == True



