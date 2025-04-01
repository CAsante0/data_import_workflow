import yaml
import pandas as pd
import ast
import json

def print_formatted_dict(dict_obt : dict, indent = 0) -> None:
    for key, value in dict_obt.items():
        print('  ' * indent + str(key))
        if isinstance(value, dict):
            print_formatted_dict(value, indent + 1)
        else:
            print('  ' * (indent + 1) + str(value))

    
class ConfigValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        super().__init__(", ".join(map(str, errors)))

    def parameter_type_error_check(error_log : list, user_val : str, key : str, rule : str) -> None:
        """
        Compares data type validation (variable.yaml) file against user provided configuration file.

        params:
        error_log : current running error log
        user_val : configuration file input
        key: current configuration file key
        rule: data type constraint in validation (variable.yaml) file

        returns:
        none 

        raises:
        ConfigValidationError : if config data input does not match expected type
        """ 
        if rule == 'boolean' and not isinstance(user_val, bool):
            error_log.append(f"Error on key {key}. Type should be boolean got {type(user_val)}")
        if rule == 'int' and not isinstance(user_val, int):
            error_log.append(f"Error on key {key}. Type should be int got {type(user_val)}")
        if rule == 'str' and not isinstance(user_val, str):
            error_log.append(f"Error on key {key}. Type should be str got {type(user_val)}")
        if rule == 'List[str]' and not isinstance(user_val, list): #not actually checking if they are lists of a specific type
            error_log.append(f"Error on key {key}. Type should be list[str] got {type(user_val)}")
        if rule == 'List[int]' and not isinstance(user_val, list):
            error_log.append(f"Error on key {key}. Type should be list[int] got {type(user_val)}")
        
        if error_log:
            raise ConfigValidationError(error_log)

    def config_shape_validation(error_log : list, validation_config_dict : str, config_dict : str) -> None:
        """
        Error check for mismatch in user provided config file and data validation file

        ###redundant to validate config params

        params:
        error_log : current error log

        raises
        ConfigValidationError : if configuration file values provided by user does not match types outline in validation.yaml
        
        """
        if isinstance(config_dict, dict) and isinstance(validation_config_dict, dict):
            if set(config_dict.keys()) != set(validation_config_dict.keys()):
                raise ConfigValidationError(['There is an error in the format of your datatype_validation_config and the provided config file. Please verify type matches.'])
            return all(ConfigValidationError.config_shape_validation(config_dict[key], validation_config_dict[key]) for key in validation_config_dict)
        
        elif isinstance(config_dict, list) and isinstance(validation_config_dict, list):
            if len(config_dict) != len(validation_config_dict):
                raise ConfigValidationError(['There is an error in the format of your datatype_validation_config and the provided config file. Please verify type matches.'])
            return all(ConfigValidationError.config_shape_validation(user_config, v_config) for user_config, v_config in zip(config_dict, validation_config_dict))


class ParseConfig: 
    """
    Handles parsing and validation configuration and datatype validation files.
    
    """

    def __init__(self, config_path : str, validation_config_path = None):
        """
        Current configuration file entries/.
        """

        self.parameter_args_dict = ParseConfig._parse_configuration_file(config_path)
        self.parameter_types_dict = ParseConfig._parse_configuration_file(validation_config_path) # generates validation file


    def _parse_configuration_file(config_path : str) -> dict:
        """
        Parses config and data type validation (variable.yaml) files. If a config file is not provided the validation (variable.yaml) file 
        will be parsed
        
        returns:
        Dict of config files

        raise:
        ConfigValidationErrors in the case the configuration file or validation file are incorrectly formatted. 
        """ 

        try:  
            with open(config_path, 'r') as file:
                config_data_dict = yaml.safe_load(file)
            return config_data_dict
        
        except FileNotFoundError as e:
            raise ConfigValidationError([f'There is an issue with the location of the {config_path}: {e}'])
        except json.JSONDecodeError as e:
            raise ConfigValidationError([f'There was an error with the format of your {config_path}: {e}'])
        



    def validate_config_params(self) -> bool:
        """
        Compares config.yaml against expected data types provided in variable_constraints.yaml to ensure user given parameters are formatted correctly.

        params:
        self: ParseConfig object

        returns:
        bool : True if all user provided configuration file data types are valid.

        raises:
        ConfigValidationError : for malformed configuration files

        """

        error_log = []

        flattened_param_types = pd.json_normalize(self.parameter_types_dict)
        flattened_param_args = pd.json_normalize(self.parameter_args_dict)

        ConfigValidationError.config_shape_validation(error_log, flattened_param_args, flattened_param_types )

        for key in flattened_param_args.columns:

            config_args = flattened_param_args[key]
            config_type_rule = flattened_param_types[key]

            if config_args.apply(lambda x: isinstance(x, list)).any():
                for dataset_entry, dataset_entry_type in zip(config_args.iloc[0], config_type_rule.iloc[0]): #assuming the entry is a dictionary, needs more explicity error handling 
                    for k in dataset_entry.keys():
                        ConfigValidationError.parameter_type_error_check(error_log, dataset_entry.get(k), k, dataset_entry_type.get(k))
            
        
        return True
            
            









