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

    def parameter_type_error_check(error_log : list, user_val : str, key : str, rule : str):
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


class ParseConfig: 

    def __init__(self, parameter_args_dict, parameter_types_dict):
        """
        Current configuration file entries/.
        """
        self.parameter_args_dict = parameter_args_dict
        self.parameter_types_dict = parameter_types_dict




    def parse_configuration_file(config_name = None) -> dict:
        """
        Parses config and data type validation (variable.yaml) files. If a config file is not provided the validation (variable.yaml) file 
        will be parsed
        
        returns:
        Dict of config files 
        """ 
        if(config_name):  
            with open(config_name, 'r') as file:
                config_data = yaml.safe_load(file)
            #self.parameter_args_dict = config_data
            print_formatted_dict(config_data)
            #print(config_data)
            return config_data
        
        with open('C:\\Users\\christina.asante\\Desktop\\erddap\\opendap\\variable.yaml', 'r') as file:
            parameter_type_assignments = yaml.safe_load(file)
        return parameter_type_assignments



    def validate_config_params(self) -> bool:
        """
        Compares config.yaml against expected data types provided in variable_constraints.yaml to ensure user given parameters are formatted correctly.

        params:
        self: ParseConfig object

        returns:
        bool : True if all user provided configuration file data types are valid.

        """

        error_log = []

        flattened_param_types = pd.json_normalize(self.parameter_types_dict)
        flattened_param_args = pd.json_normalize(self.parameter_args_dict)

        for key in flattened_param_args.columns:

            config_args = flattened_param_args[key]
            config_type_rule = flattened_param_types[key]

            if config_args.apply(lambda x: isinstance(x, list)).any():
                for dataset_entry, dataset_entry_type in zip(config_args.iloc[0], config_type_rule.iloc[0]): #assuming the entry is a dictionary, needs more explicity error handling 
                    for k in dataset_entry.keys():
                        ConfigValidationError.parameter_type_error_check(error_log, dataset_entry.get(k), k, dataset_entry_type.get(k))
        return True
            


            
            









