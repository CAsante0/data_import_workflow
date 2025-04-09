import os
import re
import time
import yaml
import pandas as pd
import ast
import json
import sys 
from urllib.parse import unquote
from datetime import datetime
import ruamel.yaml
import yaml

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
            
            
    def parse_erddap_url() -> None:
        """
        Populates configuration file based on existing URL
        
        """
        url = sys.argv[1]
        
        query_url = url.split("?")[-1]
        base_url_list = url.split("/")[0:-1]
        dataset_name = url.split("/")[-1]
        dataset_name = dataset_name.split("?")[0]
        base_url = "/".join(base_url_list)

        print(base_url_list)
        print(dataset_name)
        print(base_url)
        print(query_url)

        dap_type = 0

        if 'tabledap' in base_url:
            dap_type = 'tabledap'
            dataset_params = ParseConfig.extract_erddap_table_params(query_url)
        elif 'griddap' in base_url:
            dap_type = 'griddap'
            dataset_params = ParseConfig.extract_erddap_grid_params(query_url)

        return {
                'base_url' : base_url,
                'dap_type' : dap_type,
                'dataset_id' : dataset_name,
                'full_url' : url,
                'constraints' : dataset_params
        }

            

    def _flow_list(x):
        retval = ruamel.yaml.comments.CommentedSeq(x)
        retval.fa.set_flow_style()  # fa -> format attribute
        return retval
        
    def populate_config(args : dict) -> None:

        with open('../erddap-griddap-config.yaml', 'r') as file:
            data = yaml.safe_load(file)
        print(data)

        repl_val = ""
        if '.csv' in args['dataset_id']:
            repl_val = '.csv'
        if '.nc' in args['dataset_id']:
            repl_val = '.nc'

        data['server']['url'] = args['base_url']
        data['server']['dap_type'] = args['dap_type']
        data['datasets']['name'] = args['dataset_id'].replace(repl_val, "")
        data['datasets']['dataset_url'] = args['full_url']
        # dataset constraints
        data['datasets']['variables'][0]['name'] = ParseConfig._flow_list(args['constraints']['dataset_variables'])
        data['datasets']['variables'][0]['dimensions'] = ParseConfig._flow_list(['time', 'latitude', 'longitude'])
        data['datasets']['variables'][0]['range']['time'] = ParseConfig._flow_list(args['constraints']['time'])
        data['datasets']['variables'][0]['range']['lat'] = ParseConfig._flow_list(args['constraints']['latitude'])
        data['datasets']['variables'][0]['range']['long'] = ParseConfig._flow_list(args['constraints']['longitude'])

        if args['dap_type'] == 'griddap':
            data['query']['output_format'] = "nc"
        else:
            data['query']['output_format'] = "csv"

        yml = ruamel.yaml.YAML()


        with open(f'../output/erddap-{args['dap_type']}-config-{time.strftime("%Y-%m-%d", time.localtime())}.yaml', 'w') as new_file:
            yml.dump(data, new_file)
            
    
    def extract_erddap_table_params(query : str) -> None:
        """
        Extrapolates config parameters to manually generate config given an erddap tabledap URL

        query : The parameter extensions of the ERDDAP URL

        returns:
        None
        """
        # NOTE: Currently does not return dataset variables
        params = query.split("&")[0]
        ranges = query.split("&")[1:]

        range_dict = {}
        for r in ranges:

            r = unquote(r)
            current_param = r.split('<>=<=>=')[0]
            print(current_param)
            range_val = 0
            if '>=' in r:
               range_val = r.split(">=")[1]
               current_param = r.split(">=")[0]
            elif '<=' in r:
               range_val = r.split("<=")[1]
               current_param = r.split("<=")[0]
            elif '=' in r:
               range_val = r.split("=")[1]
               current_param = r.split("=")[0]


            if current_param in range_dict:
                if current_param == 'latitude':
                    range_val = float(range_val)
                    if range_dict['latitude'][0] >= range_val:
                        range_dict[current_param].append(range_val)
                    else:
                        range_dict[current_param].insert(0,range_val)
                if current_param == 'time':
                    print(f"this is the current time{range_dict[current_param]}")
                    print(f"this is the current compared time: {range_val}") 
                    #if datetime.strptime(range_dict[current_param][0], "%Y-%m-%dT%H:%M:%SZ") >= datetime.strptime(range_val, "%Y-%m-%dT%H:%M:%SZ"):
                    if datetime.strptime(range_dict[current_param][0], "%Y-%m-%d") >= datetime.strptime(range_val, "%Y-%m-%dT%H:%M:%SZ"):
                    
                        date = datetime.strptime(range_val, "%Y-%m-%dT%H:%M:%SZ")
                        range_val = date.strftime("%Y-%m-%d")
                        print(f"this is the range_val: {range_val}")

                        range_dict[current_param].insert(0, range_val)
                    else: 
                        date = datetime.strptime(range_val, "%Y-%m-%dT%H:%M:%SZ")
                        range_val = date.strftime("%Y-%m-%d")
                        print(f"this is the range_val: {range_val}")
                        range_dict[current_param].append(range_val)
                if current_param == 'longitude':
                    range_val = float(range_val)
                    if range_dict[current_param][0] >= range_val:
                        range_dict[current_param].insert(0, range_val)
                    else:
                        range_dict[current_param].append(range_val)
            else:
                if current_param == 'time':
                    date = datetime.strptime(range_val, "%Y-%m-%dT%H:%M:%SZ")
                    range_val = date.strftime("%Y-%m-%d")
                elif current_param == 'latitude' or current_param == 'longitude':
                    range_val = float(range_val)
    

                range_dict[current_param] = [range_val]

        range_dict['dataset_variables'] = unquote(params).split(',')
        print(range_dict)
        return range_dict


    def extract_erddap_grid_params(query : str) -> None:
        """
        Extrapolates config parameters to manually generate config given an erddap griddap URL

        query : The griddap compatible parameter extensions of the ERDDAP URL

        returns:
        None
        """
     
        query = unquote(query)
        
        vars = re.match(r'^[^\[]+', query).group()
        constraints = re.findall(r'\[([^\]]+)\]', query)

        constraint_pairs = []
        constraint_list = []

        for x in constraints:
            constraint_list = re.findall(r'\((.*?)\)', x)
            if len(constraint_list) == 2:
                constraint_pairs.append(constraint_list)


        # this will always be ordered as time, altitude, latitude, longitude

        param_dict = dict(zip(['time', 'altitude', 'latitude', 'longitude'], constraint_pairs))
        param_dict['dataset_variables'] = vars
        print(param_dict)
        return param_dict







        








if __name__ == '__main__':
    x = ParseConfig.parse_erddap_url()
    ParseConfig.populate_config(x)

    