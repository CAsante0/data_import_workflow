import yaml


### configuration file error handling ###
class ConfigValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        super.__init__(f"Configuration File errors in: {errors}")

    def parameter_type_error_check(error_log, user_val, key, rule):
        if rule == 'boolean' and not isinstance(user_val, bool):
            error_log.append(f"Error on key {key}. Type should be boolean got {type(user_val)}")
        if rule == 'int' and not isinstance(user_val, int):
            error_log.append(f"Error on key {key}. Type should be int got {type(user_val)}")
        if rule == 'str' and not isinstance(user_val, str):
            error_log.append(f"Error on key {key}. Type should be str got {type(user_val)}")
        if rule == 'List[str]' and not isinstance(user_val, list[str]):
            error_log.append(f"Error on key {key}. Type should be list[str] got {type(user_val)}")
        if rule == 'List[int]' and not isinstance(user_val, list[int]):
            error_log.append(f"Error on key {key}. Type should be list[int] got {type(user_val)}")


class ParseConfig: 

    def __init__(self, parameter_args_dict, parameter_types_dict):
        self.parameter_args_dict = parameter_args_dict
        self.parameter_types_dict = parameter_types_dict

    def parse_configuration_file(config_name):
        with open(config_name, 'r') as file:
            config_data = yaml.safe_load(file)
            print(config_data)
        #self.parameter_args_dict = config_data
        return config_data
    
    def read_param_type_assignment():
        # read the current parameter type(s) in order to compare them with the arguments given by the user
        with open('C:\\Users\\christina.asante\\Desktop\\erddap\\opendap\\variable.yaml', 'r') as file:
            parameter_type_assignments = yaml.safe_load(file)
            print(parameter_type_assignments)

        return parameter_type_assignments
        #self.parameter_types_dict = parameter_type_assignments

    def validate_config_params(self):
        # validate parameter arguments against types outlined in variable.yaml
        ##### need to handle date format types

        error_log = []
        for outer_key in self.parameter_types_dict:
            print(self.parameter_types_dict[outer_key])
            print(f"this is the outer key: {outer_key}")

            config_current_section = self.parameter_types_dict[outer_key]

            if isinstance(config_current_section, list): # dealing with a section presenting as a list
                for current_section_entry_dict in config_current_section:
                    for key, rule in current_section_entry_dict.items():  
                        #iterate over inner section key       
            
                        
            for key, rule in self.parameter_types_dict[outer_key].items(): 
                #user_val = self.parameter_args_dict.get(key)
               
                print(f"this is the user_val: {key}")
               
                if (key =='variables'): # iterating over variables as all variables are stored as a list of dictionaries
                    
                    variables_list_of_dicts = self.parameter_types_dict[outer_key][key] # gets the variables list 
                    variable_list_of_dict_args = self.parameter_args_dict[outer_key][key]
                    
                    for i, a in zip(variables_list_of_dicts, variable_list_of_dict_args): # I know this is very contrived and I will def fix it

                        for type_vals, arg_vals  in zip(i.items(), a.items()): 

                            type_rule = type_vals[1]
                            arg_rule = arg_vals[1]

                            print(type_rule)


                    #for inner_key, inner_rule in self.parameter_types_dict[outer_key][key].items:

               




                                
                #float consideration






                #consider range validation for dates, coordinates, numerical values

        if error_log:
            raise ConfigValidationError(error_log)
        
        return True
            


            
            









