import os
from util.parse_config import ParseConfig 



def config_file_handler(filename):

    # pc object containing file or data?
    
    param_dict = ParseConfig.parse_configuration_file(config_name=filename) #fix this
    param_type_dict = ParseConfig.read_param_type_assignment()

    config_obj = ParseConfig(parameter_args_dict = param_dict, parameter_types_dict = param_type_dict)

    config_obj.validate_config_params()  # more explicit error handling?
    
    return param_dict



def main():


### Format and Configure Parameters, source platforms, and target endpoints ###


    #######use openddap_config.yaml by default, in the future location of config should be passed as an argument via commandline
    
    # generate parameter dictionary from config file
    param_dict = config_file_handler('C:\\Users\\christina.asante\\Desktop\\erddap\\opendap\\opendap_config.yaml') 

    # extract parameter values into their own dictionaries 
    dataset_params = param_dict['datasets'] # datasets to search for 
    server_params = param_dict['server'] # source/target endpoints 
    format_params = param_dict['query'] # file details

    preprocess_params = param_dict['data_processing'] #not that important rn
    # configure target server: format url

    #source_opendap = server_params('')


    # configure endpoint server


### Data Retrieval ###
    # Format parameters for URL
    # Pull data from endpoint specific in server_params dict

### Preprocess Data ### (optional)

    # subset/augment data based on input parameters
    # qc on metadata 

### Import Formatted Dataset to Target Location ###

    # import datasets into target repositories or staging environments



if __name__ == "__main__":
    main()