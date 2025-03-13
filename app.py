import os
from util.parse_config import ParseConfig 
from util.data_transfer import ImportDatasets, ERDDAP



def config_file_handler(filename : str) -> dict:
    """
    Function for handling all configuration parsing 

    params: 
    filename : the relative path of the configuration file

    returns
    param_dict : a dictionary of the parsed configuration.yaml
    """
    param_dict = ParseConfig.parse_configuration_file(config_name=filename)
    param_type_dict = ParseConfig.parse_configuration_file()

    config_obj = ParseConfig(parameter_args_dict = param_dict, parameter_types_dict = param_type_dict)

    if config_obj.validate_config_params(): 
        return param_dict
    

def fetch_dataset(param_dict):

    pass


def main():


### Format and Configure Parameters, source platforms, and target endpoints ###

    # generate parameter dictionary from config file and validate entries
    param_dict = config_file_handler('C:\\Users\\christina.asante\\Desktop\\erddap\\opendap\\opendap_config.yaml') 


    # extract parameter values into their own dictionaries 
    dataset_params = param_dict['datasets'] # datasets to search for 
    server_params = param_dict['server'] # source/target endpoints 
    query_params = param_dict['query'] # file details

    #preprocess_params = param_dict['data_processing'] #not that important rn

    print(f"dataset_params: {dataset_params}")
    print(f"server_params: {server_params}")
    print(f"query_params: {query_params}")

    import_ds_obj = ERDDAP()
    import_ds_obj.format_erddap_url(dataset_params['name'],  dataset_params['variables'][0]['name'], dataset_params['variables'][0]['range'], query_params['output_format'], server_params)
    import_ds_obj.fetch_dataset()




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