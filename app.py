import os
from util.parse_config import ParseConfig, ConfigValidationError
from util.data_transfer import ImportDatasets, ERDDAP
import sys



def config_file_handler(filename : str, validation_config : str = None) -> dict:
    """
    Function for handling all configuration parsing 
    params: 
    filename : the relative path of the configuration file

    returns
    param_dict : a dictionary of the parsed configuration.yaml
    """
    config_arg_object = ParseConfig(config_path=filename, validation_config_path=validation_config)
    if config_arg_object.validate_config_params() == True:
        return config_arg_object.parameter_args_dict
    
    raise print("Your configuration file is ")



def main():

    source_config = sys.argv[1]
    validation_config = sys.argv[2]

    param_dict = config_file_handler(filename = source_config, validation_config = validation_config) 

    # Setting parameter dictionaries
    dataset_params = param_dict['datasets'] # datasets to search for 
    server_params = param_dict['server'] # source/target endpoints 
    query_params = param_dict['query'] # file details
    
    print(f"dataset_params: {dataset_params}")
    print(f"server_params: {server_params}")
    print(f"query_params: {query_params}")
    
    '''
    Initialize an ERDDAP Import Workflow 

    Parses the User Provided configuration files
    Creates an ERDDAP Object
    Fetches the target resource and saves it to the ERDDAP Object
    
    '''


    import_ds_obj = ERDDAP(
            base_erddap_url=server_params['url'],
            dataset_id=dataset_params['name'],
            dap_type=server_params['dap_type'],
            file_type=query_params['output_format'],
            param_variables= dataset_params['variables'][0]['name'],
            constraint_variables=dataset_params['variables'][0]['range']
    )
    #print(f"Server url: {import_ds_obj.server_url}")
    dataset, response = import_ds_obj.fetch_dataset(query_params['output_format'], stream=False)
    #print(f"{import_ds_obj.dataset}, {response}")
    '''
    Uncomment below to write the response to the application output directory.

    with open(f'response.{query_params['output_format']}', "wb") as output_file:
        output_file.write(response)

    '''


if __name__ == "__main__":
    main()