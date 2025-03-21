import pydap
import urllib
import xarray as xr
import pandas as pd
from datetime import date, datetime
#import erddapy
import requests
from io import BytesIO
#from authentication import Authentication



class ImportDatasets():
    def __init__(self, source :str = None, dap_type : str = None ):
        self.source_url = source
        #super().__init__(source_url)
        #uncomment this when transfer resources is initialized

    def fetch_dataset(self, dataset, parameters, constraints):
        pass


class ERDDAP(ImportDatasets):


    def test__init__(self, server_url : str, dataset_id : str, param_variables : list[str], dap_type : str, file_type: str, constraint_variables : list[str]):
        """
        
        ######Draft of the eventual erddap object 
        
        
        """
        self.server_url = server_url
        self.dataset_id = dataset_id
        self.param_variables = param_variables
        self.constraint_variables = constraint_variables
        self.compose_source_endpoint_url = ERDDAP.compose_erddap_base_url(server_url, dap_type, dataset_id, file_type)


    def fetch_dataset(self, file_type: str, stream : bool = False, manual_source_url : str = None):
        """
        Fetch the resource given the user-defined parameters from the configuration file

        ###this can eventually be moved to the import datsets class when opendap and globus api are both integrated
        ###what additional filetypes should be supplied?
        
        args:
        ERDDAP object with source initialized
        source_url (optional) : a manually generated URL
        """
        source = self.source_url
        if manual_source_url: # add handling for different filetypes
            source = manual_source_url
        encoded_source = urllib.parse.quote(source, safe=":/?&=()")
        try: 
            response = requests.get(encoded_source, stream = True)
            response.raise_for_status()

            #add cases for opening different filetypes here
            if file_type == "csv":
                if stream:
                    return pd.read_csv(BytesIO(response.content))
                return pd.read_csv(BytesIO(response.content))
            if file_type == 'nc':
                if stream:
                    return xr.open_dataset(BytesIO(response.content))
                xr.open_dataset(encoded_source)
        except requests.exceptions.HTTPError as err: 
            if response.status_code == 500:
                print(f"Error Code 500: Internal server error. Server response: {response.content}")
            elif response.status_code == 404:
                print(f"Error Code 404: HTTP Error, check that all inputs were formatted correctly. Server response: {response.content}")
            elif response.status_code == 400: 
                print(f"Error Code 400: HTTP Error, check that all inputs were formatted correctly. Server response: {response.content}")
        except requests.exceptions.RequestException as err:
            print(f"Request error code: {err}")
        return False

    @staticmethod
    def _dimension_assignment(key : str, value : list, dap_type :str) -> str:
        """
        Given a config file key and its associated value, formats it into the appropriate ERDDAP compliant constraint
        
        params:
        key : config key
        value : a list of list[2] constraints 
        dap_type : gridded or table
        
        returns:
        formatted constraint string
        """

        print(key)
        print(value)
        datetime_format = "%Y-%m-%d"
        # time
        if key == 'time':
            earliest_date, latest_date = 0,0 
            x_date = datetime.strptime(value[0], datetime_format)
            y_date = datetime.strptime(value[1], datetime_format)

            x_date = x_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            y_date = y_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            if x_date < y_date:
                earliest_date = x_date
                latest_date = y_date
            else:
                earliest_date = y_date
                latest_date = x_date

            if dap_type == "griddap":
                return f"[({earliest_date}):1:({latest_date})]"
            else:
                return f"&time>={earliest_date}&time<={latest_date}"
        # altitude -- always 0,0
        if key == 'altitude':
            print(f"these are the values: {value}")
            if dap_type == "griddap":
                return f"[({value[0]}):1:({value[1]})]"
            if dap_type == "tabledap":
                return ""
        # latitude 
        if key == 'lat' or key == 'latitude':
            max_lat, min_lat = min(value), max(value) #latitude decrease from top to bottom, max_lat >> smaller number, min_lat >> greater number
            if dap_type == "griddap":
                return f"[({min_lat}):1:({max_lat})]"
            else:
                return f"&latitude>={max_lat}&latitude<={min_lat}"
        # longitude
        if key == 'lon' or key == 'longitude' or key == 'long':
            min_long, max_long = min(value), max(value)
            if dap_type == "griddap":
                return f"[({min_long}):1:({max_long})]"
            else:
                return f"&longitude>={min_long}&longitude<={max_long}"

    
    @staticmethod
    def _format_constraints(dimension_constraint_dict : dict, dap_type : str) -> list: 
        """
        Format configuration file variable constraints (i.e. latitude, longitude, time, etc) 
        into erddap compatible query standards.

        params:
        dimension_constraint_dict : dictionary of dataset[variables][range] section (dimension variables: time, lat, long)

        returns: 
        list[list1, list2]
        list1: constraints name [time, lat, long]
        list2: range limitations for complimentary constraint [">YYYY-MM-ddTHH:mm:ss", (0,90), (-180, 180)]
        """
        constraint_query_string = ""

        griddap_constraint_order = ['time', 'altitude', 'latitude', 'longitude']

        
        dimension_constraint_dict['altitude'] = [0.0,0.0] # will always default to 0,0
        
        if 'lat' in dimension_constraint_dict:
            dimension_constraint_dict['latitude'] = dimension_constraint_dict.pop('lat')
        if 'long' in dimension_constraint_dict:
            dimension_constraint_dict['longitude'] = dimension_constraint_dict.pop('long')
        if 'lon' in dimension_constraint_dict:
            dimension_constraint_dict['longitude'] = dimension_constraint_dict.pop('lon')
            

        #for dimension, constraint in dimension_constraint_dict.items():
        for dimension in griddap_constraint_order:

            print(f"the dim: {dimension}")
            if dimension in dimension_constraint_dict:
            
                additional_query = ERDDAP._dimension_assignment(key = dimension, 
                                                                value = dimension_constraint_dict[dimension], 
                                                                dap_type = dap_type)
                print(additional_query)
            
            constraint_query_string = constraint_query_string + additional_query

        return constraint_query_string #there is one consistent constraint string for tabledap variables and one per value in griddap
    

    @staticmethod #doesnt do anything until tabledap and griddap are separated use format erddap url
    def _format_erddap_gridded_url(constraints: str, gridap_variables : list) -> str:
        """
        Formats a url based on erddap gridded datasets
        
        params:
        constraints: formatted gridded erddap time,lat,long 
        variables: a list of variables included in the final dataset (ex: chlor_a, salinity, etc)
        
        returns:
        formatted query string combining variables with user defined constraints
        """
        query = ""
        for var in gridap_variables:
            query = query + var + constraints

        # check variables against compliance from griddap accepted in config validation

        return query

    def compose_erddap_base_url(server_url : str, dap_type : str, dataset_id : str, file_type : str) -> str:
         
        return f"{server_url}/erddap/{dap_type}/{dataset_id}.{file_type}?"

    def format_erddap_url(self, dataset_id : str, field_variables : list, constraint_variables: dict,file_type : str, base_url: str, dap_type : str) -> None:
        """
        ##eventually all params should come from an erddap object
        Formats an ERDDAP compliant URL string 
        
        params:
        dataset : dictionary containing expected dataset id, variables, and dimension constraints
        file_type : contains format for output file
        source_details : dictionary containing source url details
        
        returns:
        ERDDAP formatted URL
        """
        # Formatting base url 
        base_formatted_url = ERDDAP.compose_erddap_base_url(server_url = base_url, dap_type = dap_type, dataset_id = dataset_id, file_type = file_type)
        full_constraint_query = ""
        
        # Formatting constraints
        formatted_constraints = ERDDAP._format_constraints( dimension_constraint_dict=constraint_variables, dap_type= dap_type)

    
        # Compose final URL
        if dap_type == "griddap":
            #_format_erddap_gridded_url
            full_constraint_query = f"{formatted_constraints}".join(map(str, field_variables)) + formatted_constraints # to put into format_erddap_gridded function
        if dap_type == 'tabledap':
            #_format_erddap_tabledap_url
            print(f"these are the field variables: {field_variables}")
            full_constraint_query = ",".join(map(str, field_variables)) + formatted_constraints
       
        full_url = base_formatted_url+full_constraint_query

        self.source_url = full_url
        print(f"this is the full url: {full_url}")
        return full_url

    def set_manual_source_url(self, url : str = None) -> None:
        self.source_url = url

class OPENDAP(ImportDatasets):

    # copy in structure 

    pass



class ExportDatasets(ImportDatasets):
    def __init__(self, endpoint):
        super().__init__(endpoint)




