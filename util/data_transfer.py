
import urllib
import xarray as xr
import pandas as pd
from datetime import date, datetime
#import erddapy
import requests
from io import BytesIO
from abc import ABC, abstractmethod
#from authentication import Authentication



class ImportDatasets():
    def __init__(self, source :str = None, dap_type : str = None ):
        self.source_url = source
        #super().__init__(source_url)
        #uncomment this when transfer resources is initialized

    @abstractmethod
    def fetch_dataset(self, dataset, parameters, constraints):
        '''
        Fetches the dataset for the given resource
        '''
    #@abstractmethod
    def derive_resource_parameters(self, **kwargs):
        '''
        Creates a configuration file based on the manual url of the resource
        '''

class ERDDAP(ImportDatasets):


    def __init__(self, base_erddap_url : str, dataset_id : str, dap_type : str, file_type: str,  param_variables : list[str], constraint_variables : list[str]):
        """
        ERDDAP Data Source Object defines componenets of the source server URL

        base_erddap_url : The base URL of the source server
        dataset_id : The ID of the dataset
        dap_type : The dap type of the erddap resource (tabledap or griddap)
        file_type : The file type of the response
        param_variables : The parameters (limited to 1 for griddap queries)
        constrain_variables : A dictionary of constraint variables along with a range represented as a list ex: { 'latitude' : [0, 90]}
        """
        self.base_erddap_url = base_erddap_url
        self.dataset_id = dataset_id
        self.file_type = file_type
        self.dap_type = dap_type

        self.param_variables = param_variables
        self.constraint_variables = constraint_variables

        self.server_url = self.format_erddap_url()
        self.dataset = None



    def fetch_dataset(self, file_type: str, stream : bool = True, manual_source_url : str = None):
        """
        Fetch the resource given the user-defined parameters from the configuration file
        
        args:
        ERDDAP object with source initialized
        source_url (optional) : a manually generated URL
        """
        source = self.server_url
        if manual_source_url:
            source = manual_source_url
        encoded_source = urllib.parse.quote(source, safe=":/?&=()")
        
        try: 
            response = requests.get(encoded_source, stream = stream)
            response.raise_for_status()

            #add cases for opening different filetypes here
            if file_type == "csv":
                if stream:
                    self.set_dataset(pd.read_csv(BytesIO(response.content)))
                    return pd.read_csv(BytesIO(response.content)), response.content
                self.set_dataset(pd.read_csv(BytesIO(response.content)))
                return pd.read_csv(BytesIO(response.content)), response.content
            if file_type == 'nc':
                if stream:
                    self.set_dataset(xr.open_dataset(BytesIO(response.content)))
                    return xr.open_dataset(BytesIO(response.content)), response.content
                self.set_dataset(xr.open_dataset(BytesIO(response.content)))
                return xr.open_dataset(BytesIO(response.content)), response.content
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

   
            if dimension in dimension_constraint_dict:
            
                additional_query = ERDDAP._dimension_assignment(key = dimension, 
                                                                value = dimension_constraint_dict[dimension], 
                                                                dap_type = dap_type)
        
            
            constraint_query_string = constraint_query_string + additional_query

        return constraint_query_string #there is one consistent constraint string for tabledap variables and one per value in griddap
    

    @staticmethod #doesnt do anything 
    def _format_erddap_gridded_url(constraints: str, griddap_variables : list) -> str:
        """
        Formats a url based on erddap gridded datasets
        
        params:
        constraints: formatted gridded erddap time,lat,long 
        variables: a list of variables included in the final dataset (ex: chlor_a, salinity, etc)
        
        returns:
        formatted query string combining variables with user defined constraints
        """
        query = ""
        for var in griddap_variables:
            query = query + var + constraints

        return query


    @staticmethod # doesnt do anything until 
    def _format_erddap_tabledap_url(constraints: str, tabledap_variables : list) -> str:
        pass

    def compose_erddap_base_url(base_erddap_url : str, dataset_id : str, dap_type : str, file_type : str, constraint_extension: str) -> str:
         
        #return f"{server_url}/erddap/{dap_type}/{dataset_id}.{file_type}?"
        return f"{base_erddap_url}/erddap/{dap_type}/{dataset_id}.{file_type}?{constraint_extension}"

    def set_dataset(self, dataset) -> None:
        self.dataset = dataset

    def set_parameter_variables(self, param_variables : list) -> None:
        self.param_variables = param_variables

    def set_constraint_variables(self, constraint_variables : dict) -> None:
        self.constraint_variables = constraint_variables
        pass


    def format_erddap_url(self, dataset_id : str = None, field_variables : list = None, constraint_variables: dict = None,file_type : str = None, base_url: str = None, dap_type : str = None) -> str:
        """
        Formats an ERDDAP compliant URL string 
        
        params:
        dataset_id : The unique ID of the erddap dataset
        field_variables : Parameters within the final dataset (chlor_a, degrees, count, etc)
        constraint_variables : A dictionary containing variables along with their range constraints
        file_type : The file type of the returned dataset
        base_url : The ERDDAP base url
        dap_type : The dap type of the target resource (gridded or tabledap)
        
        returns:
        ERDDAP formatted URL
        """
        full_constraint_query = ""
        
        # Formatting constraints
        formatted_constraints = ERDDAP._format_constraints( 
            dimension_constraint_dict=self.constraint_variables, 
            dap_type= self.dap_type)

        # Compose final constraint extension given the dap_type
        if self.dap_type == "griddap":
            full_constraint_query = f"{formatted_constraints}".join(map(str, self.param_variables)) + formatted_constraints # to put into format_erddap_gridded function
        if self.dap_type == 'tabledap':
            full_constraint_query = ",".join(map(str, self.param_variables)) + formatted_constraints
       
        full_url = ERDDAP.compose_erddap_base_url(
                        base_erddap_url = self.base_erddap_url,
                        dataset_id = self.dataset_id,
                        dap_type = self.dap_type,
                        file_type = self.file_type,
                        constraint_extension = full_constraint_query
        )
        return full_url

    def set_manual_source_url(self, url : str = None) -> None:
        self.source_url = url

class OPENDAP(ImportDatasets):

    # copy in structure 

    pass



class ExportDatasets(ImportDatasets):
    def __init__(self, endpoint):
        super().__init__(endpoint)




