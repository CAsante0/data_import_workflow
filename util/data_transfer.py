import pydap
import urllib
import xarray as xr
from datetime import date, datetime
#import erddapy
import requests
from io import BytesIO
#from authentication import Authentication



class ImportDatasets():
    def __init__(self, source :str = None, dap_type : str = None ):
        self.source_url = source
        self.dap_type = dap_type
        #super().__init__(source_url)
        #uncomment this when transfer resources is initialized, add in as class arutment

    def fetch_dataset(self, dataset, parameters, constraints):
        pass



class ERDDAP(ImportDatasets):


    def fetch_dataset(self, stream : bool = True, manual_source_url : str = None):
        """
        Fetch the resource given the user-defined parameters from the configuration file
        
        args:
        ERDDAP object with source initialized
        source_url (optional) : a manually generated URL
        """
        source = self.source_url

        if manual_source_url:
            source = manual_source_url

        encoded_source = urllib.parse.quote(source, safe=":/?&=")
        if stream:
            response = requests.get(encoded_source, stream = True)
            response.raise_for_status()
            print(xr.open_dataset(BytesIO(response.content)))
        print(xr.open_dataset(source))


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

            if dap_type == "gridded":
                return f"[({earliest_date}):1:({latest_date})]"
            else:
                return f"&time>={earliest_date}&time<={latest_date}"

            #table_additional_query = f"&time>={earliest_date}&time<={latest_date}"

        if key == 'lat' or key == 'latitude':
            min_lat, max_lat = min(value), max(value)
           # additional_query = f"[{min_lat}:{max_lat}]"
            if dap_type == "gridded":
                return f"[({min_lat}):1:({max_lat})]"
            else:
                return f"&latitude>={min_lat}&latitude<={max_lat}"
            
        if key == 'lon' or key == 'longitude' or key == 'long':
            min_long, max_long = min(value), max(value)
            #additional_query = f"[{min_long}:{max_long}]"
            if dap_type == "gridded":
                return f"[({min_long}):1:({max_long})]"
            else:
                return f"&longitude>={min_long}&longitude<={max_long}"

        # include altitude case
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

        for dimension, constraint in dimension_constraint_dict.items():
            additional_query = ERDDAP._dimension_assignment(dimension, constraint, dap_type)
            constraint_query_string = constraint_query_string + additional_query

        return constraint_query_string
    

    @staticmethod
    def _format_erddap_gridded_url(constraints: str, gridap_variables : list) -> str:
        """
        #####ignore me until you start defining classes for each source
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


    def format_erddap_url(self, dataset_id : str, field_variables : list, constraint_variables: dict,file_type : str, source_details : dict) -> None:
        """
        Formats an ERDDAP compliant URL string 
        
        params:
        dataset : dictionary containing expected dataset id, variables, and dimension constraints
        file_type : contains format for output file
        source_details : dictionary containing source url details
        
        returns:
        ERDDAP formatted URL
        """
        # Formatting base url 
        base_url = source_details['url']
        dap_type = source_details['dap_type']
        base_formatted_url = f"{base_url}/erddap/{dap_type}/{dataset_id}.{file_type}?"
        full_constraint_query = ""
        
        # Formatting constraints
        formatted_constraints = ERDDAP._format_constraints( dimension_constraint_dict=constraint_variables, dap_type= dap_type)

        #print(f"based formatted: {base_formatted_url}")
        #print(f"formatted_results_variables: {results_variables}")
        #print(f"formatted constraints: {formatted_constraints}")
    
        # Compose final URL
        if dap_type == 'gridded':
            full_constraint_query = f"{formatted_constraints}".join(map(str, field_variables)) + formatted_constraints #sloppy, please fix
        if dap_type == 'table':
            full_constraint_query = ",".join(map(str, field_variables)) + formatted_constraints
       
        full_url = base_formatted_url+full_constraint_query

        self.source_url = full_url
        print(f"this is the full url: {full_url}")
        #return full_url

    def set_manual_source_url(self, url : str = None) -> None:
        self.source_url = url

class OPENDAP(ImportDatasets):

    pass



class ExportDatasets(ImportDatasets):
    def __init__(self, endpoint):
        super().__init__(endpoint)




