from requests import HTTPError
import pytest
import json
import os
import sys
import pandas
import xarray

#sys.path.append(os.path.abspath('..'))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..\\..\\")))
from util.data_transfer import ImportDatasets, ERDDAP


def set_tabledap_test():
    return ERDDAP(
        base_erddap_url="https://ncei.noaa.gov",
        dataset_id= 'CRCP_ARMS_Cryptic_Reef_Diversity_Pacific',
        dap_type='tabledap',
        file_type= "csv",
        param_variables= ['Region', 'Island', 'Count'],
        constraint_variables= {'time': ['2016-06-01', '2017-06-20'], 'lat': [14.6, 18.4], 'lon': [-178.37, 166]},
)

def set_griddap_test():
    return ERDDAP(
        base_erddap_url="https://coastwatch.noaa.gov",
        dataset_id="noaacwNPPN20S3ASCIDINEOF2kmDaily",
        dap_type="griddap",
        file_type= "nc",
        param_variables=["chlor_a"],
        constraint_variables= {'time': ['2025-01-09', '2025-03-06'], 'lat': [-67.989586, -80.989586], 'lon': [133.98961, 161.98961]}
    )


def test_erddap_tabledap_url_generation() -> None:
    """
    Tests that erddap urls are being generated correctly
    """

    assert set_tabledap_test().server_url == "https://ncei.noaa.gov/erddap/tabledap/CRCP_ARMS_Cryptic_Reef_Diversity_Pacific.csv?Region,Island,Count&time>=2016-06-01T00:00:00Z&time<=2017-06-20T00:00:00Z&latitude>=14.6&latitude<=18.4&longitude>=-178.37&longitude<=166"
    

def test_erddap_griddap_url_generation() -> None:
    """
    Tests that erddap urls are being generated correctly
    """
    assert set_griddap_test().server_url == "https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPN20S3ASCIDINEOF2kmDaily.nc?chlor_a[(2025-01-09T00:00:00Z):1:(2025-03-06T00:00:00Z)][(0.0):1:(0.0)][(-67.989586):1:(-80.989586)][(133.98961):1:(161.98961)]"
    

def test_erddap_server_retrieval_tabledap() -> None:
    """
    Test ERDDAP fetch dataset functionality on griddap
    """
    x = set_tabledap_test().fetch_dataset('csv', False)
    assert x.shape[0] == 1543
    assert x.shape[1] == 3
    assert x.columns.tolist() == ["Region", "Island", "Count"]


def test_erddap_server_retrieval_griddap() -> None:
    """
    Test ERDDAP fetch dataset functionality on griddap
    """
    x = set_griddap_test().fetch_dataset('nc', False)
    assert isinstance(x, xarray.Dataset) == True
    # Need better validation checks for the dataset


def test_large_file_request():
    '''
    Tests that an error is raised for resources that exceed the limit
    '''
    x = set_griddap_test()
    x.constraint_variables = {'time': ['2020-01-09', '2025-03-06'], 'lat': [0, -100.989586], 'lon': [0.98961, 161.98961]}
    with pytest.raises(HTTPError):
        x.fetch_dataset('nc')



def test_erddap_resource_404():
    x = set_griddap_test()
    x.dataset_id = "asojf;ajs;foija;oifjo"
    x.format_erddap_url()
    with pytest.raises(HTTPError):
        x.fetch_dataset('nc')

# The functions below are to be defined upon expanion:


def server_capacity_check() -> None:
    """
    Check that ERDDAP.fetch_dataset() correctly returns and error message indicating the target resources parameters are too large for download
    pass

    """




def resource_chunking() -> None:
    """
    Check that resources are being sent in chunks
    """
    pass


def server_retry_attempts() -> None:
    """
    Check that target source is being retried the correct number of times upon failure
    """

    pass


def server_malformed_url() -> None:

    pass



def server_authentication() -> None:
    """
    #### Part of future authentication class
    
    """
    pass



if __name__ == "__main__":
    pytest.main()