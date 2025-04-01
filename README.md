# Data Import Workflow

This library automates data retrieval using a user-supplied configuration file, making it easier to pull predefined datasets without manual work. The configuration file specifies which platform to pull data from (ERDDAP, OPeNDAP, or GlobusAPI), what parameters to use, and where to store the results.

## Running tests:

To run tests, install pytest using the following command:

` pip install pytest `

From there, cd into either the data_retrieval or parser_testing directories. Run a test on either using the command:

`pytest test_parse_config.py` **or test_data_retrieval.py**

## Running the Application:

### Requirements

Install all required python packages using the below command in the main application directory:

` pip install -r requirements.txt `

### Creating a Config File

The configuration file defines the target resource parameters and the source server. Expansion into other data platforms (OpenDAP, Globus, etc) are in progress.

In order to create your own configuration file by copying either of the provided config yaml files (ex: erddap-griddap-config.yaml) and editing with the necessary values to create your own configuration file. Use the path of your create config file as the first argument in running the application. The required data types for each field are defined within the variable.yaml fil.

** The `variable.yaml` file should only be updated if a new version of the configuration file is released. **

### Running the Application

To run the application, ensure you are in the main application directory (same directory as app.py) and run the below command with your config file and the validation file. 

`./app.py {config file path} variable.yaml`

To obtain data pulled, uncomment the write(file) statement within the app.py.



