import pandas as pd
import requests
import io
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

### 1. Download Data ###
# Define URL for macroeconomic data
url_macro = 'https://github.com/KMueller-Lab/Global-Macro-Database/raw/refs/heads/main/data/final/chainlinked_infl.dta'
logging.info(f"Fetching macroeconomic data from: {url_macro}")
# Read Stata file into a pandas DataFrame
df_macro = pd.read_stata(url_macro)
logging.info(f"Macroeconomic data loaded successfully with shape: {df_macro.shape}")
# Display the first 2 rows of the DataFrame
logging.info("Displaying head of macroeconomic data:")
logging.info(df_macro.head(2))

# Define URL for OECD data
url_oecd = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_PDB@DF_PDB_ULC_Q,1.0/.Q.......?startPeriod=1990-Q4&format=csv"
logging.info(f"Fetching OECD data from: {url_oecd}")
# Fetch data from the URL
response = requests.get(url_oecd)
response.raise_for_status()  # Raise an exception for HTTP errors
# Read the text data into an in-memory text buffer
data = io.StringIO(response.text)
# Read CSV data into a pandas DataFrame
df_oecd = pd.read_csv(data)
logging.info(f"OECD data loaded successfully with shape: {df_oecd.shape}")
# Display the first 2 rows of the DataFrame
logging.info("Displaying head of OECD data:")
logging.info(df_oecd.head(2))

### 2. Filter and Rename Columns ###
# Filter macroeconomic data for New Zealand, select columns, and drop missing values
df_macro_nz = df_macro.query("ISO3 == 'NZL'")[['ISO3', 'year', 'OECD_KEI_infl', 'BIS_infl']].dropna()
logging.info(f"Filtered macroeconomic data for NZL with shape: {df_macro_nz.shape}")
# Display the last 2 rows of the filtered DataFrame
logging.info("Displaying tail of filtered macroeconomic data for NZL:")
logging.info(df_macro_nz.tail(2))

# Define columns of interest for OECD data
cols = ['REF_AREA', 'TIME_PERIOD', 'OBS_VALUE', 'MEASURE', 'UNIT_MEASURE']
# Filter OECD data for New Zealand and specific measures/units
df_oecd_nz = df_oecd[cols].query("REF_AREA == 'NZL' & MEASURE=='ULCE' & UNIT_MEASURE == 'PA'")
logging.info(f"Filtered OECD data for NZL with shape: {df_oecd_nz.shape}")
# Display the first 2 rows of the filtered DataFrame
logging.info("Displaying head of filtered OECD data for NZL:")
logging.info(df_oecd_nz.head(2))

# Rename columns in the New Zealand macroeconomic DataFrame
df_macro_nz = df_macro_nz.rename({"ISO3":'country', "year":'date'}, axis=1)
logging.info("Renamed columns in macroeconomic data for NZL.")
# Display the first 2 rows of the renamed DataFrame
logging.info("Displaying head of renamed macroeconomic data for NZL:")
logging.info(df_macro_nz.head(2))

# Rename columns and drop unnecessary columns in the New Zealand OECD DataFrame
df_oecd_nz = df_oecd_nz.rename({"REF_AREA":'country', "TIME_PERIOD":'date', 'OBS_VALUE':'ULCE'}, axis=1).drop(["MEASURE", "UNIT_MEASURE"], axis=1)
logging.info("Renamed and dropped columns in OECD data for NZL.")
# Display the first 2 rows of the renamed DataFrame
logging.info("Displaying head of renamed OECD data for NZL:")
logging.info(df_oecd_nz.head(2))

### 3. Datetime Conversion ###
# Convert the 'date' column in the OECD DataFrame to datetime objects from quarterly periods and then to date
df_oecd_nz['date'] = pd.PeriodIndex(df_oecd_nz['date'], freq='Q').to_timestamp().date
logging.info("Converted 'date' column in OECD data for NZL to datetime.date.")
# Display the data type of the 'date' column
logging.info(f"Data type of 'date' in OECD data for NZL: {df_oecd_nz['date'].dtype}")

# Convert the 'date' column in the macroeconomic DataFrame to datetime objects and then to date
df_macro_nz['date'] = pd.to_datetime(df_macro_nz['date'], format = '%Y').dt.date
logging.info("Converted 'date' column in macroeconomic data for NZL to datetime.date.")
# Display the data type of the 'date' column
logging.info(f"Data type of 'date' in macroeconomic data for NZL: {df_macro_nz['date'].dtype}")

### 4. Set Index ###
# Set 'country' and 'date' as the index for the macroeconomic DataFrame
df_macro_nz = df_macro_nz.set_index(['country', 'date'])
logging.info("Set index for macroeconomic data for NZL.")
# Display the first 2 rows of the indexed DataFrame
logging.info("Displaying head of indexed macroeconomic data for NZL:")
logging.info(df_macro_nz.head(2))

# Set 'country' and 'date' as the index for the OECD DataFrame
df_oecd_nz = df_oecd_nz.set_index(['country', 'date'])
logging.info("Set index for OECD data for NZL.")
# Display the first 2 rows of the indexed DataFrame
logging.info("Displaying head of indexed OECD data for NZL:")
logging.info(df_oecd_nz.head(2))

### 5. Merge DataFrames ###
# Merge the two DataFrames based on the common index ('country', 'date') using an inner join
df_merge = pd.merge(
    df_macro_nz,
    df_oecd_nz,
    right_index = True,
    left_index = True,
    how = 'inner'

)
logging.info(f"Merged DataFrames with shape: {df_merge.shape}")
# Display the last 2 rows of the merged DataFrame
logging.info("Displaying tail of merged DataFrame:")
logging.info(df_merge.tail(2))

### 6. Export Data ###
# Ensure the 'data/intermediate/' directory exists
os.makedirs("data/intermediate/", exist_ok=True)
# Export the merged DataFrame to a CSV file in the 'data/intermediate/' directory
df_merge.to_csv("data/intermediate/merged_data_nz.csv")
logging.info("Exported merged DataFrame to data/intermediate/merged_data_nz.csv")

# Ensure the 'data/raw/' directory exists
os.makedirs("data/raw/", exist_ok=True)
# Export the raw OECD DataFrame to a CSV file in the 'data/raw/' directory
df_oecd.to_csv("data/raw/oecd.csv")
logging.info("Exported raw OECD DataFrame to data/raw/oecd.csv")