# West Nile Virus Outbreak - GIS ETL and Mapping Project

## Overview
This project automates the extraction, transformation, and loading of public health opt-out addresses 
into a GIS analysis environment to support safe pesticide spraying decisions. It identifies valid 
target zones while avoiding addresses of sensitive individuals.

## Components
- **extract**: Pulls opt-out address CSV from a Google Form.
- **transform**: Appends city/state and geocodes using Nominatim.
- **load**: Loads geocoded points into an ArcGIS geodatabase.
- **exportMap**: Applies a subtitle and exports a formatted PDF map.
- **set_spatial_reference / apply_renderer / join_and_filter**: Finalize GIS visualization using spatial reference, symbol styling, and definition queries.

## Requirements
- ArcGIS Pro
- Python 3 with arcpy and requests
- Project config YAML file (`config_loader.py` required)

## How to Run
1. Ensure you have updated your `config_loader.py` to include:
    - `proj_dir`
    - `local_dir`
    - `remote_url`
    - `gdb_path`
    - `proj_loc`

2. Run the script in Python environment with ArcGIS Pro:
```bash
python finalproject.py
