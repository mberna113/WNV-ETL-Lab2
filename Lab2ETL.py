import arcpy
import requests
import csv
import time
from urllib.parse import urlencode
import os
from config_loader import load_config
config = load_config()


# Step 1: Extract CSV from Google Form
def extract():
    print("Extracting addresses from Google Form spreadsheet.")
    r = requests.get(config["remote_url"])
    r.encoding = "utf-8"
    data = r.text
    csv_path = os.path.join(config["local_dir"], "Opt_Out_Addresses.csv")
    with open(csv_path, "w", newline='', encoding="utf-8") as output_file:
        output_file.write(data)

# Step 2: Use Nominatim for geocoding
def nominatim_geocode(address):
    base_url = "https://nominatim.openstreetmap.org/search?"
    params = {
        "q": address,
        "format": "json",
        "limit": 1
    }
    full_url = base_url + urlencode(params)
    headers = {"User-Agent": "GIS_305_Assignment_ETL_Script"}

    try:
        response = requests.get(full_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data:
            x = float(data[0]['lon'])
            y = float(data[0]['lat'])
            return x, y
        else:
            print(f"No results found for: {address}")
            return None, None
    except Exception as e:
        print(f"Geocoding failed for {address}: {e}")
        return None, None

# Step 2.5: Transform address list into x/y coordinates CSV
def transform():
    print("Transforming: Adding city/state and geocoding addresses")

    input_csv = os.path.join(config["local_dir"], "Opt_Out_Addresses.csv")
    output_csv = os.path.join(config["local_dir"], "Opt_Out_Addresses_transformed.csv")

    # Delete old transformed file if it exists
    if os.path.exists(output_csv):
        try:
            os.remove(output_csv)
        except PermissionError:
            print("File is open or locked — close Excel or other apps using it.")
            return

    with open(output_csv, "w", newline='', encoding="utf-8") as transformed_file:
        writer = csv.writer(transformed_file)
        writer.writerow(["x", "y", "Type"])  # lowercase for ArcGIS

        with open(input_csv, "r", encoding="utf-8") as partial_file:
            csv_dist = csv.DictReader(partial_file)
            for row in csv_dist:
                address = row["Street Address"] + " Boulder CO"
                print(f"Geocoding: {address}")
                x, y = nominatim_geocode(address)
                time.sleep(1)  # Respect OSM usage policy

                if x is not None and y is not None:
                    try:
                        x_clean = float(str(x).strip().replace("'", "").replace('"', ''))
                        y_clean = float(str(y).strip().replace("'", "").replace('"', ''))
                        writer.writerow([x_clean, y_clean, "Residential"])
                    except ValueError:
                        print(f"Skipping invalid coordinates: x={x}, y={y}")

# Step 3: Load into ArcGIS as points
def load():
    arcpy.env.workspace = config["gdb_path"]
    arcpy.env.overwriteOutput = True

    in_table = os.path.join(config["local_dir"], "Opt_Out_Addresses_transformed.csv")
    out_features_class = "Opt_Out_Address_Points"
    x_coords = "x"
    y_coords = "y"

    arcpy.management.XYTableToPoint(in_table, out_features_class, x_coords, y_coords)
    print("Loaded into feature class:", out_features_class)
    print(arcpy.GetCount_management(out_features_class))




# Main runner
if __name__ == "__main__":

    print("Starting etl process... ")

    extract()
    transform()
    load()

    print("ETL process complete ✅")

