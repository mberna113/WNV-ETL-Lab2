import os
import requests
import csv
import time
import arcpy
from urllib.parse import urlencode
from SpatialETL_Lab2 import SpatialEtl
from config_loader import load_config
config = load_config()

class GSheetsEtl(SpatialEtl):

    def __init__(self, remote, local_dir, data_format, destination):
        super().__init__(remote, local_dir, data_format, destination)
        self.downloaded_csv = os.path.join(self.local_dir, "Opt_Out_Addresses.csv")
        self.transformed_csv = os.path.join(self.local_dir, "Opt_Out_Addresses_transformed.csv")

    def extract(self):
        print("Extracting addresses from Google Form spreadsheet.")
        r = requests.get(self.remote)
        r.encoding = "utf-8"
        data = r.text
        with open(self.downloaded_csv, "w", newline='', encoding="utf-8") as output_file:
            output_file.write(data)
        print("✅ CSV downloaded")

    def nominatim_geocode(self, address):
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
                return float(data[0]['lon']), float(data[0]['lat'])
        except Exception as e:
            print(f"⚠️ Geocoding failed for {address}: {e}")
        return None, None

    def transform(self):
        print("Transforming: Adding city/state and geocoding addresses")

        if os.path.exists(self.transformed_csv):
            try:
                os.remove(self.transformed_csv)
            except PermissionError:
                print("File is locked. Close Excel and try again.")
                return

        with open(self.transformed_csv, "w", newline='', encoding="utf-8") as transformed_file:
            writer = csv.writer(transformed_file)
            writer.writerow(["x", "y", "Type"])

            with open(self.downloaded_csv, "r", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    address = row["Street Address"] + " Boulder CO"
                    print(f"Geocoding: {address}")
                    x, y = self.nominatim_geocode(address)
                    time.sleep(1)

                    if x is not None and y is not None:
                        writer.writerow([x, y, "Residential"])

        print("✅ Transform complete")

    def load(self):
        print("Loading into ArcGIS as points...")
        arcpy.env.workspace = self.destination
        arcpy.env.overwriteOutput = True

        in_table = self.transformed_csv
        out_features = "Opt_Out_Address_Points"

        arcpy.management.XYTableToPoint(in_table, out_features, "x", "y")
        print("✅ Load complete:", out_features)

    def final_analysis(self):
        print("Starting final analysis...")

        arcpy.env.workspace = config["gdb_path"]
        arcpy.env.overwriteOutput = True

        avoid_points = "Opt_Out_Address_Points"
        buffered_layer = "Avoid_Buffer"
        intersect_layer = "High_Risk_Intersect"  # <-- change if yours is different
        final_layer = "Final_Selection"

        # Step 1: Buffer the avoid points
        arcpy.analysis.Buffer(avoid_points, buffered_layer, "1500 feet")
        print("✅ Buffered avoid points layer created:", buffered_layer)

        # Step 2: Erase buffered zones from intersect layer
        arcpy.analysis.Erase(intersect_layer, buffered_layer, final_layer)
        print("✅ Final selection layer created:", final_layer)


    def process(self):
        print("🚀 Starting ETL process...\n")
        self.extract()
        self.transform()
        self.load()
        print("\n✅ ETL process complete.")

        print("Starting Final Analysis process... ")
        self.final_analysis()
        print("Final Analysis process complete ✅")
