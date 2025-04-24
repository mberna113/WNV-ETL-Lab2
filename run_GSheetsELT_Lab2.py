from GSheetsETL_Lab2 import GSheetsEtl
from config_loader import load_config  # ← this loads the YAML

if __name__ == "__main__":
    config = load_config()

    etl_instance = GSheetsEtl(
        remote=config["remote_url"],
        local_dir=config["local_dir"],
        data_format=config["data_format"],
        destination=config["gdb_path"]
    )

    etl_instance.process()
