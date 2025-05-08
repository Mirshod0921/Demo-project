import pandas as pd
import requests
import os

def csv_to_sql():
    url_json_metadata = 'https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/column_table_map.json'
    metadata = requests.get(url_json_metadata).json()

    # Keys for the 6 CSV files
    csv_keys = ["01", "02", "03", "04", "05", "07"]

    # Folder to save processed files
    output_folder = "processed_csvs"
    os.makedirs(output_folder, exist_ok=True)

    for key in csv_keys:
        data = metadata.get(key)
        if not data or data.get("file") == "Derived":
            continue

        file_name = data["file"]
        table_name = data["table"]
        url_csv = f'https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/files_final/{file_name}'

        try:
            # Read CSV with no header and force string dtype to avoid date parsing
            df_raw = pd.read_csv(url_csv, header=None, dtype=str)
        except Exception as e:
            print(f"Error loading {file_name}: {e}")
            continue

        # Use first row as header
        df_raw.columns = df_raw.iloc[0].astype(str).str.strip()
        df = df_raw[1:].reset_index(drop=True)

        # Rename columns if mapping is present
        if "columns" in data:
            column_mapping = {(data["prefix"][:3]+k.strip()): v for k, v in data["columns"].items()}
            df = df.rename(columns=column_mapping)




        if table_name == "users":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["last_active_at"] = pd.to_datetime(df["last_active_at"], errors="coerce")
            df["is_vip"] = df["is_vip"].astype(bool)
            df["total_balance"] = pd.to_numeric(df["total_balance"], errors="coerce")
            df["invalid_email"] = ~df["email"].str.contains(r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$", na=True)
            df["invalid_phone"] = ~df["phone_number"].str.match(r"^\+?\d{7,15}$", na=True)

        elif table_name == "cards":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
            df["card_number"] = pd.to_numeric(df["card_number"], errors="coerce")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["balance"] = pd.to_numeric(df["balance"], errors="coerce")
            df["limit_amount"] = pd.to_numeric(df["limit_amount"], errors="coerce")
            df["exceeds_limit"] = df["balance"] > df["limit_amount"]


        elif table_name == "transactions":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            df["from_card_id"] = pd.to_numeric(df["from_card_id"], errors="coerce")
            df["to_card_id"] = pd.to_numeric(df["to_card_id"], errors="coerce")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["is_flagged"] = df["amount"] > 14_000_000

        elif table_name == "logs":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["transaction_id"] = pd.to_numeric(df["transaction_id"], errors="coerce")

        elif table_name == "reports":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df["total_transactions"] = pd.to_numeric(df["total_transactions"], errors="coerce")
            df["flagged_transactions"] = pd.to_numeric(df["flagged_transactions"], errors="coerce")
            df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

        elif table_name == "scheduled_payments":
            df["id"] = pd.to_numeric(df["id"], errors="coerce")
            df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
            df["status"] = df["status"].astype(bool)
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")




        # Save to local CSV
        
        
        
        
        try:
            output_path = os.path.join(output_folder, f"{table_name}.csv")
            df.to_csv(output_path, index=False)
        except Exception as e:
            print(f"Error saving {file_name}: {e}")
            continue
        print(f"Saved to {output_path}")
