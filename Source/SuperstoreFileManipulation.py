import pandas as pd
import configparser
import os

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("superstore.log"),  # Log to a file
        logging.StreamHandler(),  # Log to console
    ],
)


def process():
    logging.info("Starting the Superstore data transformation/ Manupulation process.")

    # Load configuration
    config = configparser.ConfigParser()
    config.read("config.ini")

    input_file = config["SuperstoreDataFile"]["input_file"]
    output_file = config["ExportFile"]["output_file"]

    # Read Excel file
    logging.info("Read process of input file start.")
    print(f"Checking if input file exists: {os.path.exists(input_file)}")
    xls = load_data(input_file)

    sheets = {}
    for sheet_name in xls.sheet_names:
        try:
            sheets[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
            logging.info(f"Loaded sheet: {sheet_name}")  # Use f-string for variable interpolation
        except Exception as e:
            logging.error(f"Failed to load sheet '{sheet_name}': {e}")

    try:
        orders = sheets["Orders"]
        returns = sheets["Returns"]
        people = sheets["People"]
        states = sheets["States"]
    except KeyError as e:
        logging.error(f"Missing expected sheet: {e}")
        return

    logging.info("Transforming data...")

    # 1. Join Orders and Returns to add Return Status
    logging.info(
        "Operation 1 processing Join Orders and Returns sheet and add Return Status"
    )
    orders["Return Status"] = (
        orders["Order ID"]
        .isin(returns["Order ID"])
        .replace({True: "Returned", False: "Not Returned"})
    )

    # 2. Join Orders and People to add Regional Manager
    logging.info(
        "Operation 2 processing Join Orders and People sheet and add Regional Manager"
    )
    merged_orders = orders.merge(
        people[["Person", "Region"]],  # Select Person and Region from people
        on="Region",  # Join on the Region column
        how="left",
    )
    merged_orders.rename(columns={"Person": "Regional Manager"}, inplace=True)
    orders = merged_orders

    # 3. Create new column for Shipping Duration
    logging.info(
        "Operation 3 Create new column for Shipping Duration"
    )
    orders["Shipping Duration"] = (
        pd.to_datetime(orders["Ship Date"]) - pd.to_datetime(orders["Order Date"])
    ).dt.days

    # 4. Effective Price calculation
    logging.info(
        "Operation 4 add column Effective Price and calculate Effective Price calculation"
    )
    orders["Effective Price"] = orders["Sales"] * (1 - orders["Discount"])

    # 5. Profit Margin calculation
    logging.info(
        "Operation 5 add column Profit Margin and calculate Profit Margin calculation"
    )
    orders["Profit Margin"] = orders["Profit"] / orders["Sales"]

    # 6. Profitability Segment classification
    logging.info(
        "Operation 6 add column Profitability Segment and calculate Profitability Segment classification"
    )
    profit_threshold = 0.20
    orders["Profitability Segment"] = orders["Profit Margin"].apply(
        lambda x: "High Profit" if x > profit_threshold else "Low Profit"
    )

    # 7. Get State Name and State Codes and add State Code for all states
    logging.info(
        "Operation 7 add column State Code"
    )
    states_merged_orders = orders.merge(
        states[['State Name', 'State Code']],  # Select only 'State Name' and 'State Code'
        left_on='State',  # Join on the 'State' column in orders
        right_on='State Name',  # Join on the 'State Name' column in states
        how='left'  # Use left join to keep all orders
    )
    orders['State Code'] = states_merged_orders['State Code']

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(f"{output_file}")
    os.makedirs(output_dir, exist_ok=True)
    print("Output Directory:", output_dir)
    print("Directory created or already exists:", os.path.exists(output_dir))

    # Construct full output path
    full_output_path = f"{output_file}"
    print("Full Output Path:", full_output_path)

    # Save the DataFrame to CSV
    try:
        orders.to_csv(full_output_path, index=False)
        print("File saved successfully.")
    except Exception as e:
        print(f"Failed to save the file: {e}")

    print(f"Data successfully transformed and saved to {output_file}")

# Load the Excel file
def load_data(input_file):
    try:
        xls = pd.ExcelFile(input_file)  # This will use xlrd for .xls files
        logging.info("Excel file loaded successfully.")
        return xls
    except Exception as e:
        logging.error(f"Failed to load Excel file: {e}")
        return None


if __name__ == "__main__":
    process()
