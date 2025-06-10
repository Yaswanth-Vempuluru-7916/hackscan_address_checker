import requests
import pandas as pd
from dotenv import load_dotenv
import os
from db import fetch_order_addresses

load_dotenv()

def fetch_hackscan_data():
    """Fetch addresses from HackScan API."""
    url = os.getenv("HACKSCAN_API_URL")
    if not url:
        print("HACKSCAN_API_URL not set in .env")
        return None

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("API Request Error:", e)
        return None
    

def get_hackscan_addresses(data):
    addresses = set()
    if not data :
        return addresses
    for key, blockchains in data.items():
        for chain, addr_list in blockchains.items():
            for addr in addr_list:
                if addr :
                    addresses.add(addr.lower())
    
     # Temporary test addresses to simulate hacked addresses (remove after testing)
    
    # addresses.update([
    #     '0xdda173bd23b07007394611d789ef789a9aae5cf5',
    #     'cee2e8e9315259576532b26afdfa95a5bed56dd5a3e792fd1dfa57842945800b',
    #     'tb1qyux728n0x5n3cwexkzc86q0v654kewlwqmqeu8',
    #     '0xefbe82f422bfac3f8801dcbfdfe7d88bbd3b9dda',
    #     '0xd906f443401047c6f66dc6ce5ccbcdc9aa0c57b7',
    #     '0x89bfbcf387a930e210fc82b12a7608ff6e0d95e6'
    # ])

    print(addresses)
    
    return addresses

def match_addresses(db_rows, hackscan_addresses):
    hacked_rows = []

    for row in db_rows : 
        hacked_fields = []

        if row["initiator_source_address"] and row["initiator_source_address"].lower() in hackscan_addresses:
            hacked_fields.append("initiator_source_address")
        
        if row["initiator_destination_address"] and row["initiator_destination_address"].lower() in hackscan_addresses:
            hacked_fields.append("initiator_destination_address")
        
        # if row["bitcoin_optional_recipient"] and row["bitcoin_optional_recipient"].lower() in hackscan_addresses:
        #     hacked_fields.append("bitcoin_optional_recipient")

        if row["bitcoin_optional_recipient"] and isinstance(row["bitcoin_optional_recipient"], str) and row["bitcoin_optional_recipient"].lower() in hackscan_addresses:
            hacked_fields.append("bitcoin_optional_recipient")
        
        if hacked_fields:
            row_copy = row.copy()
            row_copy["hacked_addresses"] = ", ".join(hacked_fields)
            hacked_rows.append(row_copy)

    return hacked_rows

def save_to_csv(hacked_rows) : 
    if not hacked_rows:
        print("No hacked addresses found")
        return
    df = pd.DataFrame(hacked_rows)
    columns = [
        "created_at", "order_id", "source_chain", "destination_chain",
        "source_amount", "destination_amount", "initiator_source_address",
        "initiator_destination_address", "bitcoin_optional_recipient",
        "hacked_addresses"
    ]
    df = df[columns]
    df.to_csv("output.csv", index=False)
    print("Hacked data saved to output.csv")

def main():
    """Main function to fetch, match, and save hacked addresses."""
    print("HackScan Address Checker running")
    load_dotenv()
    
    # Fetch HackScan API data
    hackscan_data = fetch_hackscan_data()
    if not hackscan_data:
        return
    
    hackscan_addresses = get_hackscan_addresses(hackscan_data)
    if not hackscan_addresses:
        print("No addresses found in HackScan API")
        return
    
    # Fetch database rows (limit 5)
    db_rows = fetch_order_addresses()
    if not db_rows:
        print("No database rows fetched")
        return
    
    # Match addresses
    hacked_rows = match_addresses(db_rows, hackscan_addresses)
    
    # Save to CSV
    save_to_csv(hacked_rows)

if __name__ == "__main__":
    main()
