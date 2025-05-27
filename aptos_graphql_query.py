#!/usr/bin/env python3
"""
Aptos GraphQL Query Script with Pagination
Fetches current fungible asset balances for a specific asset type.
"""

import requests
import json
import time
import csv
import argparse
import os
from datetime import datetime
from typing import List, Dict, Any, Optional


class AptosGraphQLClient:
    def __init__(self, endpoint: str = "https://api.mainnet.aptoslabs.com/v1/graphql"):
        self.endpoint = endpoint
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _ensure_data_directory(self):
        """Ensure the data directory exists."""
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"Created data directory: {data_dir}")
        return data_dir
    
    def _get_dated_filename(self, base_name: str, extension: str) -> str:
        """Generate a filename with current date."""
        data_dir = self._ensure_data_directory()
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"{base_name}_{current_date}.{extension}"
        return os.path.join(data_dir, filename)
    
    def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query."""
        payload = {
            'query': query,
            'variables': variables or {}
        }
        
        try:
            response = self.session.post(self.endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {e}")
            raise
    
    def fetch_fungible_asset_balances_paginated(
        self, 
        asset_type: str,
        limit: int = 100,
        delay_between_requests: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Fetch all fungible asset balances with pagination.
        
        Args:
            asset_type: The asset type to query for
            limit: Number of records per page (max 100 for Aptos)
            delay_between_requests: Delay between requests to avoid rate limiting
            
        Returns:
            List of all balance records
        """
        all_balances = []
        offset = 0
        page = 1
        
        query = """
        query MyQuery($asset_type: String!, $limit: Int!, $offset: Int!) {
          current_fungible_asset_balances(
            where: {asset_type: {_eq: $asset_type}}
            limit: $limit
            offset: $offset
            order_by: {amount: desc}
          ) {
            amount
            asset_type
            owner_address
            storage_id
            is_frozen
            is_primary
            last_transaction_timestamp
            last_transaction_version
            token_standard
          }
        }
        """
        
        print(f"Starting to fetch balances for asset type: {asset_type}")
        print(f"Using limit: {limit}, delay: {delay_between_requests}s")
        print("-" * 60)
        
        while True:
            variables = {
                'asset_type': asset_type,
                'limit': limit,
                'offset': offset
            }
            
            print(f"Fetching page {page} (offset: {offset})...")
            
            try:
                result = self.execute_query(query, variables)
                
                if 'errors' in result:
                    print(f"GraphQL errors: {result['errors']}")
                    break
                
                balances = result.get('data', {}).get('current_fungible_asset_balances', [])
                
                if not balances:
                    print(f"No more data found. Stopping pagination.")
                    break
                
                all_balances.extend(balances)
                print(f"Retrieved {len(balances)} records (total so far: {len(all_balances)})")
                
                # If we got fewer records than the limit, we've reached the end
                if len(balances) < limit:
                    print(f"Received fewer records than limit ({len(balances)} < {limit}). End of data.")
                    break
                
                offset += limit
                page += 1
                
                # Add delay to avoid rate limiting
                if delay_between_requests > 0:
                    time.sleep(delay_between_requests)
                    
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break
        
        print("-" * 60)
        print(f"Pagination complete. Total records fetched: {len(all_balances)}")
        return all_balances
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str = None):
        """Save the fetched data to a JSON file."""
        if filename is None:
            filename = self._get_dated_filename("kapt_balance", "json")
        
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Data saved to {filename}")
            return filename
        except Exception as e:
            print(f"Error saving to file: {e}")
            return None
    
    def parse_json_to_csv(self, json_filename: str = None, csv_filename: str = None):
        """
        Parse JSON file to CSV with Address and Balance headers.
        
        Args:
            json_filename: Input JSON file path
            csv_filename: Output CSV file path
        """
        if json_filename is None:
            json_filename = self._get_dated_filename("kapt_balance", "json")
        
        if csv_filename is None:
            csv_filename = self._get_dated_filename("kapt_balance", "csv")
        
        try:
            # Check if JSON file exists
            if not os.path.exists(json_filename):
                print(f"Error: JSON file '{json_filename}' not found.")
                return False
            
            # Load JSON data
            print(f"Loading data from {json_filename}...")
            with open(json_filename, 'r') as f:
                data = json.load(f)
            
            if not data:
                print("No data found in JSON file.")
                return False
            
            # Write to CSV
            print(f"Converting to CSV format...")
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write headers
                writer.writerow(['Address', 'Balance'])
                
                # Write data rows
                for record in data:
                    address = record.get('owner_address', 'N/A')
                    balance = record.get('amount', '0')
                    writer.writerow([address, balance])
            
            print(f"CSV file saved to {csv_filename}")
            print(f"Converted {len(data)} records to CSV format")
            return True
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON file: {e}")
            return False
        except Exception as e:
            print(f"Error converting to CSV: {e}")
            return False
    
    def print_summary(self, balances: List[Dict[str, Any]]):
        """Print a summary of the fetched balances."""
        if not balances:
            print("No balances found.")
            return
        
        total_amount = sum(int(balance.get('amount', 0)) for balance in balances)
        unique_owners = len(set(balance.get('owner_address') for balance in balances))
        
        print(f"\nSummary:")
        print(f"Total records: {len(balances)}")
        print(f"Unique owners: {unique_owners}")
        print(f"Total amount: {total_amount}")
        print(f"Average balance: {total_amount / len(balances):.2f}")
        
        # Show top 5 balances
        sorted_balances = sorted(balances, key=lambda x: int(x.get('amount', 0)), reverse=True)
        print(f"\nTop 5 balances:")
        for i, balance in enumerate(sorted_balances[:5], 1):
            print(f"{i}. {balance.get('owner_address', 'N/A')}: {balance.get('amount', 0)}")


def parse_arguments():
    """Parse command line arguments."""
    # Get default filenames with current date
    current_date = datetime.now().strftime("%Y%m%d")
    default_json = f"data/kapt_balance_{current_date}.json"
    default_csv = f"data/kapt_balance_{current_date}.csv"
    
    parser = argparse.ArgumentParser(
        description="Aptos GraphQL Query Script with Pagination and CSV Export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python aptos_graphql_query.py --scrape                    # Only scrape data
  python aptos_graphql_query.py --parse                     # Only parse existing JSON to CSV
  python aptos_graphql_query.py --scrape --parse            # Scrape and then parse to CSV
  python aptos_graphql_query.py --parse --json-file {default_json}

Default output files (with today's date):
  JSON: {default_json}
  CSV:  {default_csv}
        """
    )
    
    parser.add_argument(
        '--scrape',
        action='store_true',
        help='Scrape data from Aptos GraphQL API'
    )
    
    parser.add_argument(
        '--parse',
        action='store_true',
        help='Parse JSON file to CSV format'
    )
    
    parser.add_argument(
        '--asset-type',
        default="0x821c94e69bc7ca058c913b7b5e6b0a5c9fd1523d58723a966fb8c1f5ea888105",
        help='Asset type to query (default: %(default)s)'
    )
    
    parser.add_argument(
        '--json-file',
        help=f'JSON file path for input/output (default: {default_json})'
    )
    
    parser.add_argument(
        '--csv-file',
        help=f'CSV file path for output (default: {default_csv})'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Number of records per page (max 100, default: %(default)s)'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay between requests in seconds (default: %(default)s)'
    )
    
    return parser.parse_args()


def main():
    """Main function to execute the script."""
    args = parse_arguments()
    
    # If no flags are provided, default to both scraping and parsing
    if not args.scrape and not args.parse:
        print("No action specified. Use --scrape and/or --parse flags.")
        print("Run with --help for usage information.")
        return
    
    # Initialize the client
    client = AptosGraphQLClient()
    
    try:
        json_filename = None
        
        # Scraping phase
        if args.scrape:
            print("=== SCRAPING PHASE ===")
            balances = client.fetch_fungible_asset_balances_paginated(
                asset_type=args.asset_type,
                limit=args.limit,
                delay_between_requests=args.delay
            )
            
            # Print summary
            client.print_summary(balances)
            
            # Save to JSON file
            json_filename = client.save_to_json(balances, args.json_file)
            print(f"Scraping completed!\n")
        
        # Parsing phase
        if args.parse:
            print("=== PARSING PHASE ===")
            # Use the JSON file from scraping if available, otherwise use specified file
            parse_json_file = json_filename if json_filename else args.json_file
            success = client.parse_json_to_csv(parse_json_file, args.csv_file)
            if success:
                print(f"Parsing completed!")
            else:
                print(f"Parsing failed!")
        
        print(f"\nScript completed successfully!")
        
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    except Exception as e:
        print(f"Script failed with error: {e}")


if __name__ == "__main__":
    main() 