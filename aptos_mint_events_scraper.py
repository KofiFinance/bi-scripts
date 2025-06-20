#!/usr/bin/env python3
"""
Aptos Mint Events Scraper Script with Pagination
Fetches mint events from the Aptos blockchain using GraphQL.
"""

import requests
import json
import time
import csv
import argparse
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class AptosMintEventsScraper:
    def __init__(self, endpoint: str = "https://api.mainnet.aptoslabs.com/v1/graphql"):
        self.endpoint = endpoint
        self.session = requests.Session()
        auth_token = os.getenv('APTOS_AUTH_TOKEN')
        headers = {
            'Content-Type': 'application/json',
        }
        if auth_token:
            headers['Authorization'] = f"Bearer {auth_token}"
        else:
            print("Warning: APTOS_AUTH_TOKEN environment variable not set or .env file not loaded. API requests might fail or be rate-limited.")
            # Optionally, you could raise an error here if the token is strictly required:
            # raise ValueError("APTOS_AUTH_TOKEN environment variable is required and not set.")
        self.session.headers.update(headers)

        print(self.session.headers)
    
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
    
    def fetch_mint_events_paginated(
        self, 
        event_type: str = "0x7e783b349d3e89cf5931af376ebeadbfab855b3fa239b7ada8f5a92fbea6b387::event::PriceFeedUpdate",
        limit: int = 100,
        delay_between_requests: float = 0.1
    ) -> List[Dict[str, Any]]:
        """
        Fetch all mint events with pagination.
        
        Args:
            event_type: The event type to query for
            limit: Number of records per page (max 100 for Aptos)
            delay_between_requests: Delay between requests to avoid rate limiting
            
        Returns:
            List of all mint event records
        """
        all_events = []
        offset = 0
        page = 1
        
        query = """
        query MyQuery($event_type: String!) {
          events(
            where: {type: {_eq: $event_type}}
          ) {
            data
          }
        }
        """
        
        print(f"Starting to fetch mint events for type: {event_type}")
        print(f"Using limit: {limit}, delay: {delay_between_requests}s")
        print("-" * 60)
        
        while True:
            variables = {
                'event_type': event_type,
                # 'limit': limit,
                # 'offset': offset
            }
            
            print(f"Fetching page {page} (offset: {offset})...")
            
            try:
                result = self.execute_query(query, variables)
                
                if 'errors' in result:
                    print(f"GraphQL errors: {result['errors']}")
                    break
                
                events = result.get('data', {}).get('events', [])
                
                if not events:
                    print(f"No more data found. Stopping pagination.")
                    break
                
                all_events.extend(events)
                print(f"Retrieved {len(events)} records (total so far: {len(all_events)})")
                
                # If we got fewer records than the limit, we've reached the end
                if len(events) < limit:
                    print(f"Received fewer records than limit ({len(events)} < {limit}). End of data.")
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
        print(f"Pagination complete. Total records fetched: {len(all_events)}")
        return all_events
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str = None):
        """Save the fetched data to a JSON file."""
        if filename is None:
            filename = self._get_dated_filename("kapt_mint_events", "json")
        
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
        Parse JSON file to CSV with relevant mint event fields.
        
        Args:
            json_filename: Input JSON file path
            csv_filename: Output CSV file path
        """
        if json_filename is None:
            json_filename = self._get_dated_filename("kapt_mint_events", "json")
        
        if csv_filename is None:
            csv_filename = self._get_dated_filename("kapt_mint_events", "csv")
        
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
                writer.writerow([
                    'Data'
                ])
                
                # Write data rows
                for record in data:
                    writer.writerow([
                        json.dumps(record.get('data', {})) if record.get('data') else 'N/A'
                    ])
            
            print(f"CSV file saved to {csv_filename}")
            print(f"Converted {len(data)} records to CSV format")
            return True
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON file: {e}")
            return False
        except Exception as e:
            print(f"Error converting to CSV: {e}")
            return False
    
    def print_summary(self, events: List[Dict[str, Any]]):
        """Print a summary of the fetched mint events."""
        if not events:
            print("No mint events found.")
            return
        
        unique_accounts = len(set(event.get('account_address') for event in events))
        unique_transaction_versions = len(set(event.get('transaction_version') for event in events))
        
        # Get block height range
        block_heights = [event.get('transaction_block_height', 0) for event in events if event.get('transaction_block_height')]
        min_block = min(block_heights) if block_heights else 0
        max_block = max(block_heights) if block_heights else 0
        
        print(f"\nSummary:")
        print(f"Total mint events: {len(events)}")
        print(f"Unique accounts: {unique_accounts}")
        print(f"Unique transactions: {unique_transaction_versions}")
        print(f"Block height range: {min_block} - {max_block}")
        
        # Show most recent events
        sorted_events = sorted(events, key=lambda x: int(x.get('transaction_version', 0)), reverse=True)
        print(f"\nMost recent 5 events:")
        for i, event in enumerate(sorted_events[:5], 1):
            print(f"{i}. TX Version: {event.get('transaction_version', 'N/A')}, "
                  f"Account: {event.get('account_address', 'N/A')}")


def parse_arguments():
    """Parse command line arguments."""
    # Get default filenames with current date
    current_date = datetime.now().strftime("%Y%m%d")
    default_json = f"data/kapt_mint_events_{current_date}.json"
    default_csv = f"data/kapt_mint_events_{current_date}.csv"
    
    parser = argparse.ArgumentParser(
        description="Aptos Mint Events Scraper with Pagination and CSV Export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python aptos_mint_events_scraper.py --scrape                    # Only scrape mint events
  python aptos_mint_events_scraper.py --parse                     # Only parse existing JSON to CSV
  python aptos_mint_events_scraper.py --scrape --parse            # Scrape and then parse to CSV
  python aptos_mint_events_scraper.py --parse --json-file {default_json}

Default output files (with today's date):
  JSON: {default_json}
  CSV:  {default_csv}
        """
    )
    
    parser.add_argument(
        '--scrape',
        action='store_true',
        help='Scrape mint events from Aptos GraphQL API'
    )
    
    parser.add_argument(
        '--parse',
        action='store_true',
        help='Parse JSON file to CSV format'
    )
    
    parser.add_argument(
        '--event-type',
        default="0x2cc52445acc4c5e5817a0ac475976fbef966fedb6e30e7db792e10619c76181f::minting_manager::MintEvent",
        help='Event type to query (default: %(default)s)'
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
    
    # If no flags are provided, show help
    if not args.scrape and not args.parse:
        print("No action specified. Use --scrape and/or --parse flags.")
        print("Run with --help for usage information.")
        return
    
    # Initialize the scraper
    scraper = AptosMintEventsScraper()
    
    try:
        json_filename = None
        
        # Scraping phase
        if args.scrape:
            print("=== SCRAPING PHASE ===")
            events = scraper.fetch_mint_events_paginated(
                event_type=args.event_type,
                limit=args.limit,
                delay_between_requests=args.delay
            )
            
            # Print summary
            scraper.print_summary(events)
            
            # Save to JSON file
            json_filename = scraper.save_to_json(events, args.json_file)
            print(f"Scraping completed!\n")
        
        # Parsing phase
        if args.parse:
            print("=== PARSING PHASE ===")
            # Use the JSON file from scraping if available, otherwise use specified file
            parse_json_file = json_filename if json_filename else args.json_file
            success = scraper.parse_json_to_csv(parse_json_file, args.csv_file)
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