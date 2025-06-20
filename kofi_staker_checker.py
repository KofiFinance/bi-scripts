#!/usr/bin/env python3
"""
Kofi Staker Checker Script
Checks if a given Aptos address has a cumulative amount from MintEvents
exceeding a specified threshold.
"""

import requests
import json
import time
import argparse
import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
DEFAULT_ENDPOINT = "https://api.mainnet.aptoslabs.com/v1/graphql"
MINT_EVENT_TYPE = "0x2cc52445acc4c5e5817a0ac475976fbef966fedb6e30e7db792e10619c76181f::minting_manager::MintEvent"
STAKING_THRESHOLD = 100_000_000 # 100 million
DEFAULT_PAGE_LIMIT = 100 # Max limit for Aptos API is 100
DEFAULT_DELAY = 0.1 # Seconds
DEFAULT_CACHE_DIR = "data/cache"

class KofiStakerChecker:
    def __init__(self, endpoint: str = DEFAULT_ENDPOINT):
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
        self.session.headers.update(headers)
        print(f"Initialized checker for endpoint: {self.endpoint}")
        if auth_token:
            print("Using APTOS_AUTH_TOKEN.")

    def _ensure_cache_directory(self, cache_dir: str):
        """Ensure the cache directory exists."""
        if not os.path.exists(cache_dir):
            try:
                os.makedirs(cache_dir)
                print(f"Created cache directory: {cache_dir}")
            except OSError as e:
                print(f"Warning: Could not create cache directory {cache_dir}: {e}. Cache will not be saved.")
                return False
        return True

    def _get_cache_filename(self, event_type: str, cache_dir: str) -> str:
        """Generate a standardized cache filename for the event type and current date."""
        safe_event_type = event_type.replace("::", "_").replace("<", "_").replace(">", "_") # Make it filename friendly
        current_date = time.strftime("%Y%m%d")
        filename = f"{safe_event_type}_events_{current_date}.json"
        return os.path.join(cache_dir, filename)

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
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} - {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {e}")
            raise

    def fetch_all_events_by_type(
        self,
        event_type: str = MINT_EVENT_TYPE, # This is the event type signature
        limit: int = DEFAULT_PAGE_LIMIT,
        delay_between_requests: float = DEFAULT_DELAY
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL specified events by their full type signature, paginating as needed.
        This fetches events globally for the type, not per user at this stage.
        """
        all_events_of_type = []
        offset = 0
        page = 1
        
        # GraphQL query to fetch all events of a specific type globally.
        query = """
        query GetAllEventsByType($event_type: String!, $limit: Int!, $offset: Int!) {
          events(
            where: {
              indexed_type: {_eq: $event_type} # Filter by the full event type signature
            },
            limit: $limit,
            offset: $offset,
            order_by: {transaction_version: asc}
          ) {
            data                # Essential for data.user and data.amount
            indexed_type      # Can be included if needed, but type is primary for full signature
          }
        }
        """
        
        print(f"Starting to fetch ALL events of type: '{event_type}'")
        print(f"This may take some time depending on the total number of such events on the blockchain.")
        print(f"Using limit: {limit}, delay: {delay_between_requests}s")
        print("-" * 60)
        
        while True:
            variables = {
                'event_type': event_type,
                'limit': limit,
                'offset': offset
            }
            
            print(f"Fetching page {page} (offset: {offset}) of all '{event_type}' events...")
            
            try:
                result = self.execute_query(query, variables)
                
                if 'errors' in result:
                    print(f"GraphQL errors: {result['errors']}")
                    for error_detail in result['errors']:
                        if 'message' in error_detail:
                            print(f"  Error message: {error_detail['message']}")
                        if 'extensions' in error_detail and 'code' in error_detail['extensions']:
                             print(f"  Error code: {error_detail['extensions']['code']}")
                    break 
                
                events_data = result.get('data', {})
                if not events_data:
                    print("No 'data' field in GraphQL response. Stopping pagination.")
                    break

                events_page = events_data.get('events', [])
                
                if not events_page:
                    print(f"No more events found on page {page} for type '{event_type}'. Stopping pagination.")
                    break
                
                all_events_of_type.extend(events_page)
                print(f"Retrieved {len(events_page)} events (total for type '{event_type}' so far: {len(all_events_of_type)})")
                
                if len(events_page) < limit:
                    print(f"Received fewer events than limit ({len(events_page)} < {limit}). Assuming end of data for this event type.")
                    break
                
                offset += limit
                page += 1
                
                if delay_between_requests > 0:
                    time.sleep(delay_between_requests)
                    
            except Exception as e:
                print(f"Error on page {page} while fetching all events of type '{event_type}': {e}")
                break 
        
        print("-" * 60)
        print(f"Global event fetching complete. Total '{event_type}' events retrieved: {len(all_events_of_type)}")
        return all_events_of_type

    def calculate_cumulative_amount(self, events: List[Dict[str, Any]]) -> int:
        """
        Calculate the cumulative amount from the 'data' field of events.
        Assumes event['data']['amount'] exists and is a string representing an integer.
        """
        total_amount = 0
        processed_count = 0
        malformed_data_count = 0

        if not events:
            print("No events provided for amount calculation.")
            return 0

        print(f"Calculating cumulative amount from {len(events)} events...")
        for i, event in enumerate(events):
            try:
                event_data = event.get('data')
                if not isinstance(event_data, dict):
                    # print(f"Warning: Event {i+1} (TX: {event.get('transaction_version', 'N/A')}) 'data' field is not a dictionary. Skipping.")
                    # print(f"  Data type: {type(event_data)}, Data content: {event_data}")
                    malformed_data_count += 1
                    continue

                amount_str = event_data.get('amount')
                if amount_str is None:
                    # print(f"Warning: Event {i+1} (TX: {event.get('transaction_version', 'N/A')}) 'data' field does not contain 'amount' key. Skipping.")
                    # print(f"  Data content: {event_data}")
                    malformed_data_count += 1
                    continue
                
                total_amount += int(amount_str)
                processed_count += 1

            except ValueError:
                # print(f"Warning: Event {i+1} (TX: {event.get('transaction_version', 'N/A')}) 'amount' ('{amount_str}') is not a valid integer. Skipping.")
                malformed_data_count +=1
            except Exception as e:
                # print(f"Unexpected error processing event {i+1} (TX: {event.get('transaction_version', 'N/A')}): {e}. Skipping.")
                malformed_data_count +=1
        
        if malformed_data_count > 0:
            print(f"Warning: Skipped {malformed_data_count} events due to missing or malformed 'amount' in data field.")
        print(f"Successfully processed {processed_count} events for amount calculation.")
        return total_amount

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Check if an Aptos address meets Kofi staking criteria based on MintEvent cumulative amount.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--address',
        type=str,
        help='A single Aptos account address to check.'
    )
    
    group.add_argument(
        '--addresses-file',
        type=str,
        help='Path to a JSON file containing a list of Aptos account addresses to check.'
    )
    
    parser.add_argument(
        '--event-type',
        type=str,
        default=MINT_EVENT_TYPE,
        help=f'The event type to query for (default: {MINT_EVENT_TYPE})'
    )

    parser.add_argument(
        '--threshold',
        type=int,
        default=STAKING_THRESHOLD,
        help=f'The cumulative amount threshold to check against (default: {STAKING_THRESHOLD})'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=DEFAULT_PAGE_LIMIT,
        help=f'Number of records per page for API requests (max 100, default: {DEFAULT_PAGE_LIMIT})'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=DEFAULT_DELAY,
        help=f'Delay between API requests in seconds (default: {DEFAULT_DELAY})'
    )

    parser.add_argument(
        '--cache-dir',
        type=str,
        default=DEFAULT_CACHE_DIR,
        help=f'Directory to store/load cache files (default: {DEFAULT_CACHE_DIR})'
    )

    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Ignore existing cache and fetch fresh data from the API. Updates the cache.'
    )
    
    return parser.parse_args()

def main():
    """Main function to execute the script."""
    args = parse_arguments()
    
    addresses_to_check = []
    if args.addresses_file:
        try:
            with open(args.addresses_file, 'r') as f:
                addresses_to_check = json.load(f)
            if not isinstance(addresses_to_check, list) or not all(isinstance(addr, str) for addr in addresses_to_check):
                print(f"Error: Content of {args.addresses_file} is not a valid JSON list of strings.")
                return
            print(f"Loaded {len(addresses_to_check)} addresses from {args.addresses_file}.")
        except FileNotFoundError:
            print(f"Error: Addresses file not found: {args.addresses_file}")
            return
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {args.addresses_file}")
            return
    elif args.address:
        addresses_to_check.append(args.address)

    if not addresses_to_check:
        print("Error: No addresses provided to check. Use --address or --addresses-file.")
        parser.print_help() # type: ignore
        return

    print(f"Event type: {args.event_type}")
    print(f"Staking threshold: {args.threshold:,}") # Formatted threshold
    
    checker = KofiStakerChecker()
    results_summary = []

    # Prepare cache directory and filename
    can_use_cache_dir = checker._ensure_cache_directory(args.cache_dir)
    cache_filename = None
    if can_use_cache_dir:
        cache_filename = checker._get_cache_filename(args.event_type, args.cache_dir)
        print(f"Cache file for this run: {cache_filename}")

    all_target_events = None

    # Step 1: Try loading from cache or fetch all relevant events from the blockchain
    if not args.no_cache and cache_filename and os.path.exists(cache_filename):
        print(f"\n=== Step 1: Attempting to load events from cache: {cache_filename} ===")
        try:
            with open(cache_filename, 'r') as f:
                all_target_events = json.load(f)
            if isinstance(all_target_events, list): # Basic validation
                print(f"Successfully loaded {len(all_target_events)} events from cache.")
            else:
                print(f"Warning: Cache file {cache_filename} did not contain a valid list. Re-fetching.")
                all_target_events = None # Invalidate to trigger re-fetch
        except json.JSONDecodeError as e:
            print(f"Warning: Could not decode JSON from cache file {cache_filename}: {e}. Re-fetching.")
            all_target_events = None # Invalidate
        except Exception as e:
            print(f"Warning: Could not load from cache file {cache_filename}: {e}. Re-fetching.")
            all_target_events = None # Invalidate

    if all_target_events is None: # If cache not used, not found, or failed to load
        if not args.no_cache and cache_filename: # Only print if we didn't explicitly ask to ignore cache
            print("Cache not used or failed. Fetching from API...")
        print("\n=== Step 1: Fetching all relevant events from the blockchain ===")
        all_target_events = checker.fetch_all_events_by_type(
            event_type=args.event_type,
            limit=args.limit,
            delay_between_requests=args.delay
        )

        if all_target_events is not None and cache_filename: # Save if fetch was successful (even if empty list)
            print(f"Attempting to save {len(all_target_events)} fetched events to cache: {cache_filename}")
            try:
                with open(cache_filename, 'w') as f:
                    json.dump(all_target_events, f, indent=2)
                print("Successfully saved events to cache.")
            except Exception as e:
                print(f"Warning: Could not save events to cache file {cache_filename}: {e}")

    if all_target_events is None: # Should only happen if API fetch also failed critically
        all_target_events = [] # Ensure it's an empty list to prevent downstream errors
        print("Critical: Failed to load events from cache and API. Proceeding with no events.")

    if not all_target_events:
        print(f"\nNo events of type '{args.event_type}' found globally on the blockchain.")
        print("Cannot proceed with checking addresses.")
        # Fill summary for all addresses indicating no events found for them
        for addr_to_check in addresses_to_check:
            results_summary.append({
                "address": addr_to_check,
                "meets_criteria": False,
                "cumulative_amount": 0,
                "events_found": 0,
                "error": "No global events of target type found"
            })
    else:
        print(f"\n=== Step 2: Processing {len(addresses_to_check)} addresses against {len(all_target_events)} fetched events ===")
        for i, address_to_check in enumerate(addresses_to_check):
            print(f"\n--- Processing address {i+1}/{len(addresses_to_check)}: {address_to_check} ---")
            
            current_result = {
                "address": address_to_check,
                "meets_criteria": False,
                "cumulative_amount": 0,
                "events_found": 0, # Will be updated after filtering
                "error": None
            }

            # Step 2a: Filter the global list for events relevant to the current address_to_check
            user_specific_events = []
            if all_target_events: # Only filter if there are global events
                print(f"Filtering for events where event.data.user == {address_to_check}...")
                for event in all_target_events:
                    event_data_dict = event.get('data')
                    if isinstance(event_data_dict, dict):
                        data_user_value = event_data_dict.get('user')
                        if data_user_value == address_to_check:
                            user_specific_events.append(event)
                current_result["events_found"] = len(user_specific_events)
                print(f"Found {len(user_specific_events)} events for {address_to_check} from the global list.")

            if not user_specific_events:
                print(f"No '{args.event_type}' events found specifically for user {address_to_check} within the fetched global events.")
                print(f"Result for {address_to_check}: Address does NOT meet the staking criteria.")
                results_summary.append(current_result)
                continue

            # Step 2b: Calculate cumulative amount for these user-specific events
            try:
                cumulative_amount = checker.calculate_cumulative_amount(user_specific_events)
                current_result["cumulative_amount"] = cumulative_amount
                print(f"Cumulative amount for '{args.event_type}' by {address_to_check}: {cumulative_amount:,}")
                
                if cumulative_amount > args.threshold:
                    print(f"Result for {address_to_check}: Address MEETS the staking criteria (>{args.threshold:,}).")
                    current_result["meets_criteria"] = True
                else:
                    print(f"Result for {address_to_check}: Address does NOT meet the staking criteria (not >{args.threshold:,}).")
                
            except Exception as e: # Catch errors during calculation for this specific user
                print(f"Error calculating amount for address {address_to_check}: {e}")
                current_result["error"] = str(e)
            
            results_summary.append(current_result)

    # Step 3: Print summary (already exists, no change needed here)
    print("\n" + "="*60)
    print("=== Overall Summary ===")
    print(f"Total addresses processed: {len(addresses_to_check)}")
    
    met_criteria_count = sum(1 for r in results_summary if r["meets_criteria"])
    print(f"Addresses meeting criteria: {met_criteria_count}")
    
    print("-" * 20)
    for result in results_summary:
        status = "MEETS CRITERIA" if result["meets_criteria"] else "DOES NOT MEET"
        error_info = f" (Error: {result['error']})" if result['error'] else ""
        amount_info = f" (Amount: {result['cumulative_amount']:,}, Events: {result['events_found']})" if not result['error'] else ""
        print(f"- {result['address']}: {status}{amount_info}{error_info}")
    print("="*60)

if __name__ == "__main__":
    main() 