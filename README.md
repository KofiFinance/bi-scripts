# Aptos GraphQL Query Script

This Python script fetches current fungible asset balances from the Aptos blockchain using GraphQL with automatic pagination support.

## Features

- **Automatic Pagination**: Continues fetching data until all records are retrieved
- **Rate Limiting**: Built-in delays between requests to avoid API rate limits
- **Error Handling**: Robust error handling for network and API issues
- **Data Export**: Saves results to JSON file
- **Summary Statistics**: Provides useful statistics about the fetched data

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Run the script with the default asset type:
```bash
python aptos_graphql_query.py
```

### Customization

You can modify the script to:

1. **Change the asset type**: Edit the `asset_type` variable in the `main()` function
2. **Adjust pagination limit**: Modify the `limit` parameter (max 100 for Aptos API)
3. **Change request delay**: Adjust `delay_between_requests` to control rate limiting
4. **Customize output filename**: Modify the filename in `save_to_json()` call

### Example Output

```
Starting to fetch balances for asset type: 0x821c94e69bc7ca058c913b7b5e6b0a5c9fd1523d58723a966fb8c1f5ea888105
Using limit: 100, delay: 0.1s
------------------------------------------------------------
Fetching page 1 (offset: 0)...
Retrieved 100 records (total so far: 100)
Fetching page 2 (offset: 100)...
Retrieved 100 records (total so far: 200)
...
------------------------------------------------------------
Pagination complete. Total records fetched: 1250

Summary:
Total records: 1250
Unique owners: 1180
Total amount: 50000000000
Average balance: 40000000.00

Top 5 balances:
1. 0x1234...5678: 1000000000
2. 0x2345...6789: 500000000
3. 0x3456...789a: 250000000
4. 0x4567...89ab: 100000000
5. 0x5678...9abc: 75000000

Data saved to aptos_balances.json
Script completed successfully!
```

## API Details

The script queries the Aptos mainnet GraphQL endpoint:
- **Endpoint**: https://api.mainnet.aptoslabs.com/v1/graphql
- **Query**: `current_fungible_asset_balances`
- **Asset Type**: `0x821c94e69bc7ca058c913b7b5e6b0a5c9fd1523d58723a966fb8c1f5ea888105`

## Data Fields

The script fetches the following fields for each balance record:
- `amount`: The balance amount
- `asset_type`: The asset type identifier
- `owner_address`: The address of the balance owner
- `storage_id`: Storage identifier
- `is_frozen`: Whether the balance is frozen
- `is_primary`: Whether this is a primary balance
- `last_transaction_timestamp`: Timestamp of last transaction
- `last_transaction_version`: Version of last transaction
- `token_standard`: Token standard used

## Error Handling

The script includes comprehensive error handling for:
- Network connectivity issues
- API rate limiting
- Invalid responses
- JSON parsing errors
- Keyboard interruption (Ctrl+C)

## Rate Limiting

To be respectful to the Aptos API, the script includes:
- Configurable delays between requests (default: 100ms)
- Proper HTTP headers
- Session reuse for connection pooling

## Output Files

- `aptos_balances.json`: Complete dataset in JSON format
- Console output: Real-time progress and summary statistics 