from datetime import datetime
import json
import os
import pytz
import requests
import boto3 # AWS Library

# Define Yangon Timezone
yangon_tz = pytz.timezone("Asia/Yangon")

# Set up Kinesis Client
kinesis_client = boto3.client("kinesis", region_name="ap-southeast-1")
STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "crypto-stream-1")

# CoinGecko API URL
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether&vs_currencies=usd,thb"

# ETL
def lambda_handler(event, context):
    # Get current time in Yangon timezone
    now = datetime.now(yangon_tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Fetch cryptocurrency data
    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        data = response.json()

        # Extract crypto prices
        bitcoin_usd = data["bitcoin"]["usd"]
        bitcoin_thb = data["bitcoin"]["thb"]
        ethereum_usd = data["ethereum"]["usd"]
        ethereum_thb = data["ethereum"]["thb"]
        tether_usd = data["tether"]["usd"]
        tether_thb = data["tether"]["thb"]

        # Prepare the structured data payload
        data_to_send = {
            "timestamp": timestamp,
            "bitcoin_usd": bitcoin_usd,
            "bitcoin_thb": bitcoin_thb,
            "ethereum_usd": ethereum_usd,
            "ethereum_thb": ethereum_thb,
            "tether_usd": tether_usd,
            "tether_thb": tether_thb,
        }

        try:
            # Convert to JSON line and send to Kinesis Data Stream
            data_json = json.dumps(data_to_send)

            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=data_json.encode("utf-8"),
                PartitionKey="crypto-prices-NRT"
            )

            print(f"Data successfully sent to Kinesis at {timestamp} (Yangon Time)")
            return {
                "statusCode": 200,
                "body": json.dumps("Data sent to Kinesis successfully"),
            }
        
        except Exception as e:
            print(f"Error sending to Kinesis: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps(f"Error: {str(e)}")
            }
    
    else:
        print("Error fetching data from CoinGecko API")
        return {
            "statusCode": 500,
            "body": json.dumps("Error fetching data from CoinGecko API"),
        }

#if __name__ == "__main__":
    # Local Test
#    print("Running local test...")
#    result = lambda_handler(None, None)
#    print("Result: ", result)