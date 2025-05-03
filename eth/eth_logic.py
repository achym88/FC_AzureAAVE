import logging
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, date
from azure.storage.blob.aio import BlobServiceClient
from io import StringIO
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Shared_Functions"))
from fce_aggregate_orders_by_levels import aggregate_orders_by_levels

CONTAINER_NAME = "ethereum"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M"
BLOB_SERVICE_CLIENT = None

async def initialize_blob_client():
    global BLOB_SERVICE_CLIENT
    if BLOB_SERVICE_CLIENT is None:
        BLOB_SERVICE_CLIENT = BlobServiceClient(
            account_url=f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.blob.core.windows.net",
            credential=os.environ['STORAGE_ACCOUNT_KEY']
        )

def get_csv_filename():
    today = date.today()
    return f"eth_liquidity_{today.strftime('%Y%m%d')}.csv"

async def get_binance_price(symbol):
    """Získá aktuální cenu pro daný symbol"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": symbol}) as response:
                data = await response.json()
                return float(data['price'])
    except Exception as e:
        logging.error(f"Error fetching {symbol} price: {e}")
        return None

async def get_binance_liquidity(symbol):
    orderbook_url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": symbol, "limit": 500}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(orderbook_url, params=params) as response:
                orderbook_data = await response.json()
                best_bid = float(orderbook_data['bids'][0][0])
                best_ask = float(orderbook_data['asks'][0][0])
                current_price = (best_bid + best_ask) / 2

                processed_asks = [[float(ask[0]), float(ask[1]) * float(ask[0])] for ask in orderbook_data['asks']]
                processed_bids = [[float(bid[0]), float(bid[1]) * float(bid[0])] for bid in orderbook_data['bids']]

                aggregated_asks = aggregate_orders_by_levels(processed_asks, current_price, True)
                aggregated_bids = aggregate_orders_by_levels(processed_bids, current_price, False)

                return {
                    'price': current_price,
                    'orderbook': {
                        'asks': aggregated_asks,
                        'bids': aggregated_bids
                    }
                }
    except Exception as e:
        logging.error(f"Binance API error for {symbol}: {e}")
        return None

async def aggregate_usd_liquidity(usdt_data, usdc_data):
    if not usdt_data or not usdc_data:
        return None
    price = usdt_data['price']
    all_asks = usdt_data['orderbook']['asks'] + usdc_data['orderbook']['asks']
    all_bids = usdt_data['orderbook']['bids'] + usdc_data['orderbook']['bids']

    aggregated_asks = aggregate_orders_by_levels(
        [[ask[0], ask[1]] for ask in all_asks],
        price,
        True
    )
    aggregated_bids = aggregate_orders_by_levels(
        [[bid[0], bid[1]] for bid in all_bids],
        price,
        False
    )

    return {
        'price': price,
        'orderbook': {
            'asks': aggregated_asks,
            'bids': aggregated_bids
        }
    }

def format_data_for_csv(liquidity_data, btc_usd_price=None):
    rows = []
    timestamp = datetime.utcnow().strftime(TIMESTAMP_FORMAT)
    level_mapping = {
        "0-0.5%": 1,
        "0.5-1.5%": 2,
        "1.5-3%": 3,
        "0 to -0.5%": -1,
        "-0.5 to -1.5%": -2,
        "-1.5 to -3%": -3
    }

    for quote_asset, exchange_data in liquidity_data.items():
        price = exchange_data['price']
        for ask_price, ask_value, ask_range in exchange_data['orderbook']['asks']:
            value_usd = ask_value if quote_asset == 'USD' else ask_value * btc_usd_price
            rows.append({
                'timestamp': timestamp,
                'quote_asset': quote_asset,
                'current_price': price,
                'type': 'ask',
                'level_number': level_mapping[ask_range],
                'level_range': ask_range,
                'price': ask_price,
                'value_usd': value_usd
            })
        for bid_price, bid_value, bid_range in exchange_data['orderbook']['bids']:
            value_usd = bid_value if quote_asset == 'USD' else bid_value * btc_usd_price
            rows.append({
                'timestamp': timestamp,
                'quote_asset': quote_asset,
                'current_price': price,
                'type': 'bid',
                'level_number': level_mapping[bid_range],
                'level_range': bid_range,
                'price': bid_price,
                'value_usd': value_usd
            })
    return pd.DataFrame(rows)

async def save_to_blob_storage(data, btc_usd_price):
    start_time = datetime.utcnow()
    try:
        df = format_data_for_csv(data, btc_usd_price)
        filename = get_csv_filename()

        async with BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME) as container_client:
            try:
                blob_client = container_client.get_blob_client(filename)
                existing_data = await blob_client.download_blob()
                existing_content = await existing_data.content_as_text()
                existing_df = pd.read_csv(StringIO(existing_content))
                df = pd.concat([existing_df, df], ignore_index=True)
            except Exception:
                logging.info(f"Creating new file {filename}")

            csv_data = df.to_csv(index=False)
            await container_client.upload_blob(name=filename, data=csv_data, overwrite=True)

            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logging.info(f"CSV storage operation time: {execution_time} seconds")
            logging.info(f"Data successfully appended to CSV: {filename}")
    except Exception as e:
        logging.error(f"Error saving to blob storage: {str(e)}")
        raise

async def eth_liquidity_storage_impl(timer):
    logging.info('Azure Function triggered for ETH liquidity storage by timer.')
    start_time = datetime.utcnow()

    try:
        await initialize_blob_client()

        # Paralelní volání všech API
        usdt_data, usdc_data, btc_data, btc_usd_price = await asyncio.gather(
            get_binance_liquidity("ETHUSDT"),
            get_binance_liquidity("ETHUSDC"),
            get_binance_liquidity("ETHBTC"),
            get_binance_price("BTCUSDT")
        )

        # Agregace USD dat
        usd_data = await aggregate_usd_liquidity(usdt_data, usdc_data)

        if usd_data and btc_data and btc_usd_price:
            combined_data = {
                'USD': usd_data,
                'BTC': btc_data
            }

            await save_to_blob_storage(combined_data, btc_usd_price)
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logging.info(f"Total execution time: {execution_time} seconds")
            logging.info("ETH liquidity data successfully saved to Blob Storage")
        else:
            logging.error("Failed to fetch complete liquidity data")

    except Exception as e:
        logging.error(f"Error in eth_liquidity_storage function: {str(e)}")