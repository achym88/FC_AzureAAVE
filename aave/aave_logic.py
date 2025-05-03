import logging
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, date
from azure.storage.blob.aio import BlobServiceClient
from io import StringIO
import os
import sys

# Přidání cesty ke sdíleným funkcím
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "Shared_Functions"))
from fce_aggregate_orders_Large import aggregate_orders_by_levels

CONTAINER_NAME = "aave"
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
    return f"aave_liquidity_{today.strftime('%Y%m%d')}.csv"

def format_data_for_csv(liquidity_data):
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
    for exchange_data in liquidity_data:
        exchange = exchange_data['exchange']
        price = exchange_data['price']
        for ask_price, ask_quantity, ask_range in exchange_data['orderbook']['asks']:
            rows.append({
                'timestamp': timestamp,
                'exchange': exchange,
                'current_price': price,
                'type': 'ask',
                'level_number': level_mapping[ask_range],
                'level_range': ask_range,
                'price': ask_price,
                'quantity_usd': ask_quantity
            })
        for bid_price, bid_quantity, bid_range in exchange_data['orderbook']['bids']:
            rows.append({
                'timestamp': timestamp,
                'exchange': exchange,
                'current_price': price,
                'type': 'bid',
                'level_number': level_mapping[bid_range],
                'level_range': bid_range,
                'price': bid_price,
                'quantity_usd': bid_quantity
            })
    return pd.DataFrame(rows)

async def fetch_liquidity_data():
    tasks = [
        asyncio.create_task(get_binance_liquidity()),
        asyncio.create_task(get_okx_liquidity()),
        asyncio.create_task(get_bybit_liquidity())
    ]
    results = await asyncio.gather(*tasks)
    valid_results = [r for r in results if r is not None]
    return valid_results

async def save_to_blob_storage(data):
    start_time = datetime.utcnow()
    try:
        df = format_data_for_csv(data)
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

async def get_binance_liquidity():
    orderbook_url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": "AAVEUSDT", "limit": 2000}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(orderbook_url, params=params) as response:
                orderbook_data = await response.json()
                best_bid = float(orderbook_data['bids'][0][0])
                best_ask = float(orderbook_data['asks'][0][0])
                current_price = (best_bid + best_ask) / 2
                aggregated_asks = aggregate_orders_by_levels(orderbook_data['asks'], current_price, True)
                aggregated_bids = aggregate_orders_by_levels(orderbook_data['bids'], current_price, False)
                return {
                    'exchange': 'Binance',
                    'price': current_price,
                    'orderbook': {
                        'asks': aggregated_asks,
                        'bids': aggregated_bids
                    }
                }
    except Exception as e:
        logging.error(f"Binance API error: {e}")
        return None

async def get_okx_liquidity():
    orderbook_url = "https://www.okx.com/api/v5/market/books"
    params = {"instId": "AAVE-USDT", "sz": "400"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(orderbook_url, params=params) as response:
                data = await response.json()
                orderbook_data = data['data'][0]
                bids = [[bid[0], bid[1]] for bid in orderbook_data['bids']]
                asks = [[ask[0], ask[1]] for ask in orderbook_data['asks']]
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                current_price = (best_bid + best_ask) / 2
                aggregated_asks = aggregate_orders_by_levels(asks, current_price, True)
                aggregated_bids = aggregate_orders_by_levels(bids, current_price, False)
                return {
                    'exchange': 'OKX',
                    'price': current_price,
                    'orderbook': {
                        'asks': aggregated_asks,
                        'bids': aggregated_bids
                    }
                }
    except Exception as e:
        logging.error(f"OKX API error: {e}")
        return None

async def get_bybit_liquidity():
    orderbook_url = "https://api.bybit.com/v5/market/orderbook"
    params = {"category": "spot", "symbol": "AAVEUSDT", "limit": 500}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(orderbook_url, params=params) as response:
                data = await response.json()
                orderbook_data = data['result']
                bids = [[bid[0], bid[1]] for bid in orderbook_data['b']]
                asks = [[ask[0], ask[1]] for ask in orderbook_data['a']]
                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                current_price = (best_bid + best_ask) / 2
                aggregated_asks = aggregate_orders_by_levels(asks, current_price, True)
                aggregated_bids = aggregate_orders_by_levels(bids, current_price, False)
                return {
                    'exchange': 'Bybit',
                    'price': current_price,
                    'orderbook': {
                        'asks': aggregated_asks,
                        'bids': aggregated_bids
                    }
                }
    except Exception as e:
        logging.error(f"Bybit API error: {e}")
        return None

async def aave_liquidity_storage_impl(timer):
    logging.info('Azure Function triggered for AAVE liquidity storage by timer.')
    start_time = datetime.utcnow()
    try:
        await initialize_blob_client()
        liquidity_data = await fetch_liquidity_data()
        await save_to_blob_storage(liquidity_data)
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logging.info(f"Total execution time: {execution_time} seconds")
        logging.info("Liquidity data successfully saved to Blob Storage")
    except Exception as e:
        logging.error(f"Error in aave_liquidity_storage function: {str(e)}")