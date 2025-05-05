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
from fce_aggregate_orders_Medium import aggregate_orders_by_levels_medium

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

async def get_binance_volume(symbol, minutes=3):
    """Získá objem obchodů za posledních X minut"""
    trades_url = "https://api.binance.com/api/v3/trades"
    params = {
        "symbol": symbol,
        "limit": 1000  # maximální limit pro jeden request
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(trades_url, params=params) as response:
                trades = await response.json()

        # Získat časovou hranici (3 minuty zpět)
        time_threshold = datetime.utcnow().timestamp() * 1000 - (minutes * 60 * 1000)

        # Filtrovat a sečíst objemy obchodů za posledních 3 minuty
        recent_volume = sum(
            float(trade['price']) * float(trade['qty'])
            for trade in trades
            if float(trade['time']) > time_threshold
        )

        return recent_volume
    except Exception as e:
        logging.error(f"Error fetching {symbol} volume: {e}")
        return None

def format_data_for_csv(liquidity_data):
    rows = []
    timestamp = datetime.utcnow().strftime(TIMESTAMP_FORMAT)
    level_mapping = {
        "0-0.25%": 1,
        "0.25-1%": 2,
        "0 to -0.25%": -1,
        "-0.25 to -1%": -2
    }
    
    exchange_data = liquidity_data
    exchange = 'Binance'
    price = exchange_data['price']
    volume_3min = exchange_data.get('volume_3min', 0)
    
    for ask_price, ask_quantity, ask_range in exchange_data['orderbook']['asks']:
        rows.append({
            'timestamp': timestamp,
            'exchange': exchange,
            'current_price': price,
            'type': 'ask',
            'level_number': level_mapping[ask_range],
            'level_range': ask_range,
            'price': ask_price,
            'quantity_usd': ask_quantity,
            'volume_3min_usd': volume_3min
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
            'quantity_usd': bid_quantity,
            'volume_3min_usd': volume_3min
        })
    return pd.DataFrame(rows)

async def fetch_liquidity_data():
    result = await get_binance_liquidity()
    return result

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
    params = {"symbol": "AAVEUSDT", "limit": 1000}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(orderbook_url, params=params) as response:
                orderbook_data = await response.json()
                best_bid = float(orderbook_data['bids'][0][0])
                best_ask = float(orderbook_data['asks'][0][0])
                current_price = (best_bid + best_ask) / 2
                
                # Přepočet na USD hodnoty
                processed_asks = [[float(ask[0]), float(ask[1]) * float(ask[0])] for ask in orderbook_data['asks']]
                processed_bids = [[float(bid[0]), float(bid[1]) * float(bid[0])] for bid in orderbook_data['bids']]
                
                aggregated_asks = aggregate_orders_by_levels_medium(processed_asks, current_price, True)
                aggregated_bids = aggregate_orders_by_levels_medium(processed_bids, current_price, False)
                
                # Získání objemu obchodů za poslední 3 minuty
                volume_3min = await get_binance_volume("AAVEUSDT", minutes=3)
                
                return {
                    'price': current_price,
                    'orderbook': {
                        'asks': aggregated_asks,
                        'bids': aggregated_bids
                    },
                    'volume_3min': volume_3min
                }
    except Exception as e:
        logging.error(f"Binance API error: {e}")
        return None

async def aave_liquidity_storage_impl(timer):
    logging.info('Azure Function triggered for AAVE liquidity storage by timer.')
    start_time = datetime.utcnow()
    try:
        await initialize_blob_client()
        liquidity_data = await fetch_liquidity_data()
        if liquidity_data:
            await save_to_blob_storage(liquidity_data)
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logging.info(f"Total execution time: {execution_time} seconds")
            logging.info("Liquidity data successfully saved to Blob Storage")
        else:
            logging.error("Failed to fetch complete liquidity data")
    except Exception as e:
        logging.error(f"Error in aave_liquidity_storage function: {str(e)}")