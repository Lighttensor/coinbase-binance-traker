import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

class BinanceDataFetcher:
    def __init__(self, target_pairs):
        self.base_url = "https://api.binance.com/api/v3/klines"
        self.all_pairs_data = []
        self.request_window = 1.0
        self.max_requests_per_second = 1000
        self.request_timestamps = []
        self.batch_delay = 0.5
        self.target_pairs = target_pairs
        self.start_time = None

    async def check_rate_limit(self):
        """Rate limit check"""
        current_time = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps 
                                 if current_time - ts <= self.request_window]
        
        if len(self.request_timestamps) >= self.max_requests_per_second:
            wait_time = self.request_timestamps[0] + self.request_window - current_time
            if wait_time > 0:
                print(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
        
        self.request_timestamps.append(current_time)

    async def fetch_historical_candles(self, session, pair, start_time, end_time, retries=3, backoff_factor=1.5):
        """Fetch historical candles with retries and proper interval handling"""
        pair_for_binance = pair.replace("/", "")
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            current_batch_end = min(current_start + timedelta(days=1), end_time)
            attempt = 0

            while attempt < retries:
                try:
                    await self.check_rate_limit()

                    params = {
                        "symbol": pair_for_binance,
                        "interval": "5m",
                        "startTime": int(current_start.timestamp() * 1000),
                        "endTime": int(current_batch_end.timestamp() * 1000),
                        "limit": 1000
                    }

                    async with session.get(self.base_url, params=params) as response:
                        if response.status == 200:
                            candles = await response.json()
                            if candles:
                                all_candles.extend(candles)
                                print(f"Fetched {len(candles)} candles for {pair} from {current_start} to {current_batch_end}")
                            else:
                                print(f"No data for {pair} from {current_start} to {current_batch_end}")
                            break
                        else:
                            print(f"Error {response.status} fetching data for {pair}")
                            raise Exception(f"HTTP {response.status}")

                except Exception as e:
                    print(f"Error fetching data for {pair}: {e}")
                    attempt += 1
                    if attempt < retries:
                        await asyncio.sleep(backoff_factor ** attempt)
                    else:
                        print(f"Max retries reached for {pair}. Skipping.")
                        break

            current_start = current_batch_end
            await asyncio.sleep(self.batch_delay)

        return all_candles if all_candles else None

    async def fetch_pair_data(self, session, pair, start_time=None, end_time=None):
        """Process data for a single pair"""
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(days=1)

        try:
            candles = await self.fetch_historical_candles(
                session, pair, start_time, end_time
            )
            
            if candles:
                processed_data = []
                for candle in candles:
                    candle_time = datetime.fromtimestamp(candle[0]/1000, tz=timezone.utc)
                    processed_candle = {
                        "market": pair,
                        "candle_date_time_utc": candle_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "opening_price": float(candle[1]),
                        "high_price": float(candle[2]),
                        "low_price": float(candle[3]),
                        "close_price": float(candle[4]),
                        "volume": float(candle[5]),
                        "quote_volume": float(candle[7]),
                        "market_type": "spot"
                    }
                    processed_data.append(processed_candle)
                
                self.all_pairs_data.extend(processed_data)
                return processed_data

        except Exception as e:
            print(f"Error processing data for {pair}: {e}")
            return None

    async def fetch_all_pairs(self, start_time=None, end_time=None):
        """Fetch data for specified pairs"""
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(days=1)

        if not self.target_pairs:
            print("No target pairs specified")
            return

        async with aiohttp.ClientSession() as session:
            batch_size = 5
            for i in range(0, len(self.target_pairs), batch_size):
                batch = self.target_pairs[i:i + batch_size]
                tasks = [self.fetch_pair_data(session, pair, start_time, end_time) 
                        for pair in batch]
                results = await asyncio.gather(*tasks)
                await asyncio.sleep(self.batch_delay)

    def save_to_csv(self):
        """Save collected data to CSV"""
        if self.all_pairs_data:
            df = pd.DataFrame(self.all_pairs_data)
            
            try:
                existing_df = pd.read_csv("binance_data.csv")
                combined_df = pd.concat([existing_df, df])
                combined_df = combined_df.drop_duplicates(
                    subset=['market', 'candle_date_time_utc', 'market_type'],
                    keep='last'
                )
                combined_df = combined_df.sort_values(['market', 'candle_date_time_utc'])
                combined_df.to_csv("binance_data.csv", index=False)
                print(f"Updated data saved: {len(combined_df)} records")
                
            except FileNotFoundError:
                df.to_csv("binance_data.csv", index=False)
                print(f"New file created with {len(df)} records")
        else:
            print("No data to save")

    async def run(self):
        """Main execution method"""
        await self.fetch_all_pairs()
        self.save_to_csv()
