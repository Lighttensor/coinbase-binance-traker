import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

class CoinbaseDataFetcher:
    def __init__(self):
        self.base_url = "https://api.exchange.coinbase.com/products"
        self.all_pairs_data = []
        self.request_window = 1.0
        self.max_requests_per_second = 10
        self.request_timestamps = []
        self.batch_delay = 0.2

    async def get_available_pairs(self, session):
        """Fetch all available trading pairs from Coinbase"""
        try:
            async with session.get(self.base_url) as response:
                if response.status == 200:
                    products = await response.json()
                    # Filter for USD pairs and status 'online'
                    usd_pairs = [
                        product['id'] for product in products 
                        if product['quote_currency'] == 'USD' 
                        and product['status'] == 'online'
                        and product['trading_disabled'] is False
                    ]
                    print(f"Found {len(usd_pairs)} USD trading pairs")
                    return usd_pairs
                else:
                    print(f"Error fetching available pairs: {response.status}")
                    return []
        except Exception as e:
            print(f"Error fetching available pairs: {e}")
            return []

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

    async def fetch_historical_candles(self, session, pair, start_time, end_time):
        """Fetch historical candles for a pair"""
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + timedelta(days=1), end_time)
            
            params = {
                'start': current_start.isoformat(),
                'end': current_end.isoformat(),
                'granularity': 300
            }

            try:
                await self.check_rate_limit()
                
                url = f"{self.base_url}/{pair}/candles"
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        candles = await response.json()
                        if candles:
                            all_candles.extend(candles)
                            print(f"Fetched {len(candles)} candles for {pair} from {current_start} to {current_end}")
                        else:
                            print(f"No data for {pair} from {current_start} to {current_end}")
                    else:
                        print(f"Error {response.status} fetching data for {pair}")
                        return None

            except Exception as e:
                print(f"Error fetching data for {pair}: {e}")
                return None

            current_start = current_end
            await asyncio.sleep(self.batch_delay)

        return all_candles if all_candles else None

    async def fetch_pair_data(self, session, pair):
        """Process data for a single pair"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=1)

        try:
            candles = await self.fetch_historical_candles(
                session, pair, start_time, end_time
            )

            if candles:
                processed_data = []
                for candle in candles:
                    processed_candle = {
                        "market": pair,
                        "candle_date_time_utc": datetime.utcfromtimestamp(candle[0]).strftime('%Y-%m-%d %H:%M:%S'),
                        "opening_price": float(candle[3]),
                        "high_price": float(candle[2]),
                        "low_price": float(candle[1]),
                        "close_price": float(candle[4]),
                        "volume": float(candle[5]),
                        "market_type": "spot"
                    }
                    processed_data.append(processed_candle)

                self.all_pairs_data.extend(processed_data)
                return processed_data

        except Exception as e:
            print(f"Error processing data for {pair}: {e}")
            return None

    async def fetch_all_pairs(self):
        """Fetch data for all available pairs"""
        async with aiohttp.ClientSession() as session:
            # First, get all available pairs
            available_pairs = await self.get_available_pairs(session)
            if not available_pairs:
                print("No trading pairs found")
                return
            
            print(f"Starting to fetch data for {len(available_pairs)} pairs")
            
            # Process pairs in batches
            batch_size = 3
            for i in range(0, len(available_pairs), batch_size):
                batch = available_pairs[i:i + batch_size]
                tasks = [self.fetch_pair_data(session, pair) for pair in batch]
                results = await asyncio.gather(*tasks)
                valid_results = [r for r in results if r is not None]
                
                await asyncio.sleep(self.batch_delay)
                print(f"Processed batch {i//batch_size + 1}/{len(available_pairs)//batch_size + 1}")

    def save_to_csv(self):
        """Save collected data to CSV"""
        if self.all_pairs_data:
            df = pd.DataFrame(self.all_pairs_data)
            
            try:
                existing_df = pd.read_csv("coinbase_data.csv")
                combined_df = pd.concat([existing_df, df])
                combined_df = combined_df.drop_duplicates(
                    subset=['market', 'candle_date_time_utc', 'market_type'],
                    keep='last'
                )
                combined_df = combined_df.sort_values(['market', 'candle_date_time_utc'])
                combined_df.to_csv("coinbase_data.csv", index=False)
                print(f"Updated data saved: {len(combined_df)} records")
                
            except FileNotFoundError:
                df.to_csv("coinbase_data.csv", index=False)
                print(f"New file created with {len(df)} records")
        else:
            print("No data to save")

    def run(self):
        """Main execution method"""
        asyncio.run(self.fetch_all_pairs())
        self.save_to_csv()

if __name__ == "__main__":
    fetcher = CoinbaseDataFetcher()
    fetcher.run()