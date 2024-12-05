import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

class BinanceDataFetcher:
    def __init__(self):
        self.base_url_spot = "https://api.binance.com/api/v3/klines"
        self.base_url_perp = "https://fapi.binance.com/fapi/v1/klines"
        self.all_pairs_data = []
        # Rate limiting parameters
        self.request_window = 1.0  # 1 second window
        self.max_requests_per_second = 1000
        self.request_timestamps = []
        self.batch_delay = 0.1  # 100ms between batches

    async def check_rate_limit(self):
        """Check and control rate limiting"""
        current_time = time.time()
        # Remove timestamps older than our window
        self.request_timestamps = [ts for ts in self.request_timestamps 
                                 if current_time - ts <= self.request_window]
        
        # If we've hit the limit, wait
        if len(self.request_timestamps) >= self.max_requests_per_second:
            wait_time = self.request_timestamps[0] + self.request_window - current_time
            if wait_time > 0:
                print(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
        
        # Add current request timestamp
        self.request_timestamps.append(current_time)

    async def fetch_historical_candles(self, session, base_url, pair, start_time, end_time):
        """Fetch historical candles for a given pair"""
        pair_for_binance = pair.replace("/", "").replace("-", "")
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + timedelta(days=1), end_time)
            
            params = {
                "symbol": pair_for_binance,
                "interval": "5m",
                "startTime": int(current_start.timestamp() * 1000),
                "endTime": int(current_end.timestamp() * 1000),
                "limit": 1000
            }

            try:
                # Check rate limit before making request
                await self.check_rate_limit()
                
                async with session.get(base_url, params=params) as response:
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
            await asyncio.sleep(self.batch_delay)  # Delay between requests

        return all_candles if all_candles else None

    async def fetch_pair_data(self, session, pair):
        """Fetch both spot and perpetual data for a pair"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=1)

        try:
            # Fetch spot data
            spot_data = await self.fetch_historical_candles(
                session, self.base_url_spot, pair, start_time, end_time
            )
            
            # Fetch perpetual data
            perp_data = await self.fetch_historical_candles(
                session, self.base_url_perp, pair, start_time, end_time
            )

            if spot_data or perp_data:
                processed_data = []
                
                if spot_data:
                    for candle in spot_data:
                        processed_candle = {
                            "market": pair,
                            "candle_date_time_utc": datetime.utcfromtimestamp(candle[0]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                            "opening_price": float(candle[1]),
                            "high_price": float(candle[2]),
                            "low_price": float(candle[3]),
                            "close_price": float(candle[4]),
                            "volume": float(candle[5]),
                            "quote_volume": float(candle[7]),
                            "market_type": "spot"
                        }
                        processed_data.append(processed_candle)
                
                elif perp_data:
                    for candle in perp_data:
                        processed_candle = {
                            "market": pair,
                            "candle_date_time_utc": datetime.utcfromtimestamp(candle[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                            "opening_price": float(candle[1]),
                            "high_price": float(candle[2]),
                            "low_price": float(candle[3]),
                            "close_price": float(candle[4]),
                            "volume": float(candle[5]),
                            "quote_volume": float(candle[7]),
                            "market_type": "perpetual"
                        }
                        processed_data.append(processed_candle)

                self.all_pairs_data.extend(processed_data)
                #print(self.all_pairs_data)
                return processed_data

        except Exception as e:
            raise e
            return None

    async def fetch_current_data(self, session, pair):
        """Fetch current data (e.g., last 5 minutes)"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)  # последние 5 минут

        # Запрос текущих данных для пары
        spot_data = await self.fetch_historical_candles(
            session, self.base_url_spot, pair, start_time, end_time
        )
        
        perp_data = await self.fetch_historical_candles(
            session, self.base_url_perp, pair, start_time, end_time
        )

        # Возвращаем собранные данные
        return spot_data, perp_data

    async def fetch_all_pairs(self, pairs):
        """Fetch data for all pairs"""
        batch_size = 5  # Process pairs in smaller batches
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(pairs), batch_size):
                batch = pairs[i:i + batch_size]
                tasks = [self.fetch_pair_data(session, pair) for pair in batch]
                # сохранять данные!!!!
                results = await asyncio.gather(*tasks)
                valid_results = [r for r in results if r is not None]
                
                # Delay between batches
                await asyncio.sleep(self.batch_delay)
                print(f"Processed batch {i//batch_size + 1}/{len(pairs)//batch_size + 1}")

            return [r for r in results if r is not None]

    def save_to_csv(self):
        """Save the collected data to CSV"""
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

    def run(self, pairs):
        """Main execution method"""
        asyncio.run(self.fetch_all_pairs(pairs))
        self.save_to_csv()

# Пример использования
if __name__ == "__main__":
    pairs = ['BTC/USDT', 'NEO/USDT', 'ETC/USDT', 'QTUM/USDT', 'SNT/USDT', 'ETH/USDT',
            'XRP/USDT', 'MTL/USDT', 'STEEM/USDT', 'XLM/USDT', 'ARDR/USDT', 'ARK/USDT',
            'LSK/USDT', 'STORJ/USDT', 'ADA/USDT', 'POWR/USDT', 'ICX/USDT', 'EOS/USDT',
            'TRX/USDT', 'SC/USDT', 'ZIL/USDT', 'ONT/USDT', 'POLYX/USDT', 'ZRX/USDT',
            'BAT/USDT', 'BCH/USDT', 'IOST/USDT', 'CVC/USDT', 'IQ/USDT', 'IOTA/USDT',
            'HIFI/USDT', 'ONG/USDT', 'GAS/USDT', 'ELF/USDT', 'KNC/USDT', 'BSV/USDT',
            'TFUEL/USDT', 'THETA/USDT', 'QKC/USDT', 'MANA/USDT', 'ANKR/USDT',
            'AERGO/USDT', 'ATOM/USDT', 'WAXP/USDT', 'HBAR/USDT', 'MBL/USDT',
            'STPT/USDT', 'ORBS/USDT', 'VET/USDT', 'CHZ/USDT', 'STMX/USDT',
            'HIVE/USDT', 'KAVA/USDT', 'LINK/USDT', 'XTZ/USDT', 'JST/USDT', 
            'TON/USDT', 'SXP/USDT', 'DOT/USDT', 'STRAX/USDT', 'GLM/USDT',
            'SAND/USDT', 'DOGE/USDT', 'PUNDIX/USDT', 'FLOW/USDT', 'AXS/USDT',
            'SOL/USDT', 'STX/USDT', 'POL/USDT', 'XEC/USDT', '1INCH/USDT', 'AAVE/USDT',
            'ALGO/USDT', 'NEAR/USDT', 'AVAX/USDT', 'GMT/USDT', 'SHIB/USDT', 
            'CELO/USDT', 'T/USDT', 'ARB/USDT', 'EGLD/USDT', 'APT/USDT', 'MASK/USDT',
            'GRT/USDT', 'SUI/USDT', 'SEI/USDT', 'MINA/USDT', 'BLUR/USDT',
            'IMX/USDT', 'ID/USDT', 'PYTH/USDT', 'ASTR/USDT', 'AKT/USDT',
            'ZETA/USDT', 'AUCTION/USDT', 'STG/USDT', 'ONDO/USDT', 'ZRO/USDT',
            'JUP/USDT', 'ENS/USDT', 'G/USDT', 'PENDLE/USDT', 'USDC/USDT',
            'UXLINK/USDT', 'BIGTIME/USDT', 'CKB/USDT', 'W/USDT', 'INJ/USDT',
            'UNI/USDT', 'MEW/USDT', 'SAFE/USDT', 'DRIFT/USDT', 'AGLD/USDT',
            'PEPE/USDT', 'BONK/USDT', 'WAVES/USDT', 'XEM/USDT', 'GRS/USDT',
            'SBD/USDT', 'BTG/USDT', 'LOOM/USDT', 'BOUNTY/USDT',
            'BTT/USDT', 'MOC/USDT', 'TT/USDT', 'GAME2/USDT', 
            'MLK/USDT', 'MED/USDT', 'DKA/USDT', 'AHT/USDT', 
            'BORA/USDT', 'CRO/USDT', 'HUNT/USDT', 'MVL/USDT', 
            'AQT/USDT', 'META/USDT', 'FCT2/USDT', 'CBK/USDT', 
            'HPO/USDT', 'STRIKE/USDT', 'CTC/USDT', 'MNT/USDT', 
            'BEAM/USDT', 'BLAST/USDT', 'TAIKO/USDT', 'ATH/USDT', 'CARV/USDT']

    fetcher = BinanceDataFetcher()
    fetcher.run(pairs)