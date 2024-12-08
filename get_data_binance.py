import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

class BinanceDataFetcher:
    def __init__(self):
        self.base_url_spot = "https://api.binance.com/api/v3/klines"
        self.all_pairs_data = []
        self.start_time = None 
        self.request_window = 1.0  # 1 second window
        self.max_requests_per_second = 1000
        self.request_timestamps = []
        self.batch_delay = 0.5

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

    async def fetch_historical_candles(self, session, base_url, pair, start_time=None, end_time=None):
        """Fetch historical candles for a given pair with proper interval handling."""
        pair_for_binance = pair.replace("/", "").replace("-", "")
        all_candles = []

        # Устанавливаем начальные значения времени
        current_start = start_time or self.start_time or (datetime.now(timezone.utc) - timedelta(days=1))
        current_end = end_time or datetime.now(timezone.utc)

        while current_start < current_end:
            # Разбиваем запросы на 1-дневные интервалы (максимальный объем данных за один запрос у Binance)
            current_batch_end = min(current_start + timedelta(days=1), current_end)

            params = {
                "symbol": pair_for_binance,
                "interval": "5m",
                "startTime": int(current_start.timestamp() * 1000),  # Начало периода в миллисекундах
                "endTime": int(current_batch_end.timestamp() * 1000),  # Конец периода в миллисекундах
                "limit": 1000  # Максимум данных за один запрос
            }

            try:
                # Проверка ограничения скорости запросов
                await self.check_rate_limit()

                # Выполнение запроса
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        candles = await response.json()
                        if candles:
                            all_candles.extend(candles)
                            print(f"Fetched {len(candles)} candles for {pair} from {current_start} to {current_batch_end}")
                        else:
                            print(candles)
                            print(f"No data for {pair} from {current_start} to {current_batch_end}")
                    else:
                        print(f"Error {response.status} fetching data for {pair}")
                        break

            except Exception as e:
                print(f"Error fetching data for {pair}: {e}")
                break

            # Переходим к следующему интервалу
            current_start = current_batch_end
            await asyncio.sleep(self.batch_delay)  # Задержка между запросами

        return all_candles if all_candles else None

    # Modified fetch_pair_data method in BinanceDataFetcher
    async def fetch_pair_data(self, session, pair, start_time=None, end_time=None):
        """Fetch data for a specific pair"""
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(days=1)

        try:
            spot_data = await self.fetch_historical_candles(
                session, self.base_url_spot, pair, start_time, end_time
            )
            
            if spot_data:
                processed_data = []
                for candle in spot_data:
                    # Убедимся, что временная метка имеет UTC
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
            print(f"Error fetching data for {pair}: {e}")
            return None

    async def fetch_current_data(self, session, pair):
        """Fetch current data (e.g., last 5 minutes)"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)  # последние 5 минут

        # Запрос текущих данных для пары
        spot_data = await self.fetch_historical_candles(
            session, self.base_url_spot, pair, start_time, end_time
        )

        # Возвращаем собранные данные
        return spot_data

    async def fetch_all_pairs(self, pairs, start_time=None, end_time=None):
        """Fetch data for all pairs"""
        if not end_time:
            end_time = datetime.now(timezone.utc)
        if not start_time:
            start_time = end_time - timedelta(days=365) # Determine start of the historical period (default 365 d)
        
        batch_size = 5
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(pairs), batch_size):
                batch = pairs[i:i + batch_size]
                tasks = [self.fetch_pair_data(session, pair, start_time, end_time) 
                        for pair in batch]
                results = await asyncio.gather(*tasks)
                await asyncio.sleep(self.batch_delay)

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
    pairs = ['BLZ/USDT', 'STG/USDT', 'IO/USDT', 'UMA/USDT', 'SAND/USDT', 'MANA/USDT', 'OP/USDT', 'PYR/USDT', 'CHZ/USDT', 'ZK/USDT', 'ZRO/USDT', 'BNT/USDT', 'ORCA/USDT', 'POWR/USDT', 'PERP/USDT', 'FIL/USDT', 'SEI/USDT', 'RONIN/USDT', 'VOXEL/USDT', 'PUNDIX/USDT', 'ADA/USDT', 'SUPER/USDT', 'SUI/USDT', 'ANKR/USDT', 'GHST/USDT', 'EIGEN/USDT', 'SOL/USDT', 'ALGO/USDT', 'AUDIO/USDT', 'BICO/USDT', 'JASMY/USDT', 'MINA/USDT', 'MLN/USDT', 'INJ/USDT', 'IOTX/USDT', 'AAVE/USDT', 'TRB/USDT', 'AMP/USDT', 'STX/USDT', 'STRK/USDT', 'DAR/USDT', 'COMP/USDT', 'METIS/USDT', 'MKR/USDT', 'ARKM/USDT', 'BONK/USDT', 'DASH/USDT', 'XLM/USDT', 'EOS/USDT', 'XTZ/USDT', 'KAVA/USDT', 'AUCTION/USDT', 'BLUR/USDT', 'LRC/USDT', 'RENDER/USDT', 'UNI/USDT', 'XRP/USDT', 'HIGH/USDT', 'LTC/USDT', 'WIF/USDT', 'ALICE/USDT', 'MDT/USDT', 'RLC/USDT', 'ETH/USDT', 'AXS/USDT', 'ICP/USDT', 'OMNI/USDT', 'TRU/USDT', 'HFT/USDT', 'REQ/USDT', 'KNC/USDT', 'FLOW/USDT', 'LPT/USDT', 'GNO/USDT', 'PEPE/USDT', 'ARB/USDT', 'POND/USDT', 'KSM/USDT', 'AGLD/USDT', 'QNT/USDT', 'FORTH/USDT', 'BAL/USDT', 'GLM/USDT', 'YFI/USDT', 'ZEN/USDT', 'MASK/USDT', 'ZRX/USDT', 'COTI/USDT', 'CLV/USDT', 'C98/USDT', 'SKL/USDT', 'LINK/USDT', 'LOKA/USDT', 'ZEC/USDT', 'ALCX/USDT', 'FARM/USDT', 'RPL/USDT', 'SPELL/USDT', 'DOGE/USDT', 'FLOKI/USDT', 'ERN/USDT', 'VET/USDT', 'FIDA/USDT', 'HBAR/USDT', 'ATOM/USDT', 'ROSE/USDT', 'ACX/USDT', 'SYN/USDT', 'TIA/USDT', 'DIA/USDT', 'FET/USDT', 'RARE/USDT', 'CVX/USDT', 'SHIB/USDT', 'NEAR/USDT', 'IMX/USDT', 'CELR/USDT', 'BAT/USDT', 'MAGIC/USDT', 'API3/USDT', 'T/USDT', 'CTSI/USDT', 'BAND/USDT', 'ENS/USDT', 'GMT/USDT', 'QI/USDT', 'LIT/USDT', 'RAD/USDT', 'G/USDT', 'NMR/USDT', 'IDEX/USDT', 'WBTC/USDT', 'COW/USDT', 'GTC/USDT', 'AERGO/USDT', 'ILV/USDT', 'AST/USDT', 'ARPA/USDT', 'DOT/USDT', 'CRV/USDT', 'ETC/USDT', 'JTO/USDT', 'APT/USDT', 'SNX/USDT', 'EGLD/USDT', 'BADGER/USDT', 'STORJ/USDT', 'CVC/USDT', 'OGN/USDT', 'VTHO/USDT', 'APE/USDT', 'AVAX/USDT', 'OSMO/USDT', 'GRT/USDT', 'ACH/USDT', 'BTC/USDT', 'TNSR/USDT', '1INCH/USDT', 'LQTY/USDT', 'AXL/USDT', 'OXT/USDT', 'FIS/USDT', 'LDO/USDT', 'POL/USDT', 'BCH/USDT', 'SUSHI/USDT', 'NKN/USDT']
 
    fetcher = BinanceDataFetcher()
    fetcher.run(pairs)