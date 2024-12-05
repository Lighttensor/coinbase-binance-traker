import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone

class CoinbaseDataFetcher:
    def __init__(self, pairs, granularity=300):
        self.base_url = "https://api.exchange.coinbase.com/products"
        self.pairs = pairs  # Пары для запроса
        self.granularity = granularity  # Интервал свечей (в секундах, 300 = 5 минут)
        self.all_candles_data = []
        self.is_first_run = True  # Флаг для определения первого запуска

    async def fetch_historical_data(self, session, pair, start_time, end_time):
        """Fetch historical candles for a specific pair in chunks."""
        all_candles = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + timedelta(seconds=self.granularity * 300), end_time)
            params = {
                'start': current_start.isoformat(),
                'end': current_end.isoformat(),
                'granularity': self.granularity
            }
            url = f"{self.base_url}/{pair}/candles"
            try:
                async with session.get(url, params=params) as response:
                    response.raise_for_status()
                    candles = await response.json()
                    for candle in candles:
                        all_candles.append({
                            'market': pair,
                            'candle_date_time_utc': datetime.utcfromtimestamp(candle[0]).strftime('%Y-%m-%d %H:%M:%S'),
                            'low': candle[1],
                            'high': candle[2],
                            'open': candle[3],
                            'close': candle[4],
                            'volume': candle[5]
                        })
                current_start = current_end
            except Exception as e:
                print(f"Error fetching historical data for {pair}: {e}")
                break

        return all_candles

    async def fetch_current_data(self, session, pair):
        """Fetch the latest 5-minute candle."""
        params = {'granularity': self.granularity}
        url = f"{self.base_url}/{pair}/candles"
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                candle = await response.json()
                if candle:
                    return {
                        'market': pair,
                        'candle_date_time_utc': datetime.utcfromtimestamp(candle[0][0]).strftime('%Y-%m-%d %H:%M:%S'),
                        'low': candle[0][1],
                        'high': candle[0][2],
                        'open': candle[0][3],
                        'close': candle[0][4],
                        'volume': candle[0][5]
                    }
        except Exception as e:
            print(f"Error fetching current data for {pair}: {e}")
            return None

    async def fetch_all_pairs(self):
        """Fetch data for all pairs based on run type."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            if self.is_first_run:
                print("\n=== Fetching Historical Data ===")
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=1)  # Данные за последний месяц
                for pair in self.pairs:
                    tasks.append(self.fetch_historical_data(session, pair, start_time, end_time))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, list):
                        self.all_candles_data.extend(result)
            else:
                print("\n=== Fetching Current Data ===")
                for pair in self.pairs:
                    tasks.append(self.fetch_current_data(session, pair))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if result:
                        self.all_candles_data.append(result)
                    
                    # Задержка для предотвращения превышения лимита запросов
                    await asyncio.sleep(1)  # Задержка в 1 секунду между запросами

    def save_to_csv(self):
        """Save the collected candles to a CSV file."""
        if not self.all_candles_data:
            print("No data to save.")
            return

        df = pd.DataFrame(self.all_candles_data)

        try:
            # Если файл существует, читаем его
            existing_df = pd.read_csv("coinbase_data.csv")
            # Преобразуем столбец времени в datetime
            existing_df['candle_date_time_utc'] = pd.to_datetime(existing_df['candle_date_time_utc'])
            df['candle_date_time_utc'] = pd.to_datetime(df['candle_date_time_utc'])
            # Конкатенируем старые и новые данные, удаляем дубликаты
            combined_df = pd.concat([existing_df, df])
            combined_df = combined_df.drop_duplicates(subset=['market', 'candle_date_time_utc'], keep='last')
            combined_df = combined_df.sort_values(['market', 'candle_date_time_utc'])
            # Сохраняем данные
            combined_df.to_csv("coinbase_data.csv", index=False)
            print(f"Saved {len(df)} new records to coinbase_data.csv.")
        except FileNotFoundError:
            # Если файл не существует, создаем новый
            df.to_csv("coinbase_data.csv", index=False)
            print(f"Created coinbase_data.csv with {len(df)} records.")

        df = pd.DataFrame(self.all_candles_data)
        df.to_csv("coinbase_data.csv", mode='a', index=False, header=False)
        print(f"Saved {len(df)} records to coinbase_data.csv")

    async def run(self):
        """Main execution method."""
        await self.fetch_all_pairs()
        self.save_to_csv()
        self.is_first_run = False  # Снять флаг после первого выполнения

if __name__ == "__main__":
    # Укажите пары, которые хотите запросить
    pairs = ["BTC-USD", "ETH-USD", "XRP-USD"]
    fetcher = CoinbaseDataFetcher(pairs=pairs, granularity=300)  # 5 минут
    while True:
        asyncio.run(fetcher.run())
        asyncio.sleep(300)  # Обновление каждые 5 минут