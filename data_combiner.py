import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import aiohttp
import logging
from typing import Dict, List, Optional, Union
import sys
from rich import print

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_combiner.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataValidationError(Exception):
    """Пользовательское исключение для ошибок валидации данных"""
    pass

class DataCombiner:
    """Класс для объединения и обработки рыночных данных от coinbase и Binance"""
    
    def __init__(self, server_url: str = 'http://localhost:5000'):
        self.server_url = server_url
        self.raw_data = pd.DataFrame()
        self.processed_data = pd.DataFrame()
        
        # Требуемые столбцы для проверки
        self.required_columns = {
            'coinbase': {
                'market': str,
                'candle_date_time_utc': str,
            #    'opening_price': float,
            #    'high_price': float,
            #    'low_price': float,
                'close_price': float,
                'volume': float
            },
            'binance': {
                'market': str,
                'candle_date_time_utc': str,
            #    'opening_price': float,
            #    'high_price': float,
            #    'low_price': float,
                'close_price': float,
                'quote_volume': float
            }
        }

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация названий колонок"""
        df.columns = df.columns.str.strip().str.lower()
        return df

    def _check_data_types(self, df: pd.DataFrame, source: str) -> None:
        """Проверка типов данных в колонках"""
        for col, expected_type in self.required_columns[source].items():
            if col in df.columns:
                if expected_type in (float, int):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif expected_type == str:
                    df[col] = df[col].astype(str)
            else:
                raise DataValidationError(f"Missing column {col} in {source} data")

    def validate_input_data(self, df: pd.DataFrame, source: str) -> None:
        """Расширенная валидация входных данных"""
        logger.info(f"Validating {source} data")
        if df.empty:
            raise DataValidationError(f"{source} data is empty")
        self._normalize_column_names(df)
        self._check_data_types(df, source)

    def _process_datetime(self, df: pd.DataFrame, datetime_column: str) -> pd.DataFrame:
        """Преобразование временных меток"""
        if datetime_column in df.columns:
            df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
            invalid_dates = df[df[datetime_column].isnull()].index
            if len(invalid_dates) > 0:
                logger.warning(f"Found invalid dates in {datetime_column}")
        return df

    def combine_data(self, coinbase_file: str, binance_file: str) -> pd.DataFrame:
        """Combines data from coinbase and Binance"""
        try:
            # Read data files
            coinbase_df = pd.read_csv(coinbase_file)
            coinbase_df['market'] = coinbase_df['market'].str.replace('-', '') + 'T'
            binance_df = pd.read_csv(binance_file)
            
            # Rename columns to avoid conflicts and create required columns
            coinbase_df = coinbase_df.rename(columns={
                'close_price': 'close_price_coinbase',
                'volume': 'volume_coinbase'
            })
            coinbase_df = coinbase_df.drop(columns=['opening_price', 'high_price', 'low_price', 'market_type'])
            
            binance_df = binance_df.rename(columns={
                'close_price': 'close_price_binance',
                'quote_volume': 'volume_binance'
            })
            binance_df = binance_df.drop(columns=['opening_price', 'volume', 'high_price', 'low_price', 'market_type'])
            binance_df['market'] = binance_df['market'].replace('/', '', regex=True)
            
            # Convert timestamps with UTC awareness
            coinbase_df['timestamp_coinbase'] = pd.to_datetime(
                coinbase_df['candle_date_time_utc']
            ).dt.tz_localize('UTC')
            
            binance_df['timestamp_binance'] = pd.to_datetime(
                binance_df['candle_date_time_utc']
            ).dt.tz_localize('UTC')
            
            # Merge data based on market and time proximity
            merged_data = pd.merge_asof(
                coinbase_df.sort_values('timestamp_coinbase'),
                binance_df.sort_values('timestamp_binance'),
                by='market',
                left_on='timestamp_coinbase',
                right_on='timestamp_binance',
                direction='nearest',
                tolerance=pd.Timedelta('5min')
            )
            
            # Verify required columns exist
            required_columns = [
                'close_price_coinbase', 
                'close_price_binance', 
                'volume_coinbase', 
                'volume_binance'
            ]
            
            missing_columns = [col for col in required_columns if col not in merged_data.columns]
            if missing_columns:
                logging.warning(f"Missing columns: {missing_columns}")
            
            # Sort and reset index
            self.processed_data = merged_data.sort_values(
                by=['market', 'timestamp_coinbase']
            ).reset_index(drop=True)
            
            return self.processed_data
                
        except Exception as e:
            logging.error(f"Error combining data: {e}")
            raise

    def _convert_volume(self, value: float) -> str:
        """
        Конвертирует числовые значения в формат с суффиксами К, М, В
        """
        if value >= 1_000_000_000:  # миллиарды
            return f"{value/1_000_000_000:.2f}B"
        elif value >= 1_000_000:     # миллионы
            return f"{value/1_000_000:.2f}M"
        elif value >= 1_000:         # тысячи
            return f"{value/1_000:.2f}K"
        else:
            return f"{value:.2f}"

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Вычисляет премии и объемы по каждому токену и сохраняет в итоговую таблицу"""
        
        # Проверка наличия нужных столбцов
        required_columns = [
            'market', 'timestamp_coinbase', 'timestamp_binance', 
            'close_price_coinbase', 'close_price_binance', 'volume_coinbase', 'volume_binance'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Warning: Missing columns: {missing_columns}")
            return pd.DataFrame()  # Возвращаем пустой DataFrame, если данных недостаточно
        
        def calculate_for_group(group):
            group = group.sort_values('timestamp_coinbase')  # Сортировка по времени
            group = group.set_index('timestamp_coinbase')  # Устанавливаем timestamp_coinbase как индекс
            
            # Премии
            group['Coinbase Premium'] = group['close_price_coinbase'] - group['close_price_binance']
            group['Coinbase Premium %'] = (group['Coinbase Premium'] / group['close_price_binance']) * 100
            
            # Скользящие метрики
            rolling_intervals = {
                '1H': '60min', 
                '24H': '1D', 
                '1M': '30D',
                '1Y': '365D'  # Добавляем годовой интервал
            }
            
            for label, period in rolling_intervals.items():
                # Используем shift(1) перед расчетом скользящих средних
                shifted_premium = group['Coinbase Premium'].shift(1)
                shifted_premium_pct = group['Coinbase Premium %'].shift(1)
                
                group[f'Avg {label} Premium Diff'] = (
                    shifted_premium
                    .rolling(window=period, min_periods=1)
                    .mean()
                )
                group[f'Avg {label} Premium %'] = (
                    shifted_premium_pct
                    .rolling(window=period, min_periods=1)
                    .mean()
                )
                group[f'Current Diff {label}'] = group['Coinbase Premium'] - group[f'Avg {label} Premium Diff']
                group[f'Current % {label}'] = group['Coinbase Premium %'] - group[f'Avg {label} Premium %']
            
            # Модифицируем расчет объемов
            shifted_volume = group['volume_coinbase'].shift(1)
            
            group['Avg 1H Volume'] = (
                shifted_volume
                .rolling(window='60min', min_periods=1)
                .mean()
            )
            
            group['Avg 24H Volume'] = (
                shifted_volume
                .rolling(window='1D', min_periods=1)
                .mean()
            )
            
            group['Avg 1M Volume'] = (
                shifted_volume
                .rolling(window='30D', min_periods=1)
                .mean()
            )
            
            group['Avg 1Y Volume'] = (
                shifted_volume
                .rolling(window='365D', min_periods=1)
                .mean()
            )
            
            group['Coinbase Volume Diff %'] = (
                (group['volume_coinbase'] - group['Avg 1H Volume']) / group['Avg 1H Volume'] * 100
            )
            
            # Перепишем таблицу с нужной структурой
            result = pd.DataFrame({
                'DateTime': group.index,
                'Coin': group['market'],
                'Price @ Coinbase': group['close_price_coinbase'],
                'Price @ Binance': group['close_price_binance'],
            #    'coinbase Premium': group['coinbase Premium'],
                'Coinbase Premium %': group['Coinbase Premium %'],
            #    'Avg 1H Premium Diff': group['Avg 1H Premium Diff'],
                'Avg 1H Premium %': group['Avg 1H Premium %'],
            #    'Current Diff 1H': group['Current Diff 1H'],
                'Current % 1H': group['Current % 1H'],
            #    'Avg 24H Premium Diff': group['Avg 24H Premium Diff'],
                'Avg 24H Premium %': group['Avg 24H Premium %'],
            #    'Current Diff 24H': group['Current Diff 24H'],
                'Current % 24H': group['Current % 24H'],
            #    'Avg 1M Premium Diff': group['Avg 1M Premium Diff'],
                'Avg 1M Premium %': group['Avg 1M Premium %'],
            #    'Current Diff 1M': group['Current Diff 1M'],
                'Current % 1M': group['Current % 1M'],
            #    'Avg 1Y Premium Diff': group['Avg 1Y Premium Diff'],  # Добавляем годовые метрики
                'Avg 1Y Premium %': group['Avg 1Y Premium %'],
             #   'Current Diff 1Y': group['Current Diff 1Y'],
                'Current % 1Y': group['Current % 1Y'],
                'Current Volume': group['volume_coinbase'],
                'Avg 1H Volume': group['Avg 1H Volume'],
                'Avg 24H Volume': group['Avg 24H Volume'],
                'Avg 1M Volume': group['Avg 1M Volume'],
                'Avg 1Y Volume': group['Avg 1Y Volume'],  # Добавляем годовой объем
                'Coinbase Volume Diff %': group['Coinbase Volume Diff %']
            })
            
            return result

        # Инициализация итогового DataFrame со всеми колонками, включая новые годовые
        result_df = pd.DataFrame(columns=[
            'DateTime', 'Coin', 'Price @ Coinbase', 'Price @ Binance',
            'Coinbase Premium %', 'Avg 1H Premium %',
            'Current % 1H', 'Avg 24H Premium %',
            'Current % 24H', 'Avg 1M Premium %',
            'Current % 1M','Avg 1Y Premium %',  
            'Current % 1Y', 'Current Volume', 
            'Avg 1H Volume','Avg 24H Volume', 
            'Avg 1M Volume', 'Avg 1Y Volume', 
            'Coinbase Volume Diff %'
        ])

        # Применяем функцию к каждой группе по токенам и добавляем к итоговому DataFrame
        grouped_df = df.groupby('market')
        
        for name, group in grouped_df:
            group_result = calculate_for_group(group)
            
            if group_result.empty:
                print(f"Warning: Group for {name} resulted in an empty DataFrame.")
            
            result_df = pd.concat([result_df, group_result], ignore_index=True)

        price_columns = [
            'Price @ Coinbase', 'Price @ Binance'
        ]

        percentage_columns = [
            'Coinbase Premium %', 'Avg 1H Premium %',
            'Current % 1H', 'Avg 24H Premium %',
            'Current % 24H', 'Avg 1M Premium %',
            'Current % 1M', 'Avg 1Y Premium %',
            'Current % 1Y', 'Coinbase Volume Diff %'
        ]

        volume_columns = [
            'Current Volume', 'Avg 1H Volume',
            'Avg 24H Volume', 'Avg 1M Volume',
            'Avg 1Y Volume'
        ]

        # Применяем форматирование к разным типам колонок
        for col in price_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].round(8)  # Цены округляем до 8 знаков

        for col in percentage_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].round(2)  # Проценты округляем до 2 знаков

        for col in volume_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(self._convert_volume)  # Убираем lambda и float()

        result_df = result_df.sort_values(['DateTime', 'Coin'])
        result_df['DateTime'] = result_df['DateTime'].apply(lambda x: str(x)[:16])
        return result_df

    def save_combined_data(self, file_path: str) -> None:
        """Сохраняет обработанные данные в файл"""
    #    logger.info(f"Saving combined data to {"combined_data_with_indicators.csv"}")
        self.processed_data.to_csv(file_path, index=False)

    def save_processed_indicators(self, file_path: str) -> None:
        """Сохраняет вычисленные индикаторы"""
      #  logger.info(f"Saving processed indicators to {"combined_data_with_indicators.csv"})
        processed_indicators = self._calculate_indicators(self.processed_data)
        processed_indicators.to_csv(file_path, index=False)

    async def send_to_web_service(self) -> None:
        """Send last record for each unique coin to web service"""
        if self.processed_data.empty:
            logger.error("No data to send to web service")
            return
        try:
            # Получаем последние данные для каждого уникального коина, удаляя дубликаты по времени
            data_to_send = (self.processed_data
                        .sort_values(['DateTime', 'Coin'])  # Сортируем по времени и монетам
                        .drop_duplicates(subset=['Coin', 'DateTime'], keep='last')  # Удаляем дубликаты, оставляя последнюю запись
                        .groupby('Coin')  # Группируем по монетам
                        .last()  # Берем последнюю запись для каждой группы
                        .reset_index()  # Сбрасываем индекс
                        .dropna()  # Удаляем строки, содержащие NaN
                        .to_dict(orient='records'))  # Конвертируем в список словарей
            
            # Выводим количество уникальных монет без NaN
            print(f"Sending data for {len(data_to_send)} unique coins (excluding rows with NaN values)")

            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.server_url}/update_data", json=data_to_send) as response:
                    if response.status == 200:
                        logger.info(f"Successfully sent data for {len(data_to_send)} coins")
                    else:
                        logger.error(f"Failed to send data to web service: {response.status}")
        except Exception as e:
            logger.error(f"Error sending data to web service: {str(e)}")

def main():
    combiner = DataCombiner()
    coinbase_file = "coinbase_data.csv"
    binance_file = "binance_data.csv"

    # Объединение данных
    combined_data = combiner.combine_data(coinbase_file, binance_file)
    
    if not combined_data.empty:
        # Расчет индикаторов
        combined_with_indicators = combiner._calculate_indicators(combined_data)
        
        # Сохранение данных с индикаторами
        combiner.processed_data = combined_with_indicators
        combiner.save_combined_data("combined_data_with_indicators.csv")
        
        print("Calculation complete. Data saved with indicators.")
    else:
        print("No data to process.")

if __name__ == "__main__":
    main()