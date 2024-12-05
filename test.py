import asyncio
import logging
from get_data_coinbase import CoinbaseDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

async def test():
    # Инициализация объектов для сбора данных
    logger.info("Initializing data combiner...")
    combiner = DataCombiner()

    # Комбинирование данных
    logger.info("Combining data from Upbit and Binance...")
    combined_data = combiner.combine_data(
        'coinbase_data.csv', 
        'binance_data.csv'
    )
    
    if not combined_data.empty:
        logger.info(f"Combined data size: {combined_data.shape[0]} rows")
        
        # Расчет индикаторов
        logger.info("Calculating indicators...")
        combined_with_indicators = combiner._calculate_indicators(combined_data)
        
        # Сохранение данных с индикаторами
        combiner.processed_data = combined_with_indicators
        logger.info("Saving combined data with indicators...")
        combiner.save_combined_data("combined_data_with_indicators.csv")

        # Отправка данных на веб-сервис
        logger.info("Sending combined data to web service...")
    #   logger.info(f"Data to send (first 5 rows): {combined_with_indicators.head().to_dict(orient='records')}")
        await combiner.send_to_web_service()
    else:
        logger.warning("No data to process.")

if __name__ == "__main__":
    asyncio.run(test())