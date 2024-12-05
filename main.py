import asyncio
from get_data_coinbase import CoinbaseDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner
from datetime import datetime, timezone, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def fetch_data_in_stages():
    """
    Основная функция для поэтапного сбора и обработки данных
    """
    try:
        # Инициализация компонентов
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(days=1)
        
        # Инициализируем Coinbase с базовыми парами
        coinbase_fetcher = CoinbaseDataFetcher()
        binance_fetcher = BinanceDataFetcher()
        combiner = DataCombiner()

        # Список торговых пар для Binance
        pairs = [
                'BTC/USDT', 'NEO/USDT', 'ETC/USDT', 'QTUM/USDT', 'SNT/USDT', 'ETH/USDT',
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
                'BEAM/USDT', 'BLAST/USDT', 'TAIKO/USDT', 'ATH/USDT', 'CARV/USDT'
            ]

        # Этап 1: Получение исторических данных
        logging.info(f"Stage 1 - Fetching historical data from {start_time} to {current_time}")
        
        # Параллельный сбор данных
        tasks = [
            coinbase_fetcher.run(),  # Используем метод run() вместо fetch_all_candles
            binance_fetcher.fetch_all_pairs(pairs)
        ]
        await asyncio.gather(*tasks)

        # Сохранение и обработка данных происходит внутри run() для Coinbase
        binance_fetcher.save_to_csv()
        
        # Комбинирование и расчет индикаторов
        combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
        if not combined_data.empty:
            processed_data = combiner._calculate_indicators(combined_data)
            combiner.processed_data = processed_data
            combiner.save_combined_data("combined_data.csv")
            await combiner.send_to_web_service()

        # Этап 2: Периодическое обновление данных
        while True:
            try:
                current_time = datetime.now(timezone.utc)
                logging.info(f"Stage 2 - Fetching current data at: {current_time}")
                
                # Параллельный сбор текущих данных
                tasks = [
                    coinbase_fetcher.run(),  # Используем run() для обновления данных
                    binance_fetcher.fetch_all_pairs(pairs)
                ]
                await asyncio.gather(*tasks)

                # Сохранение происходит внутри run() для Coinbase
                binance_fetcher.save_to_csv()
                
                # Обновление комбинированных данных
                combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
                if not combined_data.empty:
                    processed_data = combiner._calculate_indicators(combined_data)
                    combiner.processed_data = processed_data
                    combiner.save_combined_data("combined_data.csv")
                    await combiner.send_to_web_service()
                
                await asyncio.sleep(300)  # Обновляем каждые 5 минут
                
            except Exception as e:
                logging.error(f"Error in Stage 2: {str(e)}")
                await asyncio.sleep(60)  # Пауза перед повторной попыткой
                continue

    except Exception as e:
        logging.error(f"Error in fetch_data_in_stages: {str(e)}")
        return False

    return True

async def main():
    """
    Точка входа в программу
    """
    try:
        success = await fetch_data_in_stages()
        if success:
            logging.info("Data collection completed successfully")
        else:
            logging.error("Data collection failed")
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nProgram terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")