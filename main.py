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
    try:
        # Инициализация компонентов
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(days=1)
        
        coinbase_fetcher = CoinbaseDataFetcher()
        binance_fetcher = BinanceDataFetcher()
        combiner = DataCombiner()

        # Список торговых пар для Binance
        pairs = ['SNT/USDT', 'QTUM/USDT', 'BTC/USDT', 'ETC/USDT', 'NEO/USDT', 'MTL/USDT', 'ETH/USDT', 'STEEM/USDT', 'XRP/USDT', 'XLM/USDT', 'ARK/USDT', 'ADA/USDT', 'STORJ/USDT', 'LSK/USDT', 'SC/USDT', 'TRX/USDT', 'EOS/USDT', 'ICX/USDT', 'POWR/USDT', 'POLYX/USDT', 'ONT/USDT', 'BAT/USDT', 'ZIL/USDT', 'ZRX/USDT', 'BCH/USDT', 'CVC/USDT', 'IOTA/USDT', 'IOST/USDT', 'KNC/USDT', 'ONG/USDT', 'GAS/USDT', 'HIFI/USDT', 'MANA/USDT', 'BSV/USDT', 'THETA/USDT', 'HBAR/USDT', 'ANKR/USDT', 'WAXP/USDT', 'ATOM/USDT', 'AERGO/USDT', 'STPT/USDT', 'CHZ/USDT', 'ORBS/USDT', 'VET/USDT', 'STMX/USDT', 'XTZ/USDT', 'LINK/USDT', 'KAVA/USDT', 'SXP/USDT', 'STRAX/USDT', 'DOT/USDT', 'TON/USDT', 'FLOW/USDT', 'SAND/USDT', 'DOGE/USDT', 'GLM/USDT', 'STX/USDT', 'POL/USDT', 'SOL/USDT', 'AXS/USDT', '1INCH/USDT', 'AVAX/USDT', 'NEAR/USDT', 'ALGO/USDT', 'AAVE/USDT', 'T/USDT', 'ARB/USDT', 'GMT/USDT', 'CELO/USDT', 'SUI/USDT', 'EGLD/USDT', 'APT/USDT', 'GRT/USDT', 'MASK/USDT', 'SEI/USDT', 'ID/USDT', 'IMX/USDT', 'BLUR/USDT', 'MINA/USDT', 'ZETA/USDT', 'AUCTION/USDT', 'AKT/USDT', 'ASTR/USDT', 'PYTH/USDT', 'ENS/USDT', 'JUP/USDT', 'ONDO/USDT', 'ZRO/USDT', 'STG/USDT', 'UXLINK/USDT', 'BIGTIME/USDT', 'PENDLE/USDT', 'USDC/USDT', 'G/USDT', 'UNI/USDT', 'W/USDT', 'INJ/USDT', 'MEW/USDT', 'CKB/USDT', 'DRIFT/USDT', 'AGLD/USDT', 'SAFE/USDT', 'XEM/USDT', 'WAVES/USDT', 'LOOM/USDT']

        # Этап 1: Получение исторических данных
        logging.info(f"Stage 1 - Fetching historical data from {start_time} to {current_time}")
        
        # Параллельный сбор данных
        tasks = [
            coinbase_fetcher.run(),  # теперь это корутина
            binance_fetcher.run(pairs)
        ]
        await asyncio.gather(*tasks)

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
                
                tasks = [
                    coinbase_fetcher.run(),
                    binance_fetcher.fetch_all_pairs(pairs)
                ]
                await asyncio.gather(*tasks)
                
                combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
                if not combined_data.empty:
                    processed_data = combiner._calculate_indicators(combined_data)
                    combiner.processed_data = processed_data
                    combiner.save_combined_data("combined_data.csv")
                    await combiner.send_to_web_service()
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logging.error(f"Error in Stage 2: {str(e)}")
                await asyncio.sleep(60)
                continue

    except Exception as e:
        logging.error(f"Error in fetch_data_in_stages: {str(e)}")
        return False

    return True

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(fetch_data_in_stages())
    except KeyboardInterrupt:
        logging.info("\nProgram terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        loop.close()