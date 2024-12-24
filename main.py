import asyncio
from get_data_coinbase import CoinbaseDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner
from datetime import datetime, timezone, timedelta
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def fetch_data_in_stages():
    """
    Основная функция для поэтапного сбора и обработки данных.
    """
    try:
        current_time = datetime.now(timezone.utc)
        # Получаем дату ровно год назад
        start_time = current_time - timedelta(days=365)
        # Устанавливаем время на начало дня
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # Инициализация списков пар для обеих бирж
        binance_pairs = ['BLZ/USDT', 'STG/USDT', 'IO/USDT', 'UMA/USDT', 'SAND/USDT', 'MANA/USDT', 'OP/USDT', 'PYR/USDT', 'CHZ/USDT', 'ZK/USDT', 'ZRO/USDT', 'BNT/USDT', 'ORCA/USDT', 'POWR/USDT', 'PERP/USDT', 'FIL/USDT', 'SEI/USDT', 'RONIN/USDT', 'VOXEL/USDT', 'PUNDIX/USDT', 'ADA/USDT', 'SUPER/USDT', 'SUI/USDT', 'ANKR/USDT', 'GHST/USDT', 'EIGEN/USDT', 'SOL/USDT', 'ALGO/USDT', 'AUDIO/USDT', 'BICO/USDT', 'JASMY/USDT', 'MINA/USDT', 'MLN/USDT', 'INJ/USDT', 'IOTX/USDT', 'AAVE/USDT', 'TRB/USDT', 'AMP/USDT', 'STX/USDT', 'STRK/USDT', 'DAR/USDT', 'COMP/USDT', 'METIS/USDT', 'MKR/USDT', 'ARKM/USDT', 'BONK/USDT', 'DASH/USDT', 'XLM/USDT', 'EOS/USDT', 'XTZ/USDT', 'KAVA/USDT', 'AUCTION/USDT', 'BLUR/USDT', 'LRC/USDT', 'RENDER/USDT', 'UNI/USDT', 'XRP/USDT', 'HIGH/USDT', 'LTC/USDT', 'WIF/USDT', 'ALICE/USDT', 'MDT/USDT', 'RLC/USDT', 'ETH/USDT', 'AXS/USDT', 'ICP/USDT', 'OMNI/USDT', 'TRU/USDT', 'HFT/USDT', 'REQ/USDT', 'KNC/USDT', 'FLOW/USDT', 'LPT/USDT', 'GNO/USDT', 'PEPE/USDT', 'ARB/USDT', 'POND/USDT', 'KSM/USDT', 'AGLD/USDT', 'QNT/USDT', 'FORTH/USDT', 'BAL/USDT', 'GLM/USDT', 'YFI/USDT', 'ZEN/USDT', 'MASK/USDT', 'ZRX/USDT', 'COTI/USDT', 'CLV/USDT', 'C98/USDT', 'SKL/USDT', 'LINK/USDT', 'LOKA/USDT', 'ZEC/USDT', 'ALCX/USDT', 'FARM/USDT', 'RPL/USDT', 'SPELL/USDT', 'DOGE/USDT', 'FLOKI/USDT', 'ERN/USDT', 'VET/USDT', 'FIDA/USDT', 'HBAR/USDT', 'ATOM/USDT', 'ROSE/USDT', 'ACX/USDT', 'SYN/USDT', 'TIA/USDT', 'DIA/USDT', 'FET/USDT', 'RARE/USDT', 'CVX/USDT', 'SHIB/USDT', 'NEAR/USDT', 'IMX/USDT', 'CELR/USDT', 'BAT/USDT', 'MAGIC/USDT', 'API3/USDT', 'T/USDT', 'CTSI/USDT', 'BAND/USDT', 'ENS/USDT', 'GMT/USDT', 'QI/USDT', 'LIT/USDT', 'RAD/USDT', 'G/USDT', 'NMR/USDT', 'IDEX/USDT', 'WBTC/USDT', 'COW/USDT', 'GTC/USDT', 'AERGO/USDT', 'ILV/USDT', 'AST/USDT', 'ARPA/USDT', 'DOT/USDT', 'CRV/USDT', 'ETC/USDT', 'JTO/USDT', 'APT/USDT', 'SNX/USDT', 'EGLD/USDT', 'BADGER/USDT', 'STORJ/USDT', 'CVC/USDT', 'OGN/USDT', 'VTHO/USDT', 'APE/USDT', 'AVAX/USDT', 'OSMO/USDT', 'GRT/USDT', 'ACH/USDT', 'BTC/USDT', 'TNSR/USDT', '1INCH/USDT', 'LQTY/USDT', 'AXL/USDT', 'OXT/USDT', 'FIS/USDT', 'LDO/USDT', 'POL/USDT', 'BCH/USDT', 'SUSHI/USDT', 'NKN/USDT']
        coinbase_pairs = [pair.replace('/USDT', '-USD') for pair in binance_pairs]

        # Инициализация фетчеров
        coinbase_fetcher = CoinbaseDataFetcher(target_pairs=coinbase_pairs)
        binance_fetcher = BinanceDataFetcher(target_pairs=binance_pairs)
        combiner = DataCombiner()

        # Этап 1: Получение исторических данных
        logging.info(f"Stage 1 - Fetching historical data from {start_time} to {current_time}")

        try:
            # Параллельный сбор данных с обеих бирж
            await asyncio.gather(
                coinbase_fetcher.fetch_all_pairs(start_time=start_time, end_time=current_time),
                binance_fetcher.fetch_all_pairs(start_time=start_time, end_time=current_time)
            )

            # Сохранение данных
            coinbase_fetcher.save_to_csv()
            binance_fetcher.save_to_csv()
            
            # Комбинирование данных и расчет индикаторов
            combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
            if not combined_data.empty:
                processed_data = combiner._calculate_indicators(combined_data)
                combiner.processed_data = processed_data
                combiner.save_combined_data("combined_data.csv")
                await combiner.send_to_web_service()

        except Exception as e:
            logging.error(f"Error in Stage 1: {str(e)}")
            raise

        # Этап 2: Непрерывный сбор текущих данных
        while True:
            try:
                logging.info("Stage 2 - Fetching current data")
                
                try:
                    df = pd.read_csv("combined_data.csv")
                    current_time = datetime.now(timezone.utc)
                    start_time = current_time - timedelta(hours=20)

                    # В Stage 2
                    if len(df) >= 167:
                        last_106 = df.tail(167)
                        update_start_time = pd.to_datetime(last_106['DateTime'].min()).tz_localize('UTC')
                        update_end_time = datetime.now(timezone.utc)
                        logging.info(f"Fetching data from {update_start_time} to {update_end_time}")
                    else:
                        update_end_time = datetime.now(timezone.utc)
                        update_start_time = update_end_time - timedelta(minutes=2)
                        
                    # Сбор новых данных
                    await asyncio.gather(
                        coinbase_fetcher.fetch_all_pairs(start_time=update_start_time, end_time=update_end_time),
                        binance_fetcher.fetch_all_pairs(start_time=update_start_time, end_time=update_end_time)
                    )

                    # Сохранение новых данных
                    coinbase_fetcher.save_to_csv()
                    binance_fetcher.save_to_csv()

                    # Обработка и комбинирование данных
                    combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
                    if not combined_data.empty:
                        processed_data = combiner._calculate_indicators(combined_data)
                        combiner.processed_data = processed_data
                        combiner.save_combined_data("combined_data.csv")
                        await combiner.send_to_web_service()
                
                except Exception as e:
                    logging.error(f"Error in data processing: {e}")
                    await asyncio.sleep(60)
                    continue

                await asyncio.sleep(45)
                
            except Exception as e:
                logging.error(f"Error in Stage 2: {str(e)}")
                await asyncio.sleep(60)
                continue

    except Exception as e:
        logging.error(f"Critical error in fetch_data_in_stages: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(fetch_data_in_stages())
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")