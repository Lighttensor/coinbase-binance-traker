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
        # Инициализация списков пар для обеих бирж
        binance_pairs = ['BLZ/USDT', 'STG/USDT', 'IO/USDT', 'UMA/USDT', 'SAND/USDT', 'MANA/USDT', 'OP/USDT', 'PYR/USDT', 'CHZ/USDT', 'ZK/USDT', 'ZRO/USDT', 'BNT/USDT', 'ORCA/USDT', 'POWR/USDT', 'PERP/USDT', 'FIL/USDT', 'SEI/USDT', 'RONIN/USDT', 'VOXEL/USDT', 'PUNDIX/USDT', 'ADA/USDT', 'SUPER/USDT', 'SUI/USDT', 'ANKR/USDT', 'GHST/USDT', 'EIGEN/USDT', 'SOL/USDT', 'ALGO/USDT', 'AUDIO/USDT', 'BICO/USDT', 'JASMY/USDT', 'MINA/USDT', 'MLN/USDT', 'INJ/USDT', 'IOTX/USDT', 'AAVE/USDT', 'TRB/USDT', 'AMP/USDT', 'STX/USDT', 'STRK/USDT', 'DAR/USDT', 'COMP/USDT', 'METIS/USDT', 'MKR/USDT', 'ARKM/USDT', 'BONK/USDT', 'DASH/USDT', 'XLM/USDT', 'EOS/USDT', 'XTZ/USDT', 'KAVA/USDT', 'AUCTION/USDT', 'BLUR/USDT', 'LRC/USDT', 'RENDER/USDT', 'UNI/USDT', 'XRP/USDT', 'HIGH/USDT', 'LTC/USDT', 'WIF/USDT', 'ALICE/USDT', 'MDT/USDT', 'RLC/USDT', 'ETH/USDT', 'AXS/USDT', 'ICP/USDT', 'OMNI/USDT', 'TRU/USDT', 'HFT/USDT', 'REQ/USDT', 'KNC/USDT', 'FLOW/USDT', 'LPT/USDT', 'GNO/USDT', 'PEPE/USDT', 'ARB/USDT', 'POND/USDT', 'KSM/USDT', 'AGLD/USDT', 'QNT/USDT', 'FORTH/USDT', 'BAL/USDT', 'GLM/USDT', 'YFI/USDT', 'ZEN/USDT', 'MASK/USDT', 'ZRX/USDT', 'COTI/USDT', 'CLV/USDT', 'C98/USDT', 'SKL/USDT', 'LINK/USDT', 'LOKA/USDT', 'ZEC/USDT', 'ALCX/USDT', 'FARM/USDT', 'RPL/USDT', 'SPELL/USDT', 'DOGE/USDT', 'FLOKI/USDT', 'ERN/USDT', 'VET/USDT', 'FIDA/USDT', 'HBAR/USDT', 'ATOM/USDT', 'ROSE/USDT', 'ACX/USDT', 'SYN/USDT', 'TIA/USDT', 'DIA/USDT', 'FET/USDT', 'RARE/USDT', 'CVX/USDT', 'SHIB/USDT', 'NEAR/USDT', 'IMX/USDT', 'CELR/USDT', 'BAT/USDT', 'MAGIC/USDT', 'API3/USDT', 'T/USDT', 'CTSI/USDT', 'BAND/USDT', 'ENS/USDT', 'GMT/USDT', 'QI/USDT', 'LIT/USDT', 'RAD/USDT', 'G/USDT', 'NMR/USDT', 'IDEX/USDT', 'WBTC/USDT', 'COW/USDT', 'GTC/USDT', 'AERGO/USDT', 'ILV/USDT', 'AST/USDT', 'ARPA/USDT', 'DOT/USDT', 'CRV/USDT', 'ETC/USDT', 'JTO/USDT', 'APT/USDT', 'SNX/USDT', 'EGLD/USDT', 'BADGER/USDT', 'STORJ/USDT', 'CVC/USDT', 'OGN/USDT', 'VTHO/USDT', 'APE/USDT', 'AVAX/USDT', 'OSMO/USDT', 'GRT/USDT', 'ACH/USDT', 'BTC/USDT', 'TNSR/USDT', '1INCH/USDT', 'LQTY/USDT', 'AXL/USDT', 'OXT/USDT', 'FIS/USDT', 'LDO/USDT', 'POL/USDT', 'BCH/USDT', 'SUSHI/USDT', 'NKN/USDT']
        # Преобразуем пары для Coinbase (замена / на -)
        coinbase_pairs = ['BLZ-USD', 'STG-USD', 'IO-USD', 'UMA-USD', 'SAND-USD', 'MANA-USD', 'OP-USD', 'PYR-USD', 'CHZ-USD', 'ZK-USD', 'ZRO-USD', 'BNT-USD', 'ORCA-USD', 'POWR-USD', 'PERP-USD', 'FIL-USD', 'SEI-USD', 'RONIN-USD', 'VOXEL-USD', 'PUNDIX-USD', 'ADA-USD', 'SUPER-USD', 'SUI-USD', 'ANKR-USD', 'GHST-USD', 'EIGEN-USD', 'SOL-USD', 'ALGO-USD', 'AUDIO-USD', 'BICO-USD', 'JASMY-USD', 'MINA-USD', 'MLN-USD', 'INJ-USD', 'IOTX-USD', 'AAVE-USD', 'TRB-USD', 'AMP-USD', 'STX-USD', 'STRK-USD', 'DAR-USD', 'COMP-USD', 'METIS-USD', 'MKR-USD', 'ARKM-USD', 'BONK-USD', 'DASH-USD', 'XLM-USD', 'EOS-USD', 'XTZ-USD', 'KAVA-USD', 'AUCTION-USD', 'BLUR-USD', 'LRC-USD', 'RENDER-USD', 'UNI-USD', 'XRP-USD', 'HIGH-USD', 'LTC-USD', 'WIF-USD', 'ALICE-USD', 'MDT-USD', 'RLC-USD', 'ETH-USD', 'AXS-USD', 'ICP-USD', 'OMNI-USD', 'TRU-USD', 'HFT-USD', 'REQ-USD', 'KNC-USD', 'FLOW-USD', 'LPT-USD', 'GNO-USD', 'PEPE-USD', 'ARB-USD', 'POND-USD', 'KSM-USD', 'AGLD-USD', 'QNT-USD', 'FORTH-USD', 'BAL-USD', 'GLM-USD', 'YFI-USD', 'ZEN-USD', 'MASK-USD', 'ZRX-USD', 'COTI-USD', 'CLV-USD', 'C98-USD', 'SKL-USD', 'LINK-USD', 'LOKA-USD', 'ZEC-USD', 'ALCX-USD', 'FARM-USD', 'RPL-USD', 'SPELL-USD', 'DOGE-USD', 'FLOKI-USD', 'ERN-USD', 'VET-USD', 'FIDA-USD', 'HBAR-USD', 'ATOM-USD', 'ROSE-USD', 'ACX-USD', 'SYN-USD', 'TIA-USD', 'DIA-USD', 'FET-USD', 'RARE-USD', 'CVX-USD', 'SHIB-USD', 'NEAR-USD', 'IMX-USD', 'CELR-USD', 'BAT-USD', 'MAGIC-USD', 'API3-USD', 'T-USD', 'CTSI-USD', 'BAND-USD', 'ENS-USD', 'GMT-USD', 'QI-USD', 'LIT-USD', 'RAD-USD', 'G-USD', 'NMR-USD', 'IDEX-USD', 'WBTC-USD', 'COW-USD', 'GTC-USD', 'AERGO-USD', 'ILV-USD', 'AST-USD', 'ARPA-USD', 'DOT-USD', 'CRV-USD', 'ETC-USD', 'JTO-USD', 'APT-USD', 'SNX-USD', 'EGLD-USD', 'BADGER-USD', 'STORJ-USD', 'CVC-USD', 'OGN-USD', 'VTHO-USD', 'APE-USD', 'AVAX-USD', 'OSMO-USD', 'GRT-USD', 'ACH-USD', 'BTC-USD', 'TNSR-USD', '1INCH-USD', 'LQTY-USD', 'AXL-USD', 'OXT-USD', 'FIS-USD', 'LDO-USD', 'POL-USD', 'BCH-USD', 'SUSHI-USD', 'NKN-USD']

        # Инициализация фетчеров с правильными списками пар
        coinbase_fetcher = CoinbaseDataFetcher(coinbase_pairs)  # Передаем список пар для Coinbase
        binance_fetcher = BinanceDataFetcher()  # Binance получает пары при вызове run
        combiner = DataCombiner()

        # Этап 1: Получение исторических данных
        logging.info(f"Stage 1 - Fetching historical data")
        
        # Параллельный сбор данных с обеих бирж
        await asyncio.gather(
            coinbase_fetcher.run(),  # Coinbase использует пары из конструктора
            binance_fetcher.run(binance_pairs)  # Binance получает пары при вызове
        )

        # Комбинирование и расчет индикаторов
        combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
        if not combined_data.empty:
            processed_data = combiner._calculate_indicators(combined_data)
            combiner.processed_data = processed_data
            combiner.save_combined_data("combined_data.csv")
            await combiner.send_to_web_service()

        # Этап 2: Периодическое обновление
        while True:
            try:
                logging.info("Stage 2 - Fetching current data")
                
                await asyncio.gather(
                    coinbase_fetcher.run(),
                    binance_fetcher.run(binance_pairs)
                )
                
                combined_data = combiner.combine_data('coinbase_data.csv', 'binance_data.csv')
                if not combined_data.empty:
                    processed_data = combiner._calculate_indicators(combined_data)
                    combiner.processed_data = processed_data
                    combiner.save_combined_data("combined_data.csv")
                    await combiner.send_to_web_service()
                
                await asyncio.sleep(45)  # Обновление каждые 5 минут
                
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
        asyncio.run(fetch_data_in_stages())
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
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