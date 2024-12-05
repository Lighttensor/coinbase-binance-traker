from flask import Flask, render_template, request, jsonify
import pandas as pd
import threading
import time
from datetime import datetime
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

latest_data = None

@app.route('/update_data', methods=['POST'])
def update_data_endpoint():
    try:
        logger.info("Received POST request on /update_data")
        
        # Проверяем тип данных
        if isinstance(request.json, list):
            data = request.json
        elif isinstance(request.json, dict) and 'data' in request.json:
            data = request.json['data']
        else:
            raise ValueError("Invalid JSON format: expected a list or dictionary with 'data' key.")
        
        if data:
            logger.info("Data received. Converting to DataFrame...")
            
            global latest_data
            latest_data = pd.DataFrame(data)
            
            logger.debug(f"Raw DataFrame:\n{latest_data}")
            
            # Обработка данных
            latest_data['DateTime'] = pd.to_datetime(latest_data['DateTime'])
            latest_data = latest_data.sort_values('DateTime', ascending=False)
            
            logger.debug(f"Processed DataFrame:\n{latest_data}")
            
            return jsonify({"status": "success"})
        
        logger.warning("No data received in POST request.")
        return jsonify({"status": "error", "message": "No data received"}), 400
    except Exception as e:
        logger.error(f"Exception in /update_data: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def index():
    logger.info("GET request on /")
    
    if latest_data is None:
        logger.info("No data available yet.")
        return "Loading data... Please refresh in a few moments."
    
    try:
        logger.info("Preparing data for rendering...")
        display_data = latest_data.copy()
        display_data['DateTime'] = display_data['DateTime'].dt.strftime('%Y-%m-%d %H:%M')
        
        # Округляем числовые колонки для лучшего отображения
        numeric_columns = [
            'Price @ Upbit', 'Price @ Binance', 
        ]
        
        for col in numeric_columns:
            if col in display_data.columns:
                display_data[col] = display_data[col].round(7)
        
        data = display_data.to_dict('records')
        logger.debug(f"Data passed to template:\n{data}")
        
        return render_template('data.html', 
                               data=data, 
                               last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}", exc_info=True)
        return f"Error processing data: {str(e)}"

if __name__ == "__main__":
    app.run(host='localhost', port=5000, debug=True)