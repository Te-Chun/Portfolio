# Weather & AQI Data Collector for Texas Cities

## Project Objective
This project uses **Airflow** to automatically fetch real-time weather and air quality index (AQI) data for 10 major cities in Texas and stores the data in a local **SQLite** database. Users can query the database with SQL to compare weather conditions across cities, while the accumulated historical data allows for trend analysis and long-term studies.

## Key Features
- Automated scheduling with **Airflow** to fetch daily weather and AQI data for 10 Texas cities
- Stores the collected data in a **SQLite** database
- Supports SQL queries for quick comparison of weather and AQI across cities
- Accumulates historical data for trend analysis and data exploration
- Uses Airflow **XCom** to share data between tasks

## Technologies Used
- **Python**: Data fetching and processing  
- **Airflow**: Workflow management and scheduling  
- **SQLite**: Lightweight database storage  
- **OpenWeatherMap API**: Source of weather and air quality data  

## Data Source
This project uses the [OpenWeatherMap API](https://openweathermap.org/api) to fetch real-time weather and air quality data for Texas cities.S