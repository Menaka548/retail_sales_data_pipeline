# Retail Sales Data Pipeline

## Project Overview
This project uses PySpark and Spark SQL to process and analyse real-world retail sales data.

## Dataset
- Dataset file required: `online_retail_II.csv`
- Download the dataset from: https://api.datapoint.market/r/data37/online-retail
- Place the downloaded CSV file in the same project folder as `retail_pipeline.py` before running the pipeline.
- Total records: 1,067,371
- Source file: `online_retail_II.csv`

## Technologies Used
- Python
- PySpark
- Spark SQL
- Visual Studio Code

## Data Processing
- Read retail data from a CSV file.
- Calculate total sales using Quantity × Price.
- Check missing Customer IDs and Descriptions.
- Analyse revenue by country.
- Identify the top 10 customers by spending and count their orders.

## Data Quality Results
- Total rows: 1,067,371
- Missing Customer IDs: 243,007
- Missing Descriptions: 4,382
- Rows with valid Customer IDs: 824,364

## Output Files
- `output/retail_sales` — processed sales data
- `output/customer_sales` — customer spending analysis
- `output/country_sales` — country-wise revenue analysis

## Project Status
Initial pipeline and analysis completed successfully.


