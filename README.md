# nyc-taxi-pipeline
Data pipeline for NYC taxi data

##  Project Overview  
This project processes NYC Yellow Taxi trip data using **PySpark**. The workflow involves:  
- Extracting raw trip data (Parquet format)  
- Cleaning and transforming the data  
- Removing outliers using the **98th percentile method**  
- Storing the cleaned dataset back as **Parquet**  


---

##  Setup Instructions  

### **Install Dependencies**  
Ensure you have Python and PySpark installed. You can set up a virtual environment and install dependencies using:  

```bash
# Create virtual environment (optional)
python -m venv venv  
source venv/bin/activate

# Install required libraries
pip install -r requirements.txt

python scripts/spark_preprocessing.py
```
---
## Data Processing Steps
Step 1: Load Raw Data
- Reads NYC taxi trip data from Parquet format
- Loads it into a PySpark DataFrame

Step 2: Data Cleaning & Transformation
- Converts timestamps to Unix format
- Computes trip duration in minutes
- Handles missing or invalid data

Step 3: Outlier Removal
- Computes 98th percentile for trip distance, duration, and fare amount
- Removes values exceeding this threshold

Step 4: Save Cleaned Data
- Saves the processed DataFrame back into Parquet format
---
### Next Steps
Add real-time data ingestion using Kafka
