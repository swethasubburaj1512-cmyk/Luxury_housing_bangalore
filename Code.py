import pandas as pd
import numpy as np
import _mysql_connector
# -----------------------------
# 1. Load Data
# -----------------------------
df = pd.read_csv("C:/Users/swethasubburaj/OneDrive/Desktop/Git/Luxury_Housing_Bangalore.csv")

# -----------------------------
# 2. Data Cleaning
# -----------------------------
# Strip spaces from column names
df.columns = df.columns.str.strip()

# Drop duplicate rows
df = df.drop_duplicates()

# Handle missing values (replace with NaN)
df = df.replace("Nan", np.nan)
df = df.replace("Not Available", np.nan)

df.info()
df.shape()


# -----------------------------
# 3. Rename Columns
# -----------------------------
rename_map = {
    "Developer_Name": "Builder",
    "Purchase_Quarter": "Date"
}
df = df.rename(columns=rename_map)
# Convert all values in Micro_Market column to lowercase
df["Micro_Market"] = df["Micro_Market"].astype(str).str.lower()

# -----------------------------
# 4. Data Transformation 
# -----------------------------
# Convert Ticket_Price_Cr to numeric
if 'Ticket_Price_Cr' in df.columns:
    df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].astype(str).str.replace(' Cr','', regex=False).str.replace('₹','',regex=False)
    df['Ticket_Price_Cr'] = pd.to_numeric(df['Ticket_Price_Cr'], errors='coerce')
    # Removed the formatting step that converted it back to string
    # df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].apply(lambda x: f"₹{round(x,2)} Cr" if pd.notnull(x) else "Nan")

# Convert Unit_Size_Sqft to numeric
if 'Unit_Size_Sqft' in df.columns:
    df['Unit_Size_Sqft'] = pd.to_numeric(df['Unit_Size_Sqft'], errors='coerce')

# Standardize all variations of BHK to bhk
if 'Configuration' in df.columns:
    df["Configuration"] = df["Configuration"].astype(str).str.replace("+", "", regex=False).str.strip().str.lower()
    df["Configuration"] = df["Configuration"].str.replace("bhk", "bhk", regex=False) # Ensure consistency
# Convert to float and round to 2 decimals
df["Connectivity_Score"] = df["Connectivity_Score"].astype(float).round(2)

# Format with 2 decimal places but keep NaN as blank
df["Connectivity_Score"] = df["Connectivity_Score"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "")    
# Convert to numeric safely (any bad values -> NaN)
df["Amenity_Score"] = pd.to_numeric(df["Amenity_Score"], errors="coerce")

# Round to 2 decimals
df["Amenity_Score"] = df["Amenity_Score"].round(2)
df['Price_per_sqft']= (df['Ticket_Price_Cr']*1e7)/df['Unit_Size_Sqft']
# Convert to numeric safely (any bad values -> NaN)
df['Price_per_sqft'] = pd.to_numeric(df['Price_per_sqft'], errors="coerce")

# Round to 2 decimals
df['Price_per_sqft'] = df['Price_per_sqft'].round(2)
# Convert to numeric safely (any bad values -> NaN)
df['Price_per_sqft'] = pd.to_numeric(df['Price_per_sqft'], errors="coerce")

# Round to 2 decimals
df['Price_per_sqft'] = df['Price_per_sqft'].round(2)
df['Price_per_sqft']
# 1. Booking_Flag: 1 if booked, else 0
df["Booking_Flag"] = df["Possession_Status"].apply(
    lambda x: 1 if str(x).lower() in ["ready to move"] else 0
)

# 2. Booking_Status derived from Possession_Status
df["Booking_Status"] = df["Possession_Status"].apply(
    lambda x: "Yes" if str(x).lower() == "ready to move" else "No"
)

# 3. Booking_Count per project
df["Booking_Count"] = df.groupby("Project_Name")["Booking_Flag"].transform("sum")

# 4. Booking_Conversion_Rate per project
df["Booking_Conversion_Rate"] = (
    df["Booking_Count"] / df.groupby("Project_Name")["Booking_Flag"].transform("count")
).round(2)
# Convert Purchase_Quarter to datetime
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# Extract quarter as integer (no decimals)
df["Quarter"] = df["Date"].dt.quarter.astype("Int64")
print(df.columns)


####### SQL connection ########
"""from sqlalchemy import create_engine
host="localhost"
user="root"
port="3306"
database="Luxury_Housing_Bangalore"
password="Swethasubburaj12345"

engine = create_engine(f"mysql+mysqlconnector://root:Swethasubburaj12345@localhost:3306/Luxury_Housing_Bangalore")

from sqlalchemy import create_engine, text

# Example connection (replace with your DB details)
engine = create_engine("mysql+mysqlconnector://root:Swethasubburaj12345@localhost:3306/Luxury_Housing_Bangalore")

df.to_sql("Luxury_Housing_Bangalore", con=engine, if_exists="replace", index=False)"""


