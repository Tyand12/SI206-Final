import sqlite3
import pandas as pd

db_path = 'combined_data.db' 
conn = sqlite3.connect(db_path)

traffic_df = pd.read_sql_query("SELECT * FROM traffic", conn)
census_df = pd.read_sql_query("SELECT * FROM census", conn)
weather_df = pd.read_sql_query("SELECT * FROM weather", conn)

traffic_df['city'] = traffic_df['city'].str.strip()
census_df['city'] = census_df['city'].str.strip()
weather_df['city'] = weather_df['city'].str.strip()

traffic_df['road_segment'] = 1
road_miles_df = traffic_df.groupby('city')['road_segment'].count().reset_index(name='total_road_miles')

avg_speed_df = traffic_df.groupby('city').agg({
    'currentSpeed': 'mean',
    'freeFlowSpeed': 'mean'
}).reset_index()

merged_df = avg_speed_df.merge(road_miles_df, on='city')
merged_df = merged_df.merge(census_df[['city', 'population']], on='city')
merged_df = merged_df.merge(weather_df[['city', 'temperature']], on='city')

merged_df['expr1'] = ((merged_df['freeFlowSpeed'] - merged_df['currentSpeed']) * merged_df['total_road_miles']) / merged_df['population']
merged_df['expr2'] = (merged_df['freeFlowSpeed'] - merged_df['currentSpeed']) / merged_df['temperature']

output_df = merged_df[['city', 'expr1', 'expr2']]
output_df.to_csv('city_metrics.txt', index=False, sep='\t')

conn.close()
