import sqlite3
import matplotlib.pyplot as plt

# Connect to the database
conn = sqlite3.connect('combined_data.db')
cursor = conn.cursor()

# Query the city names and populations
cursor.execute("SELECT city, population FROM census") 
rows = cursor.fetchall()

# Close the connection
conn.close()

# Split data into separate lists
cities = [row[0] for row in rows]
populations = [row[1] for row in rows]

# Plot the bar graph
plt.figure(figsize=(14, 7))
plt.bar(cities, populations, color='mediumseagreen')
plt.xticks(rotation=45, ha='right')
plt.xlabel('City')
plt.ylabel('Population')
plt.title('Population per City')
plt.tight_layout()
plt.show()