import sqlite3
import matplotlib.pyplot as plt

# # Connect to the database
# conn = sqlite3.connect('combined_data.db')
# cursor = conn.cursor()

# # Query the city names and populations
# cursor.execute("SELECT city, population FROM city_stats") 
# rows = cursor.fetchall()

# # Close the connection
# conn.close()

# # Split data into separate lists
# cities = [row[0] for row in rows]
# populations = [row[1] for row in rows]

# # Plot the bar graph
# plt.figure(figsize=(14, 7))
# plt.bar(cities, populations, color='mediumseagreen')
# plt.xticks(rotation=45, ha='right')
# plt.xlabel('City')
# plt.ylabel('Population')
# plt.title('Population per City')
# plt.tight_layout()
# plt.show()


def fetch_data():
    conn = sqlite3.connect("combined_data.db")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT city, temperature, currentSpeed, freeFlowSpeed, population
        FROM combined
    ''')
    rows = cursor.fetchall()
    conn.close()
    return rows

def plot_bubble_chart(data):
    temperatures = []
    efficiencies = []
    populations = []
    city_names = []

    for city, temp, current, free, pop in data:
        if free and free > 0 and current is not None:
            efficiency = current / free
            temperatures.append(temp)
            efficiencies.append(efficiency)
            populations.append(pop / 25)
            city_names.append(city)

    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(temperatures, efficiencies, s=populations,
                          alpha=0.6, edgecolors='w', linewidth=0.5, c='dodgerblue')

    plt.xlabel("Temperature (°F)")
    plt.ylabel("Traffic Efficiency (Current / Free Flow Speed)")
    plt.title("Traffic Efficiency vs Temperature")

    for i in range(len(city_names)):
        plt.text(temperatures[i], efficiencies[i], city_names[i], fontsize=8, ha='center')

    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_temperature_vs_speed_drop(data):
    temps = []
    speed_drops = []
    sizes = []
    city_labels = []

    for city, temp, current, free, pop in data:
        if temp is not None and current is not None and free is not None:
            speed_drop = free - current
            temps.append(temp)
            speed_drops.append(speed_drop)
            sizes.append(pop / 25)
            city_labels.append(city)

    plt.figure(figsize=(12, 8))
    plt.scatter(temps, speed_drops, s=sizes, alpha=0.7,
                edgecolors='k', linewidths=0.5, c='tomato')

    for i, label in enumerate(city_labels):
        plt.text(temps[i], speed_drops[i], label, fontsize=8, ha='center', alpha=0.8)

    plt.xlabel("Temperature (°F)")
    plt.ylabel("Speed Drop (Free Flow - Current Speed)")
    plt.title("Impact of Temperature on Traffic Speed Drop")

    plt.grid(True)
    plt.tight_layout()
    plt.show()

data = fetch_data()
plot_bubble_chart(data)
plot_temperature_vs_speed_drop(data)