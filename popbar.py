import sqlite3
import matplotlib.pyplot as plt
import pandas as pd

# Connect to the database
conn = sqlite3.connect('combined_data.db')
cursor = conn.cursor()

# Query the city names and populations
cursor.execute("SELECT city, population FROM combined") 
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

def plot_traffic_vs_temperature_overlay(data, sort_by="efficiency"):
    rows = []
    for city, temp, current, free, pop in data:
        if free and free > 0 and current is not None:
            efficiency = current / free
            rows.append({
                "city": city,
                "temperature": temp,
                "efficiency": efficiency,
                "population": pop
            })
    df = pd.DataFrame(rows)

    def pop_tier(pop):
        if pop < 50000:
            return 'Small'
        elif pop <= 150000:
            return 'Medium'
        else:
            return 'Large'

    df['pop_tier'] = df['population'].apply(pop_tier)
    color_map = {'Small': 'skyblue', 'Medium': 'orange', 'Large': 'green'}
    df['color'] = df['pop_tier'].map(color_map)

    df = df.sort_values(by=sort_by, ascending=False)

    fig, ax1 = plt.subplots(figsize=(14, 8))

    ax1.bar(df['city'], df['efficiency'], color=df['color'])
    ax1.set_ylabel('Traffic Efficiency (Current / Free Flow)', color='black')
    ax1.tick_params(axis='x', rotation=45)

    custom_legend = [plt.Line2D([0], [0], color=color_map[tier], lw=10) for tier in color_map]
    ax1.legend(custom_legend, color_map.keys(), title="Population Tier")

    ax2 = ax1.twinx()
    ax2.plot(df['city'], df['temperature'], color='red', marker='o', linewidth=2, label='Temperature')
    ax2.set_ylabel('Temperature (°F)', color='red')
    ax2.tick_params(axis='y', labelcolor='red')

    plt.title("Traffic Efficiency with Temperature Overlay")
    plt.tight_layout()
    plt.show()

def normalize_column(data_column):
    min_val = min(data_column)
    max_val = max(data_column)
    if max_val == min_val:
        return [0.5] * len(data_column)
    return [(val - min_val) / (max_val - min_val) for val in data_column]

def plot_heatmap_matrix(data):
    cities = []
    efficiencies = []
    temperatures = []
    populations = []

    for city, temp, current, free, pop in data:
        if free and free > 0 and current is not None and temp is not None and pop is not None:
            efficiency = current / free
            cities.append(city)
            efficiencies.append(efficiency)
            temperatures.append(temp)
            populations.append(pop)

    norm_eff = normalize_column(efficiencies)
    norm_temp = normalize_column(temperatures)
    norm_pop = normalize_column(populations)

    heatmap_data = []
    for i in range(len(cities)):
        heatmap_data.append([norm_eff[i], norm_temp[i], norm_pop[i]])

    fig, ax = plt.subplots(figsize=(10, len(cities) * 0.4))
    im = ax.imshow(heatmap_data, aspect='auto', cmap='coolwarm')

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(['Efficiency', 'Temperature', 'Population'])
    ax.set_yticks(range(len(cities)))
    ax.set_yticklabels(cities)

    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Scaled Metric (0 to 1)")

    ax.set_title("City Metrics Heatmap (Matplotlib Only)")
    plt.tight_layout()
    plt.show()


data = fetch_data()
plot_bubble_chart(data)
plot_temperature_vs_speed_drop(data)
plot_traffic_vs_temperature_overlay(data, sort_by="efficiency")
plot_heatmap_matrix(data)
