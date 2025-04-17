import sqlite3

def compute_ratios_and_write_to_file(db_path="combined_data.db", output_file="calculations.txt"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    query = """
    SELECT city, currentSpeed, freeFlowSpeed, temperature
    FROM combined
    WHERE currentSpeed IS NOT NULL AND freeFlowSpeed IS NOT NULL AND temperature IS NOT NULL
    """
    rows = cur.execute(query).fetchall()
    conn.close()

    results = []
    for row in rows:
        city, current_speed, free_flow_speed, temperature = row
        if free_flow_speed != 0 and temperature != 0:
            speed_ratio = current_speed / free_flow_speed
            temp_adjusted_ratio = (free_flow_speed - current_speed) / temperature
            results.append(f"{city}: speed_ratio = {speed_ratio:.3f}, adjusted_temp_ratio = {temp_adjusted_ratio:.3f}")

    with open(output_file, "w") as f:
        f.write("\n".join(results))


if __name__ == "__main__":
    compute_ratios_and_write_to_file()

wivburpb


