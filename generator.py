import random
import time
from datetime import datetime, timedelta

NUM_ROWS = 1_000_000_000
OUTPUT_FILE = "measurements.txt"

CITIES = {
    "Oslo": (-20, 25),
    "São Paulo": (10, 35),
    "New York": (-10, 35),
    "Tokyo": (0, 35),
    "Moscow": (-25, 30),
    "Cape Town": (5, 30),
    "Sydney": (5, 45),
    "Reykjavik": (-15, 20),
    "Dubai": (20, 50),
    "Buenos Aires": (5, 35),
}

def generate_temperature(city: str) -> float:
    min_temp, max_temp = CITIES[city]
    base_temp = random.uniform(min_temp, max_temp)
    variation = random.uniform(-2, 2)
    return round(base_temp + variation, 1)

def main():
    cities = list(CITIES.keys())
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i in range(NUM_ROWS):
            city = random.choice(cities)
            temp = generate_temperature(city)
            f.write(f"{city};{temp}\n")

            if i % 1_000_000 == 0 and i > 0:
                print(f"{i:,} rows written...")

    print(f"Done. File saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    start = time.time()
    main()
    print(f"Elapsed: {time.time() - start:.2f}s")
