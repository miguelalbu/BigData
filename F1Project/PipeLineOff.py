import pandas as pd
import os

# Ler os arquivos CSV
drivers = pd.read_csv("F1Project/data/drivers.csv")
races = pd.read_csv("F1Project/data/races.csv")
results = pd.read_csv("F1Project/data/results.csv")

# Processamento equivalente
results_filtered = results[results["position"] == "1"]
winners_count = results_filtered.groupby("driverId").size().reset_index(name="wins")
winners_count = winners_count.merge(drivers[["driverId", "forename", "surname"]], on="driverId")
winners_count["driver_name"] = winners_count["forename"] + " " + winners_count["surname"]
winners_count = winners_count.sort_values("wins", ascending=False)

# Salvar como CSV
os.makedirs("output", exist_ok=True)
winners_count[["driver_name", "wins"]].to_csv("output/winners_count.csv", index=False)

print("Arquivo gerado em: output/winners_count.csv")