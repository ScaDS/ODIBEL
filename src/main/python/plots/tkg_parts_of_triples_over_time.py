import os
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def plot_csv_files(input_folder, output_folder):
    # Definieren der Unterordnernamen
    subfolders = ["countObjectsOverTime", "countPredicatesOverTime", "countSubjectsOverTime"]

    # Schleife über jeden Unterordner
    for subfolder in subfolders:
        subfolder_path = os.path.join(input_folder, subfolder)

        if not os.path.exists(subfolder_path):
            print(f"Unterordner {subfolder} existiert nicht. Überspringen...")
            continue

        # Alle CSV-Dateien im Unterordner auflisten
        csv_files = [f for f in os.listdir(subfolder_path) if f.endswith(".csv")]

        for csv_file in csv_files:
            csv_path = os.path.join(subfolder_path, csv_file)

            # CSV-Datei einlesen
            try:
                data = pd.read_csv(csv_path)
            except Exception as e:
                print(f"Fehler beim Lesen der Datei {csv_path}: {e}")
                continue

            # Entferne alle Daten aus 1970
            data = data[~data["time"].str.startswith("1970")]

            # Prüfen, welche Art von Datei vorliegt
            if "new_tail" in data.columns:
                prefix = "tail"
            elif "new_rel" in data.columns:
                prefix = "rel"
            elif "new_head" in data.columns:
                prefix = "head"
            else:
                print(f"Unbekanntes Format in Datei {csv_path}. Überspringen...")
                continue

            # Plot erstellen
            plt.figure(figsize=(12, 7))

            # Spalten plotten
            plt.plot(data["time"], data[f"new_{prefix}"], label=f"new_{prefix}", marker="o")
            plt.plot(data["time"], data[f"ended_{prefix}"], label=f"ended_{prefix}", marker="o")
            plt.plot(data["time"], data[f"valid_{prefix}"], label=f"valid_{prefix}", marker="o")
            plt.plot(data["time"], data["changes"], label="changes", marker="o")

            # Layout anpassen
            plot_title = subfolder.replace("count", "").replace("OverTime", "").capitalize()
            plt.title(f"{plot_title} Trends Over Time")
            plt.xlabel("Time")
            plt.ylabel("Values")

            # Verhindern von Überlappung der x-Achsen-Beschriftungen: Pro Jahr ein Tick
            data["year"] = data["time"].str[:4]  # Extrahiere das Jahr
            unique_years = data["year"].unique()
            year_indices = [data[data["year"] == year].index[0] for year in unique_years]
            plt.xticks(ticks=year_indices, labels=unique_years, rotation=45, fontsize=8)

            plt.yticks(fontsize=8)
            plt.legend(fontsize=10)
            plt.tight_layout()

            # Plot speichern
            os.makedirs(subfolder, exist_ok=True)
            output_file = os.path.join(subfolder, f"{subfolder}_plot.png")
            plt.savefig(output_file)
            plt.close()

            print(f"Plot für {csv_path} gespeichert unter {output_file}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV-Dateien plotten.")
    parser.add_argument("--input", type=str, required=True, help="Pfad zum Hauptordner mit den Unterordnern.")
    parser.add_argument("--output", type=str, required=True, help="Pfad zum Ausgabeordner für die Plots.")

    args = parser.parse_args()

    plot_csv_files(args.input, args.output)
