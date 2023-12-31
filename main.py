
import tkinter as tk
from pyspark.sql import SparkSession

def load_and_show_data(csv_path):
    # Inițializează sau obține o instanță SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Citirea fișierului CSV într-un DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Afișează primele 100 de înregistrări din DataFrame
    df.show(100)

def process_data(csv_path):
    # Inițializează sau obține o instanță SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Citirea fișierului CSV într-un DataFrame
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Var1 - Filtrare
    df_filter = df.filter(
        (df['Receiving Country Code'] == 'LV') |
        (df['Receiving Country Code'] == 'MK') |
        (df['Receiving Country Code'] == 'MT')
    ).select('Receiving Country Code', 'Sending Country Code')
    df_filter.show()

    # Var2 - groupBy
    df_groupby = df.groupby(df['Receiving Country Code'], df['Sending Country Code']).count()
    df_groupby.show(1004)

# Funcția pentru butonul "Încarcă date"
def on_load_button_click():
    # Calea către fișierul CSV
    csv_file_path = "C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv"

    # Apelează funcția pentru încărcarea și afișarea datelor
    load_and_show_data(csv_file_path)

# Funcția pentru butonul "Procesează date"
def on_process_button_click():
    # Calea către fișierul CSV
    csv_file_path = "C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv"

    # Apelează funcția pentru procesarea datelor
    process_data(csv_file_path)

# Crează fereastra interfeței

root = tk.Tk()
root.title("Interfață cu funcții pentru încărcarea și procesarea datelor")
root.configure(bg="black")

# Adaugă butonul pentru încărcarea datelor
load_button = tk.Button(root, text="Încarcă date", command=on_load_button_click)
load_button.pack(pady=12, padx=10)

# Adaugă butonul pentru procesarea datelor
process_button = tk.Button(root, text="Procesează date", command=on_process_button_click)
process_button.pack(pady=12, padx=10)

# Rulează bucla principală a interfeței
root.mainloop()
