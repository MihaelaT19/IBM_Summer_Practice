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

    # Afișează primele 100 de înregistrări din DataFrame
    df.show(100)

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

def on_load_button_click():
    # Calea către fișierul CSV
    csv_file_path = "C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv"

    # Apelează funcția pentru încărcarea și afișarea datelor
    load_and_show_data(csv_file_path)

def on_process_button_click():
    # Calea către fișierul CSV
    csv_file_path = "C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv"

    # Apelează funcția pentru procesarea datelor
    process_data(csv_file_path)

# Funcția pentru crearea interfeței de login
def create_login_interface():
    root = tk.Tk()
    root.title("Interfață cu funcții pentru încărcarea și procesarea datelor")
    root.geometry("500x350")
    root.configure(bg="black")  # Pentru modul "dark" și tema "dark-blue"

    frame = tk.Frame(master=root)
    frame.pack(pady=20, padx=60, fill="both", expand=True)

    label = tk.Label(master=frame, text="Login System")
    label.pack(pady=12, padx=10)

    entry1 = tk.Entry(master=frame)
    entry1.pack(pady=12, padx=10)
    entry1.insert(0, "Username")  # Placeholder text

    entry2 = tk.Entry(master=frame, show="*")
    entry2.pack(pady=12, padx=10)
    entry2.insert(0, "Password")  # Placeholder text

    def on_login_button_click():
        login()

    button = tk.Button(master=frame, text="Login", command=on_login_button_click)
    button.pack(pady=12, padx=10)

    checkbox = tk.Checkbutton(master=frame, text="Remember me")
    checkbox.pack(pady=12, padx=10)

    load_button = tk.Button(master=frame, text="Încarcă date", command=on_load_button_click)
    load_button.pack(pady=12, padx=10)

    process_button = tk.Button(master=frame, text="Procesează date", command=on_process_button_click)
    process_button.pack(pady=12, padx=10)

    root.mainloop()

# Apelarea funcției pentru crearea interfeței de login
create_login_interface()
