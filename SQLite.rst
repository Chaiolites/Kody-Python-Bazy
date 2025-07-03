Kod SQLight
-------------------

.. code-block:: python

    import sqlite3
    import time
    con = sqlite3.connect(":memory:")
    cursor = con.cursor()

    #LightSQL

    # Tabela przechowująca dane pracowników
    cursor.execute("""
    CREATE TABLE Pracownik (
        numer_pracownika INTEGER PRIMARY KEY AUTOINCREMENT,
        imie            VARCHAR(50) NOT NULL,
        nazwisko        VARCHAR(50) NOT NULL
    );
    """)

    # Tabela przechowująca dane klientów (sprzedawcy oraz nabywcy)
    cursor.execute("""
    CREATE TABLE Klient (
        numer_dowodu                       VARCHAR(20) PRIMARY KEY,
        imie                               VARCHAR(50) NOT NULL,
        nazwisko                           VARCHAR(50) NOT NULL,
        ilosc_wystawionych_przedmiotow    INTEGER DEFAULT 0,
        wymaga_kaucji                      BOOLEAN DEFAULT FALSE
    )
    """)

    # Tabela rejestrująca transakcje aukcyjne
    cursor.execute("""
    CREATE TABLE LogAukcji (
        id_transakcji    INTEGER PRIMARY KEY AUTOINCREMENT,
        sprzedawca       VARCHAR(20) NOT NULL,
        nabywca          VARCHAR(20),
        data_transakcji  DATE NOT NULL,
        cena_sprzedazy   NUMERIC(10,2) NOT NULL DEFAULT 0,
        numer_pracownika INTEGER NOT NULL,
        FOREIGN KEY (sprzedawca) REFERENCES Klient(numer_dowodu),
        FOREIGN KEY (nabywca) REFERENCES Klient(numer_dowodu),
        FOREIGN KEY (numer_pracownika) REFERENCES Pracownik(numer_pracownika)
    );
    """)

    cursor.execute("""
    INSERT INTO Pracownik (imie, nazwisko) VALUES
    ('Anna', 'Kowalska'),
    ('Marek', 'Nowak'),
    ('Ewa', 'Zielińska'),
    ('Tomasz', 'Wiśniewski'),
    ('Karolina', 'Mazur');
    """)

    # Klienci
    cursor.execute("""
    INSERT INTO Klient (numer_dowodu, imie, nazwisko, ilosc_wystawionych_przedmiotow, wymaga_kaucji) VALUES
    ('ID100001', 'Jan', 'Kowalski', 3, 1),
    ('ID100002', 'Anna', 'Nowak', 5, 0),
    ...
    """)
    # zacznij liczyć czas
    start_time = time.time()

    # przykładowe zapytanie do bazy danych w celu zmierzenia czasu wykonania i pobrania wyniku
    cursor.execute("""
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    """)
    # pobranie wyniku
    results = cursor.fetchall()
    # skończ liczyć czas
    end_time = time.time()
    # policz różnicę czasów od początku do końca
    elapsed = end_time - start_time
    # wypisz czas wykonania
    print(f"Czas wykonania zapytania: {elapsed:.4f} sekundy")

    start_time = time.time()

    # przykładowe zapytanie do bazy danych z podstawową optymalizacją zapytania poprzez dodanie LIMIT 10
    cursor.execute("""
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    LIMIT 10;
    """)
    results = cursor.fetchall()

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"Czas wykonania zapytania: {elapsed:.4f} sekundy")

    # dodanie indeksów do tabeli w celu zwiększenia prędkości wykonania zapytania
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_sprzedawca ON LogAukcji(sprzedawca);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_nabywca ON LogAukcji(nabywca);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_data ON LogAukcji(data_transakcji);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_data_sprzed ON LogAukcji(data_transakcji, sprzedawca);")

    start_time = time.time()

    cursor.execute("""
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    LIMIT 10;
    """)
    results = cursor.fetchall()

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"Czas wykonania zapytania: {elapsed:.4f} sekundy")

    cursor.execute("""
    DROP TABLE LogAukcji
    """)

    cursor.execute("""
    DROP TABLE Klient
    """)

    cursor.execute("""
    DROP TABLE Pracownik
    """)

    connection.commit()
    cursor.close()
    connection.close()
