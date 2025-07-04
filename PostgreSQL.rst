Kod bazy danych:
=====================

PostgreSQL
---------------

.. code-block:: python

    import simplejson                                                                          # umożliwia ładowanie plików JSON z dodatkową obsługą typów
    from sqlalchemy import create_engine, text                                                 # create_engine tworzy połączenie z bazą, text dla zapytań SQL

#Wczytanie poświadczeń z pliku JSON

    with open("/home/student06/Bazy/database_creds.json") as db_con_file:
        creds = simplejson.loads(db_con_file.read())                                    # creds to słownik: {'user_name': ..., 'password': ..., 'host_name': ..., 'port_number': ..., 'db_name': ...}

# 2. Zbudowanie łańcucha połączenia do PostgreSQL w formacie SQLAlchemy
    connection = 'postgresql+psycopg://' + \
                    creds['user_name'] + ':' + creds['password'] + '@' + \
                    creds['host_name'] + ':' + creds['port_number'] + '/' + \
                    creds['db_name']

    engine = create_engine(connection)            # create_engine otwiera silnik, ale nie wykonuje od razu połączenia

    # POSTGRESQL
    # Tabela przechowująca dane pracowników 
    #Definicja i wykonanie tworzenia tabeli Pracownik
    create_table_Pracownik = """
    CREATE TABLE Pracownik (
        numer_pracownika SERIAL PRIMARY KEY,
        imie VARCHAR(50) NOT NULL,
        nazwisko VARCHAR(50) NOT NULL
    );"""

    with engine.begin() as conn:                                 # begin() otwiera transakcję, która się automatycznie zatwierdzi lub wycofa
        conn.execute(text(create_table_Pracownik))

    # Tabela przechowująca dane klientów (zarówno sprzedawców, jak i nabywców)
#  Definicja i wykonanie tworzenia tabeli Klient
    create_table_Klient = """
    CREATE TABLE Klient (
        numer_dowodu VARCHAR(20) PRIMARY KEY,
        imie VARCHAR(50) NOT NULL,
        nazwisko VARCHAR(50) NOT NULL,
        ilosc_wystawionych_przedmiotow INT DEFAULT 0,
        wymaga_kaucji BOOLEAN DEFAULT FALSE
    );"""

    with engine.begin() as conn:
        conn.execute(text(create_table_Klient))

    # Tabela rejestrująca transakcje aukcyjne
    # Definicja i wykonanie tworzenia tabeli LogAukcji z kluczami obcymi
    create_table_LogAukcji = """
    CREATE TABLE LogAukcji (
        id_transakcji SERIAL PRIMARY KEY,
        sprzedawca VARCHAR(20) NOT NULL,
        nabywca VARCHAR(20),
        data_transakcji DATE NOT NULL,
        cena_sprzedazy NUMERIC(10,2) NOT NULL DEFAULT 0,
        numer_pracownika INT NOT NULL,
        CONSTRAINT fk_sprzedawca FOREIGN KEY (sprzedawca) REFERENCES Klient (numer_dowodu),
        CONSTRAINT fk_nabywca FOREIGN KEY (nabywca) REFERENCES Klient (numer_dowodu),
        CONSTRAINT fk_pracownik FOREIGN KEY (numer_pracownika) REFERENCES Pracownik (numer_pracownika)
    );"""

    with engine.begin() as conn:
        conn.execute(text(create_table_LogAukcji))

# Wstawianie przykładowych danych do tabeli Pracownik

    insert_Pracownik = """
    INSERT INTO Pracownik (imie, nazwisko) VALUES
    ('Anna', 'Kowalska'),
    ('Jan', 'Nowak'),
    ...
    ('Patryk', 'Majewski');
    """

    with engine.begin() as conn:
        conn.execute(text(insert_Pracownik))

# Wstawianie przykładowych danych do tabeli Klient

    insert_Klient = """
    INSERT INTO Klient (numer_dowodu, imie, nazwisko, ilosc_wystawionych_przedmiotow, wymaga_kaucji) VALUES
    ('ID100001', 'Jan', 'Kowalski', 3, TRUE),
    ...
    ('ID100200', 'Katarzyna', 'Czarnecka', 2, FALSE);
    """

    with engine.begin() as conn:
        conn.execute(text(insert_Klient))

# Wstawianie przykładowych logów aukcji do tabeli LogAukcji

    insert_LogAukcji = """
    INSERT INTO LogAukcji (sprzedawca, nabywca, data_transakcji, cena_sprzedazy, numer_pracownika) VALUES
    ('ID100001', 'ID100050', '2025-06-01', 150.75, 1),
    ...
    ('ID100095', 'ID100071', '2025-09...
    """

    with engine.begin() as conn:
        conn.execute(text(insert_LogAukcji))
    # przykładowe zapytanie do bazy danych w celu zmierzenia czasu wykonania i pobrania wyniku
    # EXPLAIN ANALYZE – pomiar czasu wykonania i użycia buforów
    explain_limit = """
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    """

    # Wypisanie planu wykonania na konsolę

    with engine.begin() as conn:
        # pierwsze wykonanie: tylko pobranie planu
        conn.execute(text(explain_limit))

    # wypisanie wyniku funkcji EXPLAIN, interesującym nas parametrem jest czas wykonania
    with engine.begin() as conn:
        result = conn.execute(text(explain_limit))
        for row in result:
            print(row[0])        # każdy wiersz to fragment planu EXPLAIN

    # przykładowe zapytanie do bazy danych z podstawową optymalizacją zapytań poprzez dodanie LIMIT 10
    # To samo zapytanie z dodaniem LIMIT 10 dla porównania
    explain_limit = """
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    LIMIT 10;
    """

    with engine.begin() as conn:
        conn.execute(text(explain_limit))

    with engine.begin() as conn:
        result = conn.execute(text(explain_limit))
        for row in result:
            print(row[0])  # plan wykonania jako pojedyncze linie tekstu

    # dodanie indeksów do tabeli w celu zwiększenia prędkości wykonania zapytania
    # Dodawanie indeksów dla optymalizacji zapytań
    create_indexes = """
    CREATE INDEX IF NOT EXISTS idx_log_sprzedawca  ON LogAukcji(sprzedawca);
    CREATE INDEX IF NOT EXISTS idx_log_nabywca     ON LogAukcji(nabywca);
    CREATE INDEX IF NOT EXISTS idx_log_data        ON LogAukcji(data_transakcji);
    CREATE INDEX IF NOT EXISTS idx_log_data_sprzed ON LogAukcji(data_transakcji, sprzedawca);
    """
    with engine.begin() as conn:
        conn.execute(text(create_indexes))

    explain_limit = """
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT k.imie, k.nazwisko, COUNT(*) AS cnt
    FROM LogAukcji l
    JOIN Klient k ON l.sprzedawca = k.numer_dowodu
    WHERE l.data_transakcji BETWEEN '2025-06-01' AND '2025-12-31'
    GROUP BY k.imie, k.nazwisko
    ORDER BY cnt DESC
    LIMIT 10;
    """
    # Powtórne EXPLAIN po dodaniu indeksów – sprawdzenie przyspieszenia

    with engine.begin() as conn:
        conn.execute(text(explain_limit))

    with engine.begin() as conn:
        result = conn.execute(text(explain_limit))
        for row in result:
            print(row[0])

    # usunięcie tabel - rollback struktury bazy
    Drop3 = """
    DROP TABLE LogAukcji
    """
    with engine.begin() as conn:
        conn.execute(text(Drop3))

    Drop1 = """
    DROP TABLE Pracownik
    """
    with engine.begin() as conn:
        conn.execute(text(Drop1))

    Drop2 = """
    DROP TABLE Klient
    """
    with engine.begin() as conn:
        conn.execute(text(Drop2))
