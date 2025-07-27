import csv
import sys
from sqlalchemy import ( create_engine, Table, Column, MetaData, String, CHAR, Date)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from src.config import DB_CONFIG

CSV_FILE_PATH = 'database_link/outdoor_2025/link_gare.csv'


def get_sqlalchemy_connection_string():
    """Generates the connection string for SQLAlchemy."""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def define_gare_table(metadata):
    """
    Defines the 'gare' table schema using SQLAlchemy's Table object.
    This is the Python representation of your database table.
    """
    return Table('gare', metadata,
        Column('codice', String(20), primary_key=True),
        Column('data', Date),
        Column('nome', String(1000)),
        Column('livello', CHAR(1)),
        Column('sigma', String(20)),
        Column('status', String(100)),
        Column('aggiornato', Date),
        Column('link_gara', String(1000)),
        Column('link_sigma', String(200)),
        Column('link_risultati', String(200))
    )


def process_csv_and_upsert(engine, table_def):
    """
    Reads the CSV, transforms the data, and performs a bulk "upsert"
    (insert or update) into the database.
    """
    print(f"Starting to process CSV file: {CSV_FILE_PATH}")
    
    records_to_upsert = []
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='~')
            next(csv_reader) # skip first row containing header
            
            for row in csv_reader:
                try:
                    # Pad the row to prevent IndexError on malformed lines
                    row.extend([None] * (9 - len(row)))
                    
                    # --- 1. Extract data from CSV row ---
                    data_str, codice, nome, link_gara, link_sigma, link_risultati, sigma_raw, status, aggiornato_str = row[:9]

                    # --- 2. Transform data based on your rules ---
                    if not codice:
                        print(f"Skipping row with no 'codice': {row}")
                        continue # A primary key is required

                    if codice.strip().upper().startswith('REG'):
                        livello = 'R'
                    elif codice.strip().upper().startswith('COD'):
                        livello = 'N'
                    else:
                        livello = None

                    # Use None for empty strings so they are inserted as NULL
                    record = {
                        "codice": codice,
                        "livello": livello,
                        "nome": nome or None,
                        "data": data_str or None,
                        "sigma": sigma_raw or None,
                        "aggiornato": aggiornato_str or None,
                        "status": status or None,
                        "link_gara": link_gara or None,
                        "link_sigma": link_sigma or None,
                        "link_risultati": link_risultati or None,
                    }
                    records_to_upsert.append(record)
                    
                except (ValueError, IndexError) as e:
                    print(f"Skipping malformed row: {row}. Error: {e}")
                    continue
        
        if not records_to_upsert:
            print("No valid records found in CSV to process.")
            return

        print(f"Found {len(records_to_upsert)} records to upsert.")

        # --- 3. Perform the bulk upsert ---
        # This statement will be used for the upsert operation
        insert_stmt = pg_insert(table_def).values(records_to_upsert)

        # On conflict (duplicate 'codice'), update the existing row
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['codice'], # The constraint to check
            # The columns to update with the new values
            set_={
                'livello': insert_stmt.excluded.livello,
                'nome': insert_stmt.excluded.nome,
                'data': insert_stmt.excluded.data,
                'sigma': insert_stmt.excluded.sigma,
                'aggiornato': insert_stmt.excluded.aggiornato,
                'status': insert_stmt.excluded.status,
                'link_gara': insert_stmt.excluded.link_gara,
                'link_sigma': insert_stmt.excluded.link_sigma,
                'link_risultati': insert_stmt.excluded.link_risultati
            }
        )
        
        # Connect to the DB and execute in a transaction
        with engine.connect() as conn:
            with conn.begin() as trans: # Starts a transaction
                conn.execute(upsert_stmt)
                # The transaction is automatically committed on success
            print("Bulk upsert completed successfully.")

    except FileNotFoundError:
        print(f"Error: The file was not found at {CSV_FILE_PATH}")
        sys.exit(1)
    except SQLAlchemyError as e:
        print(f"An error occurred during the database operation: {e}")
        # The transaction is automatically rolled back due to the 'with' block
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)


def main():
    """Main function to orchestrate the database import process."""
    print("Initializing database import process...")
    
    connection_string = get_sqlalchemy_connection_string()
    engine = create_engine(connection_string)
    
    metadata = MetaData()
    
    # Step 1: Define the table structure in Python
    gare_table = define_gare_table(metadata)
    
    # Step 2: Create the table in the database if it doesn't exist
    # This is safe to run multiple times.
    metadata.create_all(engine)
    print("Table 'gare' is defined and created if it didn't exist.")
    
    # Step 3: Process the CSV and load the data
    process_csv_and_upsert(engine, gare_table)
    
    print("Process finished.")

if __name__ == "__main__":
    main()
