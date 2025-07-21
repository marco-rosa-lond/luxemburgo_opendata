
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv("config.env")

def get_database_name():
    return os.getenv('DB_NAME')

def get_connection_string(engine: str, db_master: bool = False) -> str:
    database = 'master' if db_master else os.getenv('DB_NAME')
    server = os.getenv('DB_SERVER')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    driver_name = "ODBC Driver 17 for SQL Server"

    is_localhost = server.lower() == 'localhost'

    if engine == 'sqlalchemy':
        driver = driver_name.replace(" ", "+")
        if is_localhost:
            return f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
        return f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

    if engine == 'pyodbc':
        if is_localhost:
            return (
                f"DRIVER={{{driver_name}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                "Trusted_Connection=yes;"
            )
        else:
            return (
                f"DRIVER={{{driver_name}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )

    if engine not in ['sqlalchemy', 'pyodbc']:
        raise ValueError("Invalid engine provided. Choose 'sqlalchemy' or 'pyodbc'.")

    return ''

def drop_database():
    database_name = get_database_name()
    conn = pyodbc.connect(
        get_connection_string("pyodbc", db_master=True),
        autocommit=True
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
            ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE IF EXISTS [{database_name}];
            """)
            conn.commit()
            print(f"Database dropped: {database_name}")

    except Exception as e:
        print(f"Error dropping database {database_name}: {e}")

    finally:
        conn.close()

def create_database():

    database = os.getenv('DB_NAME')
    collation = os.getenv('DB_COLLATION')

    drop_database()

    conn = pyodbc.connect(
        get_connection_string("pyodbc", db_master=True),
        autocommit=True
    )

    try:
        with conn.cursor() as cursor:

            # Step 1: Create the database if it doesn't exist
            create_db_sql = f"""
                        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{database}')
                        BEGIN
                            CREATE DATABASE [{database}]
                            COLLATE {collation};
                        END
                        """

            cursor.execute(create_db_sql)
            print(f"✅ Database '{database}' created or already exists.")

            # Step 2: Set recovery model to SIMPLE
            set_recovery_sql = f"""
                        ALTER DATABASE [{database}] SET RECOVERY SIMPLE;
                        """

            cursor.execute(set_recovery_sql)


    except Exception as e:
        print(f"Error creating database {database}: {e}")

    finally:
        conn.close()


