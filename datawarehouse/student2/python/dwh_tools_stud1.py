import psycopg2
from config_stud1 import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

def establish_connection(server=SERVER, database=DATABASE_OP, username=USERNAME, password=PASSWORD, port=PORT):
    """
    Establishes a connection to the specified PostgreSQL database.
    Args:
        server (str): The server name or IP address.
        database (str): The name of the database.
        username (str): The username for authentication.
        password (str): The password for authentication.
        port (int): The port number for the PostgreSQL server (default is 5432).
    Returns:
        psycopg2.extensions.connection: The connection object.
    """
    try:
        connection = psycopg2.connect(
            host=server,
            database=database,
            user=username,
            password=password,
            port=port
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise
