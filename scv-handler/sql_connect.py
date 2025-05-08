import pandas as pd
import pyodbc
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Boolean, DateTime,
    ForeignKey, text
)
from datetime import datetime


def sql_query():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=DESKTOP-UBRC685\SQLEXPRESS;"
        r"Database=CSVpower;"
        r"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print("pyodbc: Connected successfully")
    except Exception as e:
        print("pyodbc: Connection failed:", e)
        raise

    # 2) SQLALCHEMY ENGINE & METADATA
    engine = create_engine(
        "mssql+pyodbc://DESKTOP-UBRC685\\SQLEXPRESS/CSVpower"
        "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )
    metadata = MetaData()

    # 3) TABLE DEFINITIONS (autoincrement=False so we keep CSV ids)
    users = Table('users', metadata,
        Column('id',            Integer, primary_key=True, autoincrement=False),
        Column('name',          String(100)),
        Column('phone_number',  String(20)),
        Column('email',         String(100)),
        Column('created_at',    DateTime),
        Column('last_active_at',DateTime),
        Column('status',        String(20)),
        Column('is_vip',        Boolean),
        Column('total_balance', Float)
    )

    cards = Table('cards', metadata,
        Column('id',            Integer, primary_key=True, autoincrement=False),
        Column('user_id',       Integer, ForeignKey('users.id')),
        Column('card_number',   String(20)),
        Column('balance',       Float),
        Column('is_blocked',    Boolean),
        Column('created_at',    DateTime),
        Column('card_type',     String(50)),
        Column('limit_amount',  Float)
    )

    transactions = Table('transactions', metadata,
        Column('id',               Integer, primary_key=True, autoincrement=False),
        Column('from_card_id',     Integer, ForeignKey('cards.id')),
        Column('to_card_id',       Integer, ForeignKey('cards.id')),
        Column('amount',           Float),
        Column('status',           String(20)),
        Column('created_at',       DateTime),
        Column('transaction_type', String(50)),
        Column('is_flagged',       Boolean)
    )

    logs = Table('logs', metadata,
        Column('id',             Integer, primary_key=True, autoincrement=False),
        Column('transaction_id', Integer, ForeignKey('transactions.id')),
        Column('message',        String),
        Column('created_at',     DateTime)
    )

    reports = Table('reports', metadata,
        Column('id',                 Integer, primary_key=True, autoincrement=False),
        Column('report_type',        String(50)),
        Column('created_at',         DateTime),
        Column('total_transactions', Integer),
        Column('flagged_transactions',Integer),
        Column('total_amount',       Float)
    )

    scheduled_payments = Table('scheduled_payments', metadata,
        Column('id',           Integer, primary_key=True, autoincrement=False),
        Column('user_id',      Integer, ForeignKey('users.id')),
        Column('card_id',      Integer, ForeignKey('cards.id')),
        Column('amount',       Float),
        Column('payment_date', DateTime),
        Column('status',       String(20)),
        Column('created_at',   DateTime)
    )

    # Metadata table to track ingestions
    retrieveinfo = Table('retrieveinfo', metadata,
        Column('retrieve_id',    Integer, primary_key=True, autoincrement=True),
        Column('source_file',    String(255)),
        Column('retrieved_at',   DateTime),
        Column('total_rows',     Integer),
        Column('processed_rows', Integer),
        Column('errors',         Integer),
        Column('notes',          String)  # TEXT can be mapped to String in SQLAlchemy
    )

    # 4) CREATE ALL TABLES IF NOT EXIST
    metadata.create_all(engine)
    print("✅ All tables ensured in database")

    # 5) CLEAR OUT OLD DATA (DELETE in child‑to‑parent order)
    delete_order = [
        "logs",
        "scheduled_payments",
        "transactions",
        "cards",
        "users",
        "reports"
    ]
    with engine.begin() as conn:
        for tbl in delete_order:
            conn.execute(text(f"DELETE FROM dbo.{tbl};"))
        print("✅ Old data deleted in all tables")

    # 6) LOAD CLEANED CSVs & LOG METADATA
    csv_table_map = {
        "t01.csv": "users",
        "t02.csv": "cards",
        "t03.csv": "transactions",
        "t04.csv": "logs",
        "t05.csv": "reports",
        "t07.csv": "scheduled_payments"
    }

    folder = r"C:\Users\LENOVO\Desktop\CSV-powerBI\processed_csvs"

    for csv_file, table_name in csv_table_map.items():
        path = f"{folder}\\{table_name}.csv"
        retrieved_at = datetime.now()

        try:
            df = pd.read_csv(path)
            total_rows = len(df)

            # INSERT rows (preserving 'id')
            df.to_sql(table_name, con=engine, if_exists='append', index=False)

            processed_rows = total_rows
            errors = 0
            notes = "Success"

        except Exception as e:
            total_rows = 0
            processed_rows = 0
            errors = 1
            notes = str(e)[:500]




        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO retrieveinfo
                (source_file, retrieved_at, total_rows, processed_rows, errors, notes)
                VALUES
                (:source_file, :retrieved_at, :total_rows,
                :processed_rows, :errors, :notes)
            """), {
                'source_file':    csv_file,
                'retrieved_at':   retrieved_at,
                'total_rows':     total_rows,
                'processed_rows': processed_rows,
                'errors':         errors,
                'notes':          notes
            })

        print(f"✅ Loaded & logged {csv_file} → {table_name}")
