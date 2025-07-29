import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Date, TIMESTAMP, Float, Integer, Numeric
from tony_models import Credit, Payment


engine = create_engine(
    r'postgresql://ricardo.torres:UW`bv&&rg>2!kwo\eOaD@34.123.217.240:5432/credito'
)


def load_credits(engine=engine) -> pd.DataFrame:
    return pd.read_sql_table('credits', engine, index_col='id')


def load_credits_assignment_history(engine=engine) -> pd.DataFrame:
    return pd.read_sql_table('credits_assignment_history', engine, index_col='id')


def load_payments(engine=engine) -> pd.DataFrame:
    return pd.read_sql_table('payments', engine, index_col='id')


def load_users(engine=engine) -> pd.DataFrame:
    return pd.read_sql_table('users', engine, index_col='id')


####################################################################################################


def _convert_types(df: pd.DataFrame, model) -> pd.DataFrame:
    for column in model.__table__.columns:
        if column.name not in df.columns:
            continue
        elif isinstance(column.type, (Date, TIMESTAMP)):
            df[column.name] = pd.to_datetime(df[column.name], errors='coerce')
        elif isinstance(column.type, (Float, Numeric)):
            df[column.name] = pd.to_numeric(df[column.name], errors='coerce')
    return df


def get_all_credits(session: Session) -> pd.DataFrame:
    credits = session.query(Credit).all()
    data = [credit.__dict__ for credit in credits]
    for row in data:
        row.pop('_sa_instance_state', None)
    df_credits = pd.DataFrame(data).set_index('id')
    return _convert_types(df_credits, Credit)


def get_all_payments(session: Session) -> pd.DataFrame:
    payments = session.query(Payment).all()
    data = [payment.__dict__ for payment in payments]
    for row in data:
        row.pop('_sa_instance_state', None)
    df_payments = pd.DataFrame(data).set_index('id')
    return _convert_types(df_payments, Payment)