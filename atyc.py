import hashlib
import numpy as np
import pandas as pd

from pandas.util import hash_pandas_object
from typing import Union


_periods_cache = {}


def add_artificial_payments(
        df_payments: pd.DataFrame,
        df_credits: pd.DataFrame,
        date: Union[pd.Timestamp, pd.Series],
        amount: Union[float, pd.Series]
) -> pd.DataFrame:
    if isinstance(date, pd.Series):
        if date.index.has_duplicates:
            raise ValueError('La serie date contiene indices duplicados.')
        if not date.index.isin(df_credits.index).all():
            raise ValueError('El indice de la serie date debe ser un subconjunto del indice de df_credits.')
    if isinstance(amount, pd.Series):
        if amount.index.has_duplicates:
            raise ValueError('La serie amount contiene indices duplicados.')
        if not amount.index.isin(df_credits.index).all():
            raise ValueError('El indice de la serie amount debe ser un subconjunto del indice de df_credits.')
    if isinstance(date, pd.Series) and isinstance(amount, pd.Series) and set(date.index) != set(amount.index):
        raise ValueError('La serie date y la serie amount no contienen los mismos indices.')
    if isinstance(date, pd.Series) and not isinstance(amount, pd.Series):
        amount = pd.Series([amount] * len(date), index=date.index)
    if isinstance(amount, pd.Series) and not isinstance(date, pd.Series):
        date = pd.Series([date] * len(amount), index=amount.index)
    if not isinstance(date, pd.Series) and not isinstance(amount, pd.Series):
        amount = pd.Series([amount] * len(df_credits), index=df_credits.index)
        date = pd.Series([date] * len(df_credits), index=df_credits.index)
    max_id = df_payments.index.max()
    index = range(max_id + 1, max_id + 1 + len(date))
    df_artificial_payments = pd.concat([date, amount, date], axis='columns').reset_index()
    df_artificial_payments.columns = ['credit_id', 'date', 'amount', 'client_payment_date']
    df_artificial_payments.index = index
    df_artificial_payments.index.name = 'id'
    df_artificial_payments = pd.concat([df_payments, df_artificial_payments])
    return df_artificial_payments


def sort_payments(df_payments: pd.DataFrame) -> pd.DataFrame:
    return df_payments.sort_values(['date', 'id'])


def cumulative_payments(df_payments: pd.DataFrame) -> pd.Series:
    df_payments = sort_payments(df_payments)
    return df_payments.groupby('credit_id')['amount'].cumsum()


def payment_progress(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    cumulative_payments_ = cumulative_payments(df_payments)
    payment_amount = df_payments['credit_id'].map(df_credits['payment_amount'])
    return cumulative_payments_ / payment_amount


def payment_progress_period(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    df_payments = sort_payments(df_payments)
    df_aux = df_payments[['credit_id']].copy()
    df_aux['payment_progress'] = payment_progress(df_payments, df_credits)
    return np.floor(df_aux.groupby('credit_id')['payment_progress'].shift(1).fillna(0)) + 1


def periods_table(df_credits: pd.DataFrame) -> pd.DataFrame:

    index_bytes = hash_pandas_object(df_credits.index).values.tobytes()
    df_hash = hashlib.sha256(index_bytes).hexdigest()
    if df_hash in _periods_cache:
        return _periods_cache[df_hash]

    records = []
    for credit_id, row in df_credits.iterrows():
        term = row['term']
        first_payment_date = row['first_payment_date']
        for period in range(term + 2):
            if period == 0:
                payment_date = pd.NaT
            else:
                payment_date = first_payment_date + pd.DateOffset(months=period - 1)
            records.append((credit_id, period, payment_date))
    df_periods = pd.DataFrame(records, columns=['credit_id', 'period', 'payment_date'])
    df_periods = df_periods.set_index(['credit_id', 'period'])

    _periods_cache[df_hash] = df_periods

    return df_periods


def actual_periods_table(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    df_periods_table = periods_table(df_credits)
    df_credits = df_credits[df_credits['opening_date'] <= date].copy()
    df_credits['closing_date'] = df_credits['closing_date'].where(df_credits['closing_date'] <= date)
    df_payments = df_payments[df_payments['date'] <= date].copy()
    df_payments = add_artificial_payments(df_payments, df_credits, date, 1e-10)
    df_payments = add_artificial_payments(df_payments, df_credits, df_credits['opening_date'], df_credits['payment_amount'] * 0.005)
    payment_progress_period_ = payment_progress_period(df_payments, df_credits)
    df_aux = df_payments[['date', 'credit_id', 'amount']].copy()
    df_aux['period'] = payment_progress_period_
    df_aux = df_aux.groupby(['credit_id', 'period']).agg({
        'date': 'max',
        'amount': 'sum'
    })
    df_periods_table = pd.merge(
        df_periods_table,
        df_aux,
        'left',
        left_index=True,
        right_index=True
    )
    df_periods_table['paid'] = df_periods_table[df_periods_table['amount'].notnull()].groupby('credit_id')['amount'].tail(1)
    df_periods_table['paid'] = df_periods_table.groupby('credit_id')['paid'].ffill().isnull()
    df_periods_table['date'] = df_periods_table.groupby('credit_id')['date'].ffill()
    df_periods_table['date'] = df_periods_table['date'].where(df_periods_table['paid'])
    closing_date = df_periods_table.index.get_level_values('credit_id').map(df_credits['closing_date'])
    df_periods_table['delay'] = (df_periods_table['date'] - df_periods_table['payment_date']).dt.days
    df_periods_table['delay'] = df_periods_table['delay'].fillna(
        (closing_date.fillna(date) - df_periods_table['payment_date']).dt.days
    )
    bins = [-float('inf'), 0, 5, 29, 59, 89, 119, 149, 179, float('inf')]
    labels = [1, 1.5, 2, 3, 4, 5, 6, 7, 8]
    delay_category = pd.cut(
        df_periods_table['delay'],
        bins,
        True,
        labels
    )
    df_periods_table['delay_category'] = delay_category
    df_periods_table['delay_category'] = df_periods_table[df_periods_table['amount'].notnull()].groupby('credit_id')['delay_category'].tail(1) #
    df_periods_table['delay_category'] = df_periods_table.groupby('credit_id')['delay_category'].ffill() #
    df_periods_table['delay_category'] = df_periods_table['delay_category'].fillna(delay_category) #
    df_periods_table = df_periods_table.groupby('credit_id').head(-1)
    return df_periods_table

