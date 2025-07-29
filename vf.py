import hashlib
import numpy as np
import numpy_financial as npf
import pandas as pd

from pandas.util import hash_pandas_object

_post_payment_balance_cache = {}


def sort_payments(df_payments: pd.DataFrame) -> pd.DataFrame:
    return df_payments.sort_values(['date', 'id'])


def previous_payment_date(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    df_payments = sort_payments(df_payments)
    opening_date = df_payments['credit_id'].map(df_credits['opening_date'])
    return df_payments.groupby('credit_id')['date'].shift(1).fillna(opening_date)


def gap_days(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    previous_payment_date_ = previous_payment_date(df_payments, df_credits)
    return (df_payments['date'] - previous_payment_date_).dt.days.clip(0)


def cumulative_payments(df_payments: pd.DataFrame) -> pd.Series:
    df_payments = sort_payments(df_payments)
    return df_payments.groupby('credit_id')['amount'].cumsum()


def post_payment_balance_fv(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    annual_interest_rate = df_payments['credit_id'].map(df_credits['annual_interest_rate'])
    cumulative_payments_ = cumulative_payments(df_payments).reindex(df_payments.index)
    payment_amount = df_payments['credit_id'].map(df_credits['payment_amount'])
    amount = df_payments['credit_id'].map(df_credits['amount'])
    post_payment_balance_fv = npf.fv(
        annual_interest_rate / 1200 * 1.16,
        cumulative_payments_ / payment_amount,
        payment_amount,
        -amount,
        0
    )
    post_payment_balance_fv = pd.Series(post_payment_balance_fv, df_payments.index)
    return post_payment_balance_fv.clip(0)


def post_payment_balance_interest_first(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:
    df_payments = sort_payments(df_payments).copy()
    df_payments['gap_days'] = gap_days(df_payments, df_credits)
    df_payments['post_payment_balance_interest_first'] = np.nan
    for credit_id, df_payments_group in df_payments.groupby('credit_id'):
        balance = df_credits.loc[credit_id, 'amount']
        annual_interest_rate = df_credits.loc[credit_id, 'annual_interest_rate']
        accumulated_interest = 0
        post_payment_balance = []
        for _, payment in df_payments_group.iterrows():
            generated_interest = max(payment['gap_days'] * annual_interest_rate / 36000 * balance, 0)
            accumulated_interest = accumulated_interest + generated_interest
            if payment['amount'] < accumulated_interest * 1.16:
                accumulated_interest = accumulated_interest - payment['amount'] / 1.16
            else:
                balance = balance - payment['amount'] + accumulated_interest * 1.16
                accumulated_interest = 0
            post_payment_balance.append(balance)
        df_payments.loc[df_payments_group.index, 'post_payment_balance_interest_first'] = post_payment_balance
    return df_payments['post_payment_balance_interest_first']


def post_payment_balance(df_payments: pd.DataFrame, df_credits: pd.DataFrame) -> pd.Series:

    index_bytes_payments = hash_pandas_object(df_payments.index).values.tobytes()
    df_payments_hash = hashlib.sha256(index_bytes_payments).hexdigest()
    index_bytes_credits = hash_pandas_object(df_credits.index).values.tobytes()
    df_credits_hash = hashlib.sha256(index_bytes_credits).hexdigest()
    cache_key = (df_payments_hash, df_credits_hash)
    if cache_key in _post_payment_balance_cache:
        return _post_payment_balance_cache[cache_key]

    post_payment_balance_fv_ = post_payment_balance_fv(df_payments, df_credits)
    post_payment_balance_interest_first_ = post_payment_balance_interest_first(df_payments, df_credits)
    post_payment_balance = post_payment_balance_interest_first_.where(
        pd.Timestamp('2021-12-01') <= df_payments['credit_id'].map(df_credits['opening_date']),
        post_payment_balance_fv_
    )

    _post_payment_balance_cache[cache_key] = post_payment_balance

    return post_payment_balance


def balance(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    df_payments = df_payments.copy()
    df_payments['post_payment_balance'] = post_payment_balance(df_payments, df_credits)
    df_payments = df_payments[df_payments['date'] <= date]
    balance = df_payments.groupby('credit_id')['post_payment_balance'].min()
    balance = balance.reindex(df_credits.index).fillna(df_credits['amount'])
    return balance
