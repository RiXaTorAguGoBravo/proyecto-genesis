import numpy as np
import pandas as pd


def required_payments(df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    last_day_of_month = date + pd.offsets.MonthEnd(0)
    is_last_day_of_month = date == last_day_of_month
    required_payments = (
        (date.year - df_credits['first_payment_date'].dt.year) * 12
        + (date.month - df_credits['first_payment_date'].dt.month)
        - ((not is_last_day_of_month) & (date.day < df_credits['first_payment_date'].dt.day)).astype('int')
        + 1
    )
    required_payments = required_payments.clip(0, df_credits['term'])
    required_payments = required_payments.where(df_credits['opening_date'] <= date)
    return required_payments


def required_amount(df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    return df_credits['payment_amount'] * required_payments(df_credits, date)


def paid_amount(df_payments: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    df_payments = df_payments[df_payments['date'] <= date]
    paid_amount = df_payments.groupby('credit_id')['amount'].sum()
    return paid_amount


def status(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    required_amount_ = required_amount(df_credits, date)
    paid_amount_ = paid_amount(df_payments, date).reindex(df_credits.index).fillna(0)
    status = pd.Series(pd.NA, required_amount_.index, name='status')
    status[paid_amount_ < required_amount_ * 0.98] = 'late'
    status[(required_amount_ * 0.98 <= paid_amount_) & (paid_amount_ <= required_amount_)] = 'on_time'
    status[paid_amount_ > required_amount_] = 'ahead'
    return status


def last_payment_date(df_payments: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    df_payments = df_payments[df_payments['date'] <= date].sort_values('date')
    last_payment_date = df_payments.groupby('credit_id')['date'].last()
    return last_payment_date


def days_without_payment(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    last_payment_date_ = last_payment_date(df_payments, date).reindex(df_credits.index)
    days_without_payment = ((date - last_payment_date_).dt.days - 30).clip(0)
    days_without_payment = days_without_payment.fillna(
        (date - df_credits['opening_date']).dt.days
    )
    return days_without_payment


def missed_payments(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    required_payments_ = required_payments(df_credits, date)
    paid_amount_ = paid_amount(df_payments, date).reindex(df_credits.index).fillna(0)
    return required_payments_ - np.round(paid_amount_ / df_credits['payment_amount'] + 1e-10, 0)


def parity_logic(row: pd.Series, date: pd.Timestamp):
    if date < row['opening_date'] or row['closing_date'] < date:
        return None
    if row['missed_payments'] <= 0:
        if row['status'] != 'late':
            return 0
        # if row['days_without_payment'] > 360:
        #     return 360
        # if row['days_without_payment'] > 270:
        #     return 270
        if row['days_without_payment'] > 180:
            return 180
        if row['days_without_payment'] > 150:
            return 150
        if row['days_without_payment'] > 120:
            return 120
        if row['days_without_payment'] > 90:
            return 90
        if row['days_without_payment'] > 60:
            return 60
        if row['days_without_payment'] > 30:
            return 30
        return 1
    # if row['days_without_payment'] > 360 or row['missed_payments'] >= 13:
    #     return 360
    # if row['days_without_payment'] > 270 or row['missed_payments'] >= 10:
    #     return 270
    if row['days_without_payment'] > 180 or row['missed_payments'] >= 7:
        return 180
    if row['days_without_payment'] > 150 or row['missed_payments'] >= 6:
        return 150
    if row['days_without_payment'] > 120 or row['missed_payments'] >= 5:
        return 120
    if row['days_without_payment'] > 90 or row['missed_payments'] >= 4:
        return 90
    if row['days_without_payment'] > 60 or row['missed_payments'] >= 3:
        return 60
    if row['days_without_payment'] > 30 or row['missed_payments'] >= 2:
        return 30
    return 1


def parity(df_payments: pd.DataFrame, df_credits: pd.DataFrame, date: pd.Timestamp):
    df = df_credits[['opening_date', 'closing_date']].copy()
    df['status'] = status(df_payments, df_credits, date)
    df['days_without_payment'] = days_without_payment(df_payments, df_credits, date)
    df['missed_payments'] = missed_payments(df_payments, df_credits, date)
    parity = df.apply(lambda row: parity_logic(row, date), axis='columns')
    parity.name = date
    return parity

