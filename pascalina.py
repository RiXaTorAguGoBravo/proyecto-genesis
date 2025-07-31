import pandas as pd

from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict, List


def interest_generated(
        balance: float,
        annual_interest_rate: float,
        start_date: date,
        end_date: date
) -> float:
    days_elapsed = (end_date - start_date).days
    interest = days_elapsed * annual_interest_rate / 36000 * balance
    interest = max(0, interest)
    return interest


def amortization_schedule(
        amount: float,
        annual_interest_rate: float,
        payment_amount: float,
        opening_date: date,
        first_payment_date: date
) -> List[Dict[str, float | date]]:
    i = 0
    balance = amount
    amortization_schedule = []
    while 0 <= balance:
        payment_date = first_payment_date + relativedelta(months=i)
        previous_date = first_payment_date + relativedelta(months=i-1) if i != 0 else opening_date
        interest = interest_generated(balance, annual_interest_rate, previous_date, payment_date)
        payment_balance = payment_amount - interest * 1.16
        balance -= payment_balance
        amortization_schedule.append({
            'payment_date': payment_date,
            'moratorium_interest': 0,
            'ordinary_interest': 0,
            'payment_balance': payment_balance
        })
        i += 1
    amortization_schedule[-1]['payment_balance'] += balance
    amortization_schedule.append({
        'payment_date': date(9999, 12, 31),
        'moratorium_interest': 0,
        'ordinary_interest': 0,
        'payment_balance': float('inf')
    })
    return amortization_schedule
    
