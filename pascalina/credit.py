from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, Float, Integer, JSON, Numeric, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from base import Base


class Credit(Base):
    __tablename__ = 'credits'

    id = Column(BigInteger, primary_key=True)
    uuid = Column(UUID)
    payment_type = Column(String)
    annual_interest_rate = Column(Numeric)
    opening_date = Column(Date)
    term = Column(Integer)
    customer_id = Column(BigInteger)
    inserted_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    paydays = Column(ARRAY(Integer))
    monthly_interest_rate = Column(Numeric)
    salesforce_credit_id = Column(String)
    first_payment_date = Column(Date)
    deposit_reference = Column(String)
    user_id = Column(BigInteger)
    status = Column(JSON)
    repair_reference = Column(String)
    open = Column(Boolean)
    country = Column(String)
    credit_tag = Column(String)
    closing_date = Column(Date)
    closing_reason = Column(String)
    batch = Column(String)
    product_id = Column(BigInteger)
    moratorium_loan_rate = Column(Numeric)
    data = Column(JSON)
    amount = Column(Float)
    payment_amount = Column(Float)
    direct_payment = Column(Boolean)
    operational_opening_date = Column(Date)
    origin = Column(String)

    payments = relationship('Payment', back_populates='credit')

