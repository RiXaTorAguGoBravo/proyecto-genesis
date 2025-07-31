from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, Float, ForeignKey, Integer, JSON, Numeric, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from base import Base


class Payment(Base):
    __tablename__ = 'payments'

    id = Column(BigInteger, primary_key=True)
    reference = Column(String)
    date = Column(Date)
    credit_id = Column(BigInteger, ForeignKey('credits.id'))
    inserted_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    granter = Column(String)
    uuid = Column(UUID)
    transferred = Column(Boolean)
    currency = Column(String)
    amount = Column(Float)
    quota = Column(JSON)
    adjustment_type = Column(String)
    adjustment_date = Column(Date)
    client_payment_date = Column(Date)
    bank_registration_date = Column(Date)

    credit = relationship('Credit', back_populates='payments')

