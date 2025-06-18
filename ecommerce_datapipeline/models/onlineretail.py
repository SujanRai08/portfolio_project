from pydantic import BaseModel, Field,validator
from datetime import datetime
from typing import Optional

class OnlineRetailRecord(BaseModel):
    """
    Pydantic models for validating UK online retial datasets records.
    THis ensures data quality and consistency throughout the etl Pipeline
    """
    invoice_no: str = Field(..., description="Invoice number - unique identifier for each transaction")
    stock_code: str = Field(..., description="Product code - unique identifier for each product")
    description: Optional[str] = Field(None, description="Product description")
    quantity: int = Field(..., description="Quantity of products purchased")
    invoice_date: datetime = Field(..., description="Date and time of the transaction")
    unit_price: float = Field(..., description="Price per unit of the product")
    customer_id: Optional[str] = Field(None, description="Customer identifier")
    country: str = Field(..., description="Country where the customer is located")
    
    @validator('quantity')
    def validate_quantity(cls, v):
        """Ensure quantity is reasonable (handle returns as negative values)"""
        if v == 0:
            raise ValueError('Quantity cannot be zero')
        return v
    
    @validator('unit_price')
    def validate_unit_price(cls, v):
        """Ensure unit price is non-negative"""
        if v < 0:
            raise ValueError('Unit price cannot be negative')
        return v
    
    @validator('invoice_no')
    def validate_invoice_no(cls, v):
        """Ensure invoice number is not empty"""
        if not v or v.strip() == '':
            raise ValueError('Invoice number cannot be empty')
        return v.strip()
    
    @validator('country')
    def validate_country(cls, v):
        """Ensure country is not empty"""
        if not v or v.strip() == '':
            raise ValueError('Country cannot be empty')
        return v.strip()
    

class ProcessedRetailRecord(OnlineRetailRecord):
    """
    Extended model for processed records with additional calculated fields
    """
    total_amount: float = Field(..., description="Calculated total: quantity * unit_price")
    is_return: bool = Field(..., description="True if quantity is negative (return)")
    year: int = Field(..., description="Year extracted from invoice_date")
    month: int = Field(..., description="Month extracted from invoice_date")


    @validator('total_amount', pre=True, always=True)
    def calculate_total_amount(cls, v, values):
        """Calculate total amount from quantity and unit price"""
        quantity = values.get('quantity', 0)
        unit_price = values.get('unit_price', 0)
        return quantity * unit_price
    
    @validator('is_return', pre=True, always=True)
    def determine_is_return(cls, v, values):
        """Determine if this is a return based on negative quantity"""
        quantity = values.get('quantity', 0)
        return quantity < 0
    
    @validator('year', pre=True, always=True)
    def extract_year(cls, v, values):
        """Extract year from invoice_date"""
        date = values.get('invoice_date')
        return date.year if date else None
    
    @validator('month', pre=True, always=True)
    def extract_month(cls, v, values):
        """Extract month from invoice_date"""
        date = values.get('invoice_date')
        return date.month if date else None



    
