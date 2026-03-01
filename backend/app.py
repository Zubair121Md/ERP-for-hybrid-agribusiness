from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from pathlib import Path
import io
import re

# OPTIMIZED: Pre-compile regex patterns for better performance (compile once, use many times)
# Time Complexity: O(1) per match instead of O(m) where m=pattern length
REGEX_PATTERNS = {
    'currency': re.compile(r'[₹$€£]'),
    'indian_number': re.compile(r'[\d,]+\.?\d*'),
    'month_yyyy_mm': re.compile(r'(\d{4}[-/]\d{2})|([A-Za-z]{3}[-/]\d{4})'),
    'period_date': re.compile(r'\d{1,2}[-/]\w{3}[-/]\d{2,4}'),
    'period_month': re.compile(r'\w{3}[-/]\d{2,4}'),
    'period_iso': re.compile(r'\d{4}[-/]\d{2}'),
    'quantity_unit': re.compile(r'([\d,]+\.?\d*)\s*([A-Za-z]*)'),
}

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./fruit_vegetable_costs.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Helper function for unit conversion
def _to_kg(product_name: str, quantity: float, unit: str) -> float:
    """Convert EA quantities to kg using product-specific conversion factors"""
    if not unit:
        return quantity
    u = unit.upper()
    # Extendable map for EA conversions
    EA_CONV_G = {
        'BUTTON MUSHROOM': 200.0,   # grams per EA
        'BABY CORN': 200.0,         # grams per EA
    }
    if u in ['EA', 'EACH', 'PC', 'PCS', 'UNIT', 'UNITS']:
        for key, g in EA_CONV_G.items():
            if key in product_name.upper():
                return (quantity * g) / 1000.0
        # No conversion → treat as value-only items
        return 0.0
    return quantity

def compute_inhouse_outsourced_ratios(db: Session, alpha: float = 0.5) -> tuple:
    """
    Compute dynamic segment ratios from current sales data
    OPTIMIZED: Single-pass iteration with pre-loaded product map
    Time Complexity: O(n) where n = number of sales records
    
    Args:
        db: Database session
        alpha: Weight for weight vs value (0.5 = 50% weight, 50% value)
    
    Returns:
        (inhouse_ratio, outsourced_ratio) tuple
    """
    # OPTIMIZED: Load all products once into a map for O(1) lookup
    products = db.query(Product).all()
    product_map = {p.id: p for p in products}  # O(m) where m = products
    
    # OPTIMIZED: Single query with join, iterate once
    sales = db.query(MonthlySale).all()  # O(n) where n = sales
    in_w = 0.0; out_w = 0.0
    in_v = 0.0; out_v = 0.0
    
    # Single-pass iteration: O(n)
    for s in sales:
        product = product_map.get(s.product_id)
        if not product:
            continue
            
        qty_kg = _to_kg(product.name, s.quantity, product.unit)
        rev = s.quantity * s.sale_price
        
        if product.source == "inhouse":
            in_w += qty_kg
            in_v += rev
        else:
            out_w += qty_kg
            out_v += rev

    # Compute shares with safety - O(1)
    total_w = in_w + out_w
    total_v = in_v + out_v
    in_w_share = (in_w / total_w) if total_w > 0 else 0.0
    out_w_share = (out_w / total_w) if total_w > 0 else 0.0
    in_v_share = (in_v / total_v) if total_v > 0 else 0.0
    out_v_share = (out_v / total_v) if total_v > 0 else 0.0

    # Hybrid segment ratio: α*weight + (1-α)*value
    in_ratio = alpha * in_w_share + (1 - alpha) * in_v_share
    out_ratio = alpha * out_w_share + (1 - alpha) * out_v_share
    
    # Normalize in case of numeric drift
    total = in_ratio + out_ratio
    if total > 0:
        in_ratio /= total
        out_ratio /= total
    else:
        # Fallback if no data
        in_ratio, out_ratio = 0.1822, 0.8178

    print(f"📊 DYNAMIC SEGMENT RATIOS (hybrid α={alpha:.2f}):")
    print(f"   📦 Weight: Inhouse {in_w:.2f}kg ({in_w_share:.1%}), Outsourced {out_w:.2f}kg ({out_w_share:.1%})")
    print(f"   💰 Value: Inhouse ₹{in_v:,.2f} ({in_v_share:.1%}), Outsourced ₹{out_v:,.2f} ({out_v_share:.1%})")
    print(f"   🎯 Final: Inhouse {in_ratio:.4f} ({in_ratio:.1%}), Outsourced {out_ratio:.4f} ({out_ratio:.1%})")
    
    return in_ratio, out_ratio

# Database Models
class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    source = Column(String)  # "inhouse" or "outsourced"
    unit = Column(String, default="kg")
    extra_info = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    monthly_sales = relationship("MonthlySale", back_populates="product")
    allocations = relationship("Allocation", back_populates="product")

class MonthlySale(Base):
    __tablename__ = "monthly_sales"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    month = Column(String, index=True)  # Format: "2025-10"
    quantity = Column(Float)  # Outward quantity (sold)
    sale_price = Column(Float)
    direct_cost = Column(Float, default=0.0)
    inward_quantity = Column(Float, default=0.0)  # Inward quantity (purchased/grown)
    inward_rate = Column(Float, default=0.0)  # Inward rate per kg
    inward_value = Column(Float, default=0.0)  # Total inward value
    inhouse_production = Column(Float, default=0.0)  # Extra production (outward > inward)
    wastage = Column(Float, default=0.0)  # Wastage (inward > outward)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    product = relationship("Product", back_populates="monthly_sales")
    allocations = relationship("Allocation", back_populates="monthly_sale")

class Cost(Base):
    __tablename__ = "costs"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    amount = Column(Float)
    applies_to = Column(String)  # "inhouse", "outsourced", "both", "all"
    cost_type = Column(String)  # "purchase-only", "sales-only", "common", "inhouse-only"
    basis = Column(String)  # "weight", "value", "trips"
    month = Column(String, index=True)
    is_fixed = Column(String, default="variable")  # "fixed" or "variable"
    category = Column(String, default="general")  # "transport", "marketing", "storage", etc.
    
    # NEW: P&L classification fields
    pl_classification = Column(String, default=None)  # 'B', 'I', 'O'
    original_amount = Column(Float, default=None)     # Original P&L amount
    allocation_ratio = Column(Float, default=None)    # Ratio used for B items
    source_file = Column(String, default='manual')    # 'excel_upload' or 'manual'
    pl_period = Column(String, default=None)          # '1-Apr-24 to 30-Apr-24'
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Allocation(Base):
    __tablename__ = "allocations"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"))
    monthly_sale_id = Column(Integer, ForeignKey("monthly_sales.id"))
    cost_id = Column(Integer, ForeignKey("costs.id"))
    month = Column(String, index=True)
    allocated_amount = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    product = relationship("Product", back_populates="allocations")
    monthly_sale = relationship("MonthlySale", back_populates="allocations")
    cost = relationship("Cost")

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)

# Pydantic models
class ProductCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    source: str = Field(..., pattern="^(inhouse|outsourced)$")
    unit: str = Field(default="kg", max_length=20)
    extra_info: Optional[str] = Field(None, max_length=500)

class ProductUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    source: Optional[str] = Field(None, pattern="^(inhouse|outsourced)$")
    unit: Optional[str] = Field(None, max_length=20)
    extra_info: Optional[str] = Field(None, max_length=500)
    is_active: Optional[bool] = None

class ProductResponse(BaseModel):
    id: int
    name: str
    source: str
    unit: str
    extra_info: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

class MonthlySaleCreate(BaseModel):
    product_id: int
    month: str = Field(..., pattern="^\\d{4}-\\d{2}$")
    quantity: float = Field(..., gt=0)  # Outward quantity
    sale_price: float = Field(..., gt=0)
    direct_cost: float = Field(default=0.0, ge=0)
    inward_quantity: float = Field(default=0.0, ge=0)
    inward_rate: float = Field(default=0.0, ge=0)
    inward_value: float = Field(default=0.0, ge=0)
    inhouse_production: float = Field(default=0.0, ge=0)
    wastage: float = Field(default=0.0, ge=0)

class MonthlySaleUpdate(BaseModel):
    quantity: Optional[float] = Field(None, gt=0)
    sale_price: Optional[float] = Field(None, gt=0)
    direct_cost: Optional[float] = Field(None, ge=0)
    inward_quantity: Optional[float] = Field(None, ge=0)
    inward_rate: Optional[float] = Field(None, ge=0)
    inward_value: Optional[float] = Field(None, ge=0)
    inhouse_production: Optional[float] = Field(None, ge=0)
    wastage: Optional[float] = Field(None, ge=0)

class MonthlySaleResponse(BaseModel):
    id: int
    product_id: int
    product_name: str
    unit: str
    month: str
    quantity: float
    sale_price: float
    direct_cost: float
    inward_quantity: float
    inward_rate: float
    inward_value: float
    inhouse_production: float
    wastage: float
    created_at: datetime

class CostCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    amount: float = Field(..., gt=0)
    applies_to: str = Field(..., pattern="^(inhouse|outsourced|both|all)$")
    cost_type: str = Field(..., pattern="^(purchase-only|sales-only|common|inhouse-only)$")
    basis: str = Field(..., pattern="^(weight|value|trips|hybrid)$")
    month: str = Field(..., pattern="^\\d{4}-\\d{2}$")
    is_fixed: str = Field(default="variable", pattern="^(fixed|variable)$")
    category: str = Field(default="general", max_length=50)
    
    # NEW: P&L fields (optional)
    pl_classification: Optional[str] = Field(None, pattern="^[BIO]$")
    original_amount: Optional[float] = Field(None, ge=0)
    allocation_ratio: Optional[float] = Field(None, ge=0, le=1)
    source_file: Optional[str] = Field(default="manual", max_length=100)
    pl_period: Optional[str] = Field(None, max_length=100)

class CostUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    amount: Optional[float] = Field(None, gt=0)
    applies_to: Optional[str] = Field(None, pattern="^(inhouse|outsourced|both|all)$")
    cost_type: Optional[str] = Field(None, pattern="^(purchase-only|sales-only|common|inhouse-only)$")
    basis: Optional[str] = Field(None, pattern="^(weight|value|trips|hybrid)$")
    is_fixed: Optional[str] = Field(None, pattern="^(fixed|variable)$")
    category: Optional[str] = Field(None, max_length=50)

class CostResponse(BaseModel):
    id: int
    name: str
    amount: float
    applies_to: str
    cost_type: str
    basis: str
    month: str
    is_fixed: str
    category: str
    
    # NEW: P&L fields
    pl_classification: Optional[str] = None
    original_amount: Optional[float] = None
    allocation_ratio: Optional[float] = None
    source_file: Optional[str] = None
    pl_period: Optional[str] = None
    
    created_at: datetime

class AllocationResponse(BaseModel):
    id: int
    product_id: int
    product_name: str
    month: str
    allocated_amount: float
    cost_name: str
    cost_category: str
    created_at: datetime

class DashboardStats(BaseModel):
    total_products: int
    active_products: int
    total_revenue: float
    total_costs: float
    total_profit: float
    profit_margin: float
    inhouse_revenue: float
    outsourced_revenue: float
    inhouse_profit: float
    outsourced_profit: float

class MonthlyReport(BaseModel):
    month: str
    products: List[Dict[str, Any]]
    total_revenue: float
    total_costs: float
    total_profit: float
    profit_margin: float
    inhouse_summary: Dict[str, float]
    outsourced_summary: Dict[str, float]
    cost_breakdown: Dict[str, float]
    top_products: List[Dict[str, Any]]

# Excel Upload Models
class ExcelRowData(BaseModel):
    month: str
    particulars: str
    type: str  # "In-house" or "Outsourced"
    inward_quantity: float
    inward_rate: float
    inward_value: float
    outward_quantity: float
    outward_rate: float
    outward_value: float
    inhouse_production: float = 0.0
    wastage: float = 0.0

class ExcelUploadResponse(BaseModel):
    success: bool
    message: str
    parsed_data: List[ExcelRowData]
    errors: List[str] = []
    products_created: int = 0
    sales_created: int = 0

class ExcelPreviewData(BaseModel):
    products: List[Dict[str, Any]]
    sales: List[Dict[str, Any]]
    summary: Dict[str, Any]

# FastAPI app
app = FastAPI(
    title="🍇 Fruit & Vegetable Cost Allocation System",
    description="A comprehensive system for calculating costs and profits for fruit and vegetable businesses",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Enhanced Cost Allocation Engine
class CostAllocationEngine:
    def __init__(self, db: Session):
        self.db = db
        # Settings: Use REAL values from P&L - no artificial damping
        # High-volume products will get more costs because they actually consume more resources
        # This shows TRUE profitability based on actual resource consumption
        # FAIR ALLOCATION: Overhead allocated by weight + revenue (NOT purchase cost)
        # Purchase cost is already in direct_cost - don't penalize outsourced twice
        self.B_HYBRID_ALPHA = 0.6  # 60% weight (resource consumption), 40% revenue (business contribution)
        self.DAMP_WEIGHT_FOR_B = False  # NO damping - use actual weight to reflect real resource consumption
        self.DAMP_VALUE_FOR_B = False  # NO damping - use actual revenue
        self.OVERHEAD_CAP_FACTOR = None  # No cap - let real costs flow through
    
    def allocate_costs_for_month(self, month: str) -> Dict[str, Any]:
        """Enhanced allocation function - works with all data regardless of month"""
        
        try:
            # Get all active products (ignore month)
            products = self.db.query(Product).filter(Product.is_active == True).all()
            product_map = {p.id: p for p in products}
            
            # Get all monthly sales (ignore month)
            monthly_sales = self.db.query(MonthlySale).all()
            sales_map = {s.product_id: s for s in monthly_sales}
            
            # Get all costs (ignore month)
            costs = self.db.query(Cost).all()
            
            if not costs:
                raise HTTPException(
                    status_code=400, 
                    detail="No costs found. Please add costs before running allocation."
                )
            
            if not monthly_sales:
                raise HTTPException(
                    status_code=400, 
                    detail="No sales data found. Please add sales data before running allocation."
                )
            
            # Clear existing allocations (ignore month)
            self.db.query(Allocation).delete()
            
            # No overhead cap - let real P&L costs flow through to show true profitability
            allocated_so_far: Dict[int, float] = {pid: 0.0 for pid in product_map.keys()}
            cap_by_product: Dict[int, float] = {}
            # No cap applied - removed artificial limit to show real cost allocation

            # Process each cost
            for cost in costs:
                self._allocate_single_cost(cost, product_map, sales_map, month, allocated_so_far, cap_by_product)
            
            self.db.commit()
            
            # Generate comprehensive report
            return self._generate_monthly_report(month, product_map, sales_map)
            
        except Exception as e:
            self.db.rollback()
            raise HTTPException(status_code=500, detail=f"Allocation failed: {str(e)}")
    
    def _allocate_single_cost(self, cost: Cost, product_map: Dict, sales_map: Dict, month: str, allocated_so_far: Dict[int, float], cap_by_product: Dict[int, float]):
        """Allocate a single cost to applicable products
        - INHOUSE: Normalized allocation (percentages) - balances weight and profit contribution
        - OUTSOURCED: Absolute gross profit allocation - protects low-margin products
        """
        
        # Step 1: Determine which products are affected
        applicable_products = self._get_applicable_products(cost, product_map, sales_map)
        
        if not applicable_products:
            return
        
        # Step 2: Compute total basis and allocate
        # For hybrid basis: uses pure gross profit (no weight) - same for all products
        # For other bases: standard allocation
        total_basis = self._compute_total_basis(cost, applicable_products, sales_map)
        
        if total_basis == 0:
            return
        
        # Allocate cost proportionally based on basis
        for product_id, product in applicable_products.items():
            if product_id not in sales_map:
                continue
                
            sale = sales_map[product_id]
            product_basis = self._compute_product_basis(cost, sale)
            
            if product_basis > 0:
                allocated_amount = (product_basis / total_basis) * cost.amount

                # Store allocation if amount is positive
                if allocated_amount > 0:
                    allocation = Allocation(
                        product_id=product_id,
                        monthly_sale_id=sale.id,
                        cost_id=cost.id,
                        month=month,
                        allocated_amount=allocated_amount
                    )
                    self.db.add(allocation)
                    allocated_so_far[product_id] = allocated_so_far.get(product_id, 0.0) + allocated_amount
    
    def _get_applicable_products(self, cost: Cost, product_map: Dict, sales_map: Dict) -> Dict:
        """Get products that this cost applies to"""
        applicable = {}
        
        for product_id, product in product_map.items():
            if product_id not in sales_map:
                continue
                
            if cost.applies_to == "all":
                applicable[product_id] = product
            elif cost.applies_to == "inhouse" and product.source == "inhouse":
                applicable[product_id] = product
            elif cost.applies_to == "outsourced" and product.source == "outsourced":
                applicable[product_id] = product
            elif cost.applies_to == "both" and product.source in ["inhouse", "outsourced"]:
                applicable[product_id] = product
        
        return applicable
    
    def _compute_total_basis(self, cost: Cost, applicable_products: Dict, sales_map: Dict) -> float:
        """Compute total basis for allocation"""
        total = 0.0
        
        for product_id in applicable_products:
            if product_id in sales_map:
                sale = sales_map[product_id]
                total += self._compute_product_basis(cost, sale)
        
        return total
    
    def _compute_product_basis(self, cost: Cost, sale: MonthlySale) -> float:
        """Compute basis for a single product"""
        # Get product to access unit information
        product = sale.product
        pname = (product.name or "").lower()
        unit_upper = (product.unit or "").upper() if hasattr(product, 'unit') and product.unit else ""
        is_ea = unit_upper in ['EA', 'EACH', 'PC', 'PCS', 'UNIT', 'UNITS']
        is_hamper = "hamper" in pname

        # Special handling for hampers:
        # Hampers are assembled products, not directly cultivated, so:
        # - EXCLUDED from I costs (Cultivation, Wastage-in Farm) - return 0 (no allocation)
        # - Use REVENUE-only for all other costs (B costs, O costs)
        if is_hamper:
            # Check if this is an inhouse-specific cost (I classification)
            is_inhouse_cost = cost.pl_classification == "I" if hasattr(cost, 'pl_classification') and cost.pl_classification else False
            if is_inhouse_cost:
                # Hampers don't consume cultivation/wastage costs - they're assembled from already-produced items
                return 0.0
            # For all other costs, use revenue-based allocation
            return sale.quantity * sale.sale_price
        
        if cost.basis == "weight":
            # Use REAL weight - no damping. High-volume products consume more resources and should pay proportionally
            if hasattr(product, 'unit') and product.unit and product.unit.upper() in ['EA', 'EACH', 'PC', 'PCS', 'UNIT', 'UNITS']:
                # Use the shared _to_kg helper to convert EA to actual kg
                qty_kg = _to_kg(product.name, sale.quantity, product.unit)
                if qty_kg > 0:
                    return qty_kg  # Use actual weight - no artificial reduction
                # No conversion factor (value-only items like hampers), use revenue
                return sale.quantity * sale.sale_price
            return sale.quantity  # Use actual weight - no damping
        elif cost.basis == "value":
            # Use REVENUE (not purchase cost) for fair allocation
            # Purchase cost is already accounted for in direct_cost - don't penalize twice
            # Revenue reflects business contribution and is fair to both inhouse and outsourced
            return sale.quantity * sale.sale_price
        elif cost.basis == "trips":
            # For trips, use value-based to avoid unit issues
            if hasattr(product, 'unit') and product.unit and product.unit.upper() in ['EA', 'EACH', 'PC', 'PCS', 'UNIT', 'UNITS']:
                return sale.quantity * sale.sale_price
            return sale.quantity
        elif cost.basis == "hybrid":
            # NEW LOGIC: Different allocation for inhouse-specific costs (I items)
            # Get product source
            product_source = product.source if hasattr(product, 'source') else None
            is_inhouse = product_source == "inhouse"
            
            # Check if this is an inhouse-specific cost (I classification like Cultivation, Wastage)
            is_inhouse_cost = cost.pl_classification == "I" if hasattr(cost, 'pl_classification') and cost.pl_classification else False
            
            # For inhouse products with inhouse-specific costs (I items):
            # Allocate by WEIGHT ONLY - these are direct production costs proportional to quantity produced
            # Examples: Cultivation Expenses I, Wastage-in Farm (Quality Check) I, Rejection Own Farm Harvest I
            # These costs scale directly with production volume, not profitability
            # NOTE: For graded products (A/B/C) from same harvest, this ensures fair per-kg allocation
            if is_inhouse and is_inhouse_cost:
                qty_kg = _to_kg(product.name, sale.quantity, product.unit) if hasattr(product, 'unit') else sale.quantity
                if qty_kg > 0:
                    return qty_kg  # Pure weight-based allocation for cultivation/wastage costs
                # Fallback to revenue if no weight conversion
                return sale.quantity * sale.sale_price
            
            # For all other products (outsourced products, or inhouse products with B costs):
            # Use standard hybrid: 20% weight + 80% gross profit
            # This balances resource consumption with profitability for shared overhead costs
            # Weight part (20%): Use ACTUAL weight in kg (with EA→kg conversion where applicable)
            qty_kg = _to_kg(product.name, sale.quantity, product.unit) if hasattr(product, 'unit') else sale.quantity
            weight_part = qty_kg
            
            # Gross Profit part (80%): Revenue - Direct Cost
            revenue = sale.quantity * sale.sale_price
            direct_cost = sale.direct_cost or 0.0
            gross_profit = max(0.0, revenue - direct_cost)  # Ensure non-negative
            
            # IMPORTANT: For inhouse products, if direct_cost is 0, gross_profit = revenue
            # This means high-revenue products (like A Grade) get 80% of allocation based on revenue
            # Lower-revenue products (like C Grade) get penalized even if they're same product family
            # Current logic: 20% weight + 80% revenue (since direct_cost = 0 for inhouse)
            
            # Combined basis: 20% weight + 80% gross profit
            # Weight and profit are on different scales, but this creates fair balance
            # High-profit products still get most allocation (80%), but weight matters (20%)
            return 0.20 * weight_part + 0.80 * gross_profit
        return 0.0
    
    def _generate_monthly_report(self, month: str, product_map: Dict, sales_map: Dict) -> Dict[str, Any]:
        """Generate comprehensive report with enhanced analytics (ignores month)"""
        
        # Get all allocations (ignore month)
        allocations = self.db.query(Allocation).all()
        
        # Group allocations by product
        product_allocations = {}
        for allocation in allocations:
            if allocation.product_id not in product_allocations:
                product_allocations[allocation.product_id] = []
            product_allocations[allocation.product_id].append(allocation)
        
        # Calculate per-product costs and profits
        products_data = []
        total_revenue = 0.0
        total_costs = 0.0
        
        inhouse_revenue = 0.0
        inhouse_costs = 0.0
        outsourced_revenue = 0.0
        outsourced_costs = 0.0
        
        cost_breakdown = {}
        
        for product_id, sale in sales_map.items():
            product = product_map[product_id]
            allocated_costs = product_allocations.get(product_id, [])
            
            total_allocated = sum(a.allocated_amount for a in allocated_costs)
            total_cost = sale.direct_cost + total_allocated
            revenue = sale.quantity * sale.sale_price
            profit = revenue - total_cost
            cost_per_kg = total_cost / sale.quantity if sale.quantity > 0 else 0
            profit_margin = (profit / revenue * 100) if revenue > 0 else 0
            
            # Cost breakdown by category
            for allocation in allocated_costs:
                category = allocation.cost.category
                if category not in cost_breakdown:
                    cost_breakdown[category] = 0.0
                cost_breakdown[category] += allocation.allocated_amount
            
            product_data = {
                "product_id": product_id,
                "product_name": product.name,
                "source": product.source,
                "unit": getattr(product, 'unit', 'kg'),
                "quantity": sale.quantity,
                "sale_price": sale.sale_price,
                "direct_cost": sale.direct_cost,
                "allocated_costs": total_allocated,
                "total_cost": total_cost,
                "revenue": revenue,
                "profit": profit,
                "cost_per_kg": cost_per_kg,
                "profit_margin": profit_margin,
                "allocations": [
                    {
                        "cost_name": a.cost.name,
                        "category": a.cost.category,
                        "amount": a.allocated_amount
                    } for a in allocated_costs
                ]
            }
            
            products_data.append(product_data)
            total_revenue += revenue
            total_costs += total_cost
            
            if product.source == "inhouse":
                inhouse_revenue += revenue
                inhouse_costs += total_cost
            else:
                outsourced_revenue += revenue
                outsourced_costs += total_cost
        
        # Sort products by profit (DSA optimization)
        products_data.sort(key=lambda x: x["profit"], reverse=True)
        
        # Calculate top products
        top_products = products_data[:5]  # Top 5 by profit
        
        return {
            "month": month,
            "products": products_data,
            "total_revenue": total_revenue,
            "total_costs": total_costs,
            "total_profit": total_revenue - total_costs,
            "profit_margin": ((total_revenue - total_costs) / total_revenue * 100) if total_revenue > 0 else 0,
            "inhouse_summary": {
                "revenue": inhouse_revenue,
                "costs": inhouse_costs,
                "profit": inhouse_revenue - inhouse_costs,
                "profit_margin": ((inhouse_revenue - inhouse_costs) / inhouse_revenue * 100) if inhouse_revenue > 0 else 0
            },
            "outsourced_summary": {
                "revenue": outsourced_revenue,
                "costs": outsourced_costs,
                "profit": outsourced_revenue - outsourced_costs,
                "profit_margin": ((outsourced_revenue - outsourced_costs) / outsourced_revenue * 100) if outsourced_revenue > 0 else 0
            },
            "cost_breakdown": cost_breakdown,
            "top_products": top_products
        }

# API Endpoints
@app.get("/")
async def root():
    return FileResponse("index.html")

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow()}

@app.get("/api")
@app.get("/api/info")
async def api_info():
    """Return API information and version"""
    return {
        "message": "Purple Patch Farms ERP - Hybrid Agribusiness Management System",
        "version": "2.0.0",
        "description": "A comprehensive cost allocation system for fruit and vegetable farming operations",
        "features": [
            "Product Management",
            "Sales Tracking",
            "Cost Management & Allocation",
            "Excel Upload (Standard & Auto-Detection Mode)",
            "P&L Upload",
            "Reports & Analytics",
            "Profitability Analysis"
        ]
    }

@app.post("/api/reset-database")
async def reset_database(db: Session = Depends(get_db)):
    """Reset the entire database by deleting all records"""
    try:
        print("🗑️  Starting database reset...")
        
        # Delete all records from all tables (in correct order due to foreign keys)
        allocations_count = db.query(Allocation).count()
        sales_count = db.query(MonthlySale).count()
        costs_count = db.query(Cost).count()
        products_count = db.query(Product).count()
        
        print(f"   📊 Records before reset: {allocations_count} allocations, {sales_count} sales, {costs_count} costs, {products_count} products")
        
        # Delete in order: allocations first (has foreign keys), then sales, then costs, then products
        db.query(Allocation).delete()
        db.query(MonthlySale).delete()
        db.query(Cost).delete()
        db.query(Product).delete()
        
        # Commit the changes
        db.commit()
        
        # Verify deletion
        remaining_allocations = db.query(Allocation).count()
        remaining_sales = db.query(MonthlySale).count()
        remaining_costs = db.query(Cost).count()
        remaining_products = db.query(Product).count()
        
        print(f"   ✅ Records after reset: {remaining_allocations} allocations, {remaining_sales} sales, {remaining_costs} costs, {remaining_products} products")
        
        if remaining_allocations > 0 or remaining_sales > 0 or remaining_costs > 0 or remaining_products > 0:
            print(f"   ⚠️  WARNING: Some records still exist after reset!")
            return {
                "message": f"Database reset completed with warnings. Remaining: {remaining_costs} costs, {remaining_sales} sales, {remaining_products} products",
                "timestamp": datetime.utcnow(),
                "remaining": {
                    "allocations": remaining_allocations,
                    "sales": remaining_sales,
                    "costs": remaining_costs,
                    "products": remaining_products
                }
            }
        
        return {
            "message": "Database reset successfully - all records deleted",
            "timestamp": datetime.utcnow(),
            "deleted": {
                "allocations": allocations_count,
                "sales": sales_count,
                "costs": costs_count,
                "products": products_count
            }
        }
    except Exception as e:
        db.rollback()
        print(f"   ❌ Error resetting database: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error resetting database: {str(e)}")

# Dashboard endpoints
@app.get("/api/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats(db: Session = Depends(get_db)):
    """Get overall dashboard statistics"""
    
    # Product stats
    total_products = db.query(Product).count()
    active_products = db.query(Product).filter(Product.is_active == True).count()
    
    # Revenue and cost stats for ALL data (no month filtering)
    sales = db.query(MonthlySale).all()
    all_costs = db.query(Cost).all()
    
    # Check if allocation has been run (if there are any Allocation records)
    allocations_exist = db.query(Allocation).count() > 0
    
    total_revenue = sum(s.quantity * s.sale_price for s in sales)
    total_direct_costs = sum(s.direct_cost for s in sales)
    
    if allocations_exist:
        # If allocation has been run, only count allocated costs
        # Get all allocations and sum their amounts (only valid ones with existing sales)
        allocations = db.query(Allocation).join(MonthlySale).all()  # Only get allocations with valid sales
        total_allocated_costs = sum(a.allocated_amount for a in allocations)
        total_shared_costs = total_allocated_costs
        print(f"📊 Dashboard: Using allocated costs (₹{total_allocated_costs:,.2f}) from {len(allocations)} valid allocations")
    else:
        # If allocation hasn't been run, don't include unallocated costs in total_costs
        total_unallocated_costs = sum(c.amount for c in all_costs)
        total_shared_costs = 0.0  # Don't count unallocated costs as "total costs"
        if total_unallocated_costs > 0:
            print(f"📊 Dashboard: Allocation not run yet. Unallocated costs in system: ₹{total_unallocated_costs:,.2f} (not included in totals)")
        else:
            print(f"📊 Dashboard: No costs in system")
    
    total_costs = total_direct_costs + total_shared_costs
    total_profit = total_revenue - total_costs
    profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    # Source-wise breakdown
    inhouse_sales = [s for s in sales if s.product.source == "inhouse"]
    outsourced_sales = [s for s in sales if s.product.source == "outsourced"]
    
    inhouse_revenue = sum(s.quantity * s.sale_price for s in inhouse_sales)
    outsourced_revenue = sum(s.quantity * s.sale_price for s in outsourced_sales)
    
    inhouse_direct_costs = sum(s.direct_cost for s in inhouse_sales)
    outsourced_direct_costs = sum(s.direct_cost for s in outsourced_sales)
    
    if allocations_exist:
        # Use actual allocated amounts by source (only valid allocations)
        try:
            inhouse_allocations = db.query(Allocation).join(MonthlySale).join(Product).filter(Product.source == "inhouse").all()
            outsourced_allocations = db.query(Allocation).join(MonthlySale).join(Product).filter(Product.source == "outsourced").all()
            inhouse_shared_costs = sum(a.allocated_amount for a in inhouse_allocations)
            outsourced_shared_costs = sum(a.allocated_amount for a in outsourced_allocations)
        except Exception as e:
            # If there are orphaned allocations (sales/products deleted), ignore them
            print(f"⚠️  Warning: Some orphaned allocations found, ignoring: {str(e)}")
            inhouse_shared_costs = 0.0
            outsourced_shared_costs = 0.0
    else:
        # Simple allocation for dashboard preview (50-50 split for shared costs)
        # But since total_shared_costs is 0, these will be 0
        inhouse_shared_costs = 0.0
        outsourced_shared_costs = 0.0
    
    inhouse_costs = inhouse_direct_costs + inhouse_shared_costs
    outsourced_costs = outsourced_direct_costs + outsourced_shared_costs
    
    inhouse_profit = inhouse_revenue - inhouse_costs
    outsourced_profit = outsourced_revenue - outsourced_costs
    
    return DashboardStats(
        total_products=total_products,
        active_products=active_products,
        total_revenue=total_revenue,
        total_costs=total_costs,
        total_profit=total_profit,
        profit_margin=profit_margin,
        inhouse_revenue=inhouse_revenue,
        outsourced_revenue=outsourced_revenue,
        inhouse_profit=inhouse_profit,
        outsourced_profit=outsourced_profit
    )

# Product endpoints
@app.post("/api/products/", response_model=ProductResponse)
async def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    # Check if product already exists
    existing = db.query(Product).filter(Product.name == product.name).first()
    if existing:
        raise HTTPException(
            status_code=400, 
            detail=f"Product '{product.name}' already exists"
        )
    
    db_product = Product(**product.model_dump())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

@app.get("/api/products/", response_model=List[ProductResponse])
async def get_products(active_only: bool = True, db: Session = Depends(get_db)):
    query = db.query(Product)
    if active_only:
        query = query.filter(Product.is_active == True)
    return query.order_by(Product.name).all()

@app.get("/api/products/{product_id}", response_model=ProductResponse)
async def get_product(product_id: int, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product

@app.put("/api/products/{product_id}", response_model=ProductResponse)
async def update_product(product_id: int, product_update: ProductUpdate, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    update_data = product_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(product, field, value)
    
    product.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(product)
    return product

@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Soft delete
    product.is_active = False
    product.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Product deactivated successfully"}

# Monthly Sales endpoints
@app.post("/api/monthly-sales/", response_model=MonthlySaleResponse)
async def create_monthly_sale(sale: MonthlySaleCreate, db: Session = Depends(get_db)):
    # Verify product exists
    product = db.query(Product).filter(Product.id == sale.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Check if sale already exists for this product and month
    existing = db.query(MonthlySale).filter(
        MonthlySale.product_id == sale.product_id,
        MonthlySale.month == sale.month
    ).first()
    if existing:
        raise HTTPException(
            status_code=400, 
            detail=f"Sales data already exists for {product.name} in {sale.month}"
        )
    
    db_sale = MonthlySale(**sale.model_dump())
    db.add(db_sale)
    db.commit()
    db.refresh(db_sale)
    
    # Add product name and unit to response
    sale_response = MonthlySaleResponse(
        **db_sale.__dict__,
        product_name=product.name,
        unit=getattr(product, 'unit', 'kg')  # Get unit from product, default to 'kg'
    )
    return sale_response


@app.get("/api/sales", response_model=List[MonthlySaleResponse])
async def get_all_sales(db: Session = Depends(get_db)):
    """
    Get all sales data - no month filtering
    OPTIMIZED: Fixed N+1 query problem - single query with join
    Time Complexity: O(n) instead of O(n*m) where n=sales, m=products
    """
    # OPTIMIZED: Single query with join to avoid N+1 problem
    sales = db.query(MonthlySale).join(Product).all()
    
    # Build product map for O(1) lookup
    product_map = {s.product_id: s.product for s in sales}
    
    # Single-pass iteration: O(n)
    sales_with_names = []
    for sale in sales:
        product = product_map.get(sale.product_id)
        sales_with_names.append(MonthlySaleResponse(
            **sale.__dict__,
            product_name=product.name if product else "Unknown",
            unit=product.unit if product and getattr(product, 'unit', None) else 'kg'
        ))
    
    return sales_with_names

@app.get("/api/monthly-sales/{param}", response_model=Union[MonthlySaleResponse, List[MonthlySaleResponse]])
async def get_monthly_sales_or_by_id(param: str, db: Session = Depends(get_db)):
    """
    Get sales by ID or month
    OPTIMIZED: Fixed N+1 query problem - use joins instead of separate queries
    Time Complexity: O(1) for ID lookup, O(n) for month (n=sales in month)
    """
    print(f"DEBUG: Received param: '{param}'")
    
    # Check if param is a number (ID) or string (month)
    if param.isdigit():
        print(f"DEBUG: Treating '{param}' as ID")
        # OPTIMIZED: Single query with join
        sale = db.query(MonthlySale).join(Product).filter(MonthlySale.id == int(param)).first()
        if not sale:
            print(f"DEBUG: Sale not found for ID {param}")
            raise HTTPException(status_code=404, detail="Sales record not found")
        
        sale_response = MonthlySaleResponse(
            **sale.__dict__,
            product_name=sale.product.name if sale.product else "Unknown",
            unit=sale.product.unit if sale.product and getattr(sale.product, 'unit', None) else 'kg'
        )
        print(f"DEBUG: Returning single sale: {sale_response}")
        return sale_response
    else:
        print(f"DEBUG: Treating '{param}' as month")
        # OPTIMIZED: Single query with join to avoid N+1
        sales = db.query(MonthlySale).join(Product).filter(MonthlySale.month == param).all()
        print(f"DEBUG: Found {len(sales)} sales for month {param}")
        
        # Single-pass iteration: O(n)
        sales_with_names = []
        for sale in sales:
            sales_with_names.append(MonthlySaleResponse(
                **sale.__dict__,
                product_name=sale.product.name if sale.product else "Unknown",
                unit=sale.product.unit if sale.product and getattr(sale.product, 'unit', None) else 'kg'
            ))
        
        return sales_with_names

@app.get("/api/sales/{sale_id}", response_model=MonthlySaleResponse)
async def get_sale_by_id(sale_id: int, db: Session = Depends(get_db)):
    print(f"DEBUG: Getting sale with ID {sale_id}")
    sale = db.query(MonthlySale).filter(MonthlySale.id == sale_id).first()
    if not sale:
        print(f"DEBUG: Sale not found for ID {sale_id}")
        raise HTTPException(status_code=404, detail="Sales record not found")
    
    # Add product name and unit to response
    product = db.query(Product).filter(Product.id == sale.product_id).first()
    sale_response = MonthlySaleResponse(
        **sale.__dict__,
        product_name=product.name if product else "Unknown",
        unit=product.unit if product and getattr(product, 'unit', None) else 'kg'
    )
    print(f"DEBUG: Returning sale: {sale_response}")
    return sale_response

@app.put("/api/monthly-sales/{sale_id}", response_model=MonthlySaleResponse)
async def update_monthly_sale(sale_id: int, sale_update: MonthlySaleUpdate, db: Session = Depends(get_db)):
    sale = db.query(MonthlySale).filter(MonthlySale.id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="Sales record not found")
    
    update_data = sale_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(sale, field, value)
    
    sale.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(sale)
    
    # Add product name to response
    product = db.query(Product).filter(Product.id == sale.product_id).first()
    return MonthlySaleResponse(
        **sale.__dict__,
        product_name=product.name if product else "Unknown",
        unit=product.unit if product and getattr(product, 'unit', None) else 'kg'
    )

# Cost endpoints
@app.post("/api/costs/", response_model=CostResponse)
async def create_cost(cost: CostCreate, db: Session = Depends(get_db)):
    db_cost = Cost(**cost.model_dump())
    db.add(db_cost)
    db.commit()
    db.refresh(db_cost)
    return db_cost

@app.get("/api/costs", response_model=List[CostResponse])
async def get_all_costs(db: Session = Depends(get_db)):
    """Get all costs data - no month filtering"""
    return db.query(Cost).order_by(Cost.created_at.desc()).all()

@app.get("/api/costs/{month}", response_model=List[CostResponse])
async def get_costs(month: str, db: Session = Depends(get_db)):
    return db.query(Cost).filter(Cost.month == month).order_by(Cost.created_at.desc()).all()

@app.get("/api/costs/id/{cost_id}", response_model=CostResponse)
async def get_cost_by_id(cost_id: int, db: Session = Depends(get_db)):
    cost = db.query(Cost).filter(Cost.id == cost_id).first()
    if not cost:
        raise HTTPException(status_code=404, detail="Cost not found")
    return cost

@app.put("/api/costs/{cost_id}", response_model=CostResponse)
async def update_cost(cost_id: int, cost_update: CostUpdate, db: Session = Depends(get_db)):
    cost = db.query(Cost).filter(Cost.id == cost_id).first()
    if not cost:
        raise HTTPException(status_code=404, detail="Cost not found")
    
    update_data = cost_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(cost, field, value)
    
    cost.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(cost)
    return cost

@app.delete("/api/costs/{cost_id}")
async def delete_cost(cost_id: int, db: Session = Depends(get_db)):
    cost = db.query(Cost).filter(Cost.id == cost_id).first()
    if not cost:
        raise HTTPException(status_code=404, detail="Cost not found")
    
    db.delete(cost)
    db.commit()
    return {"message": "Cost deleted successfully"}

# Allocation and Reports
@app.post("/api/allocate/{month}")
async def allocate_costs(month: str, db: Session = Depends(get_db)):
    engine = CostAllocationEngine(db)
    result = engine.allocate_costs_for_month(month)
    return result

@app.get("/api/report/{month}")
async def get_monthly_report(month: str, db: Session = Depends(get_db)):
    engine = CostAllocationEngine(db)
    # Build maps from DB so report has data
    products = db.query(Product).filter(Product.is_active == True).all()
    product_map = {p.id: p for p in products}
    monthly_sales = db.query(MonthlySale).all()
    sales_map = {s.product_id: s for s in monthly_sales}
    return engine._generate_monthly_report(month, product_map, sales_map)

# Export endpoints
@app.get("/api/export/{month}/csv")
async def export_monthly_csv(month: str, db: Session = Depends(get_db)):
    """Export monthly report as CSV"""
    engine = CostAllocationEngine(db)
    # Build maps from DB so report has data
    products = db.query(Product).filter(Product.is_active == True).all()
    product_map = {p.id: p for p in products}
    monthly_sales = db.query(MonthlySale).all()
    sales_map = {s.product_id: s for s in monthly_sales}
    report = engine._generate_monthly_report(month, product_map, sales_map)
    
    # Create DataFrame
    df = pd.DataFrame(report['products'])
    
    # Save to CSV
    csv_path = f"static/exports/report_{month}.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    
    return {"download_url": f"/static/exports/report_{month}.csv"}

@app.get("/api/export/{month}/xlsx")
async def export_monthly_xlsx(month: str, db: Session = Depends(get_db)):
    """Export monthly report as Excel with multiple sheets"""
    engine = CostAllocationEngine(db)
    # Build maps from DB so report has data
    products = db.query(Product).filter(Product.is_active == True).all()
    product_map = {p.id: p for p in products}
    monthly_sales = db.query(MonthlySale).all()
    sales_map = {s.product_id: s for s in monthly_sales}
    report = engine._generate_monthly_report(month, product_map, sales_map)
    
    # Build DataFrames
    products_df = pd.DataFrame(report['products'])
    
    # Build formatted Product-wise Allocation Results as requested
    formatted_rows = []
    # Sort by profit desc to match expected view
    products_sorted = sorted(report['products'], key=lambda x: x.get('profit', 0), reverse=True)
    for p in products_sorted:
        # Build friendly quantity string with EA/grams handling
        pname = (p.get('product_name') or '').lower()
        unit = (p.get('unit') or 'kg')
        qty = p.get('quantity', 0)
        ea_units = ['EA','EACH','PC','PCS','UNIT','UNITS']
        if unit.upper() in ea_units:
            # Special cases: hampers → show EA only; mushroom/corn → show EA with grams and kg
            if 'hamper' in pname:
                qty_str = f"{qty} EA"
            elif ('button mushroom' in pname) or ('baby corn' in pname):
                grams_per_ea = 200.0
                kg_equiv = (qty * grams_per_ea) / 1000.0
                qty_str = f"{qty} EA (200 g ea, {kg_equiv:.2f} kg)"
            else:
                qty_str = f"{qty} EA"
        else:
            qty_str = f"{qty} {unit}"
        price_str = f"₹{p['sale_price']:,.2f}"
        direct_cost_str = f"₹{p['direct_cost']:,.2f}"
        allocated_str = f"₹{p['allocated_costs']:,.2f}"
        total_cost_str = f"₹{p['total_cost']:,.2f}"
        revenue_str = f"₹{p['revenue']:,.2f}"
        profit_str = f"₹{p['profit']:,.2f}"
        margin_str = f"{p['profit_margin']:.1f}%"
        formatted_rows.append({
            'Product': p['product_name'],
            'Source': p['source'],
            'Qty': qty_str,
            'Price': price_str,
            'Direct Cost': direct_cost_str,
            'Allocated': allocated_str,
            'Total Cost': total_cost_str,
            'Revenue': revenue_str,
            'Profit': profit_str,
            'Margin': margin_str,
        })
    products_formatted_df = pd.DataFrame(formatted_rows)
    
    # Flatten allocations into a table
    allocations_rows = []
    for p in report['products']:
        for a in p.get('allocations', []):
            allocations_rows.append({
                'product_id': p['product_id'],
                'product_name': p['product_name'],
                'source': p['source'],
                'cost_name': a['cost_name'],
                'category': a['category'],
                'allocated_amount': a['amount']
            })
    allocations_df = pd.DataFrame(allocations_rows) if allocations_rows else pd.DataFrame(columns=['product_id','product_name','source','cost_name','category','allocated_amount'])
    
    # Summary sheet
    summary_rows = [
        {'metric': 'total_revenue', 'value': report['total_revenue']},
        {'metric': 'total_costs', 'value': report['total_costs']},
        {'metric': 'total_profit', 'value': report['total_revenue'] - report['total_costs']},
        {'metric': 'profit_margin_%', 'value': report['profit_margin']},
        {'metric': 'inhouse_revenue', 'value': report['inhouse_summary']['revenue']},
        {'metric': 'inhouse_costs', 'value': report['inhouse_summary']['costs']},
        {'metric': 'inhouse_profit', 'value': report['inhouse_summary']['profit']},
        {'metric': 'inhouse_profit_margin_%', 'value': report['inhouse_summary']['profit_margin']},
        {'metric': 'outsourced_revenue', 'value': report['outsourced_summary']['revenue']},
        {'metric': 'outsourced_costs', 'value': report['outsourced_summary']['costs']},
        {'metric': 'outsourced_profit', 'value': report['outsourced_summary']['profit']},
        {'metric': 'outsourced_profit_margin_%', 'value': report['outsourced_summary']['profit_margin']},
    ]
    # Add cost breakdown rows
    for category, amount in report.get('cost_breakdown', {}).items():
        summary_rows.append({'metric': f'cost_{category}', 'value': amount})
    summary_df = pd.DataFrame(summary_rows)
    
    # Save to Excel
    xlsx_path = f"static/exports/report_{month}.xlsx"
    os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)
    with pd.ExcelWriter(xlsx_path, engine='xlsxwriter') as writer:
        summary_df.to_excel(writer, index=False, sheet_name='Summary')
        products_df.to_excel(writer, index=False, sheet_name='Products (Raw)')
        # Sheet name exactly as requested
        products_formatted_df.to_excel(writer, index=False, sheet_name='Product-wise Allocation Results')
        allocations_df.to_excel(writer, index=False, sheet_name='Allocations')
    
    return {"download_url": f"/static/exports/report_{month}.xlsx"}

# Excel Upload endpoints
@app.post("/api/upload-excel")
async def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    BULLETPROOF Excel upload - handles all edge cases and data formats.
    Now with Auto-Detection Mode for Purple Patch Farms format.
    """
    
    print(f"🚀 BULLETPROOF Excel upload starting for: {file.filename}")
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return {
            "success": False,
            "message": "File must be an Excel file (.xlsx or .xls)",
            "products_created": 0,
            "sales_created": 0,
            "parsed_data": [],
            "errors": ["Invalid file type"]
        }
    
    try:
        # Read Excel file
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        print(f"📋 Excel columns: {list(df.columns)}")
        print(f"📊 Total rows: {len(df)}")
        print(f"📋 Sample data:")
        print(df.head(2).to_string())
        
        # ============================================
        # AUTO-DETECTION: Check if Purple Patch format
        # ============================================
        if detect_purple_patch_format(df):
            print("🔄 Switching to Auto Mode for Purple Patch format...")
            try:
                # OPTIMIZED: Extract month from filename or file content
                # Time Complexity: O(1) for filename, O(n) for file content (n=rows scanned, early exit)
                month = "2025-04"  # Default
                # OPTIMIZED: Use pre-compiled regex pattern
                filename_month = REGEX_PATTERNS['month_yyyy_mm'].search(file.filename)
                if filename_month:
                    month_str = filename_month.group(0)
                    # Convert to YYYY-MM format if needed
                    if len(month_str) == 7 and '-' in month_str:
                        month = month_str
                    else:
                        # Try to parse other formats
                        try:
                            from datetime import datetime
                            parsed = datetime.strptime(month_str, "%b-%Y")
                            month = parsed.strftime("%Y-%m")
                        except:
                            pass
                
                # OPTIMIZED: Scan only first 50 rows instead of converting entire DataFrame
                # Early exit on first match - O(n) worst case, typically O(1)
                if month == "2025-04":  # Only scan if not found in filename
                    max_scan_rows = min(50, len(df))
                    for idx in range(max_scan_rows):
                        row = df.iloc[idx]
                        row_str = ' '.join(str(cell).upper() for cell in row if pd.notna(cell))
                        # OPTIMIZED: Use pre-compiled patterns
                        match = REGEX_PATTERNS['month_yyyy_mm'].search(row_str)
                        if match:
                            month_candidate = match.group(1) or match.group(2)
                            if month_candidate and len(month_candidate) >= 7:
                                month = month_candidate[:7] if len(month_candidate) == 7 else month_candidate
                                break
                
                print(f"📅 Using month: {month}")
                result = parse_purple_patch_auto_mode(df, db, month)
                return result
            except Exception as e:
                import traceback
                print(f"⚠️  Auto Mode parsing failed: {str(e)}")
                print(f"📋 Traceback: {traceback.format_exc()}")
                print("🔄 Falling back to standard format...")
                # Continue to standard format parsing below
        else:
            print("ℹ️  Using standard format parsing...")
        
        # BULLETPROOF column matching - handles any variation
        column_mapping = {
            'month': ['Month', 'month', 'MONTH', 'Date', 'date'],
            'particulars': ['Particulars', 'particulars', 'PARTICULARS', 'Product', 'product', 'Item', 'item'],
            'type': ['Type', 'type', 'TYPE', 'Source', 'source'],
            'inward_qty': ['Inward Quantity', 'inward quantity', 'INWARD QUANTITY', 'Inward Qty', 'inward qty', 'Inward', 'inward'],
            'inward_rate': ['Inward Eff. Rate', 'inward eff. rate', 'INWARD EFF. RATE', 'Inward Rate', 'inward rate', 'Inward Price', 'inward price'],
            'inward_value': ['Inward Value', 'inward value', 'INWARD VALUE', 'Inward Total', 'inward total'],
            'outward_qty': ['Outward Quantity', 'outward quantity', 'OUTWARD QUANTITY', 'Outward Qty', 'outward qty', 'Outward', 'outward', 'Sold', 'sold'],
            'outward_rate': ['Outward Eff. Rate', 'outward eff. rate', 'OUTWARD EFF. RATE', 'Outward Rate', 'outward rate', 'Outward Price', 'outward price', 'Selling Price', 'selling price'],
            'outward_value': ['Outward Value', 'outward value', 'OUTWARD VALUE', 'Outward Total', 'outward total', 'Sales Value', 'sales value']
        }
        
        # Find matching columns with fuzzy matching
        found_columns = {}
        for key, possible_names in column_mapping.items():
            for col_name in df.columns:
                col_clean = str(col_name).strip().lower()
                for possible in possible_names:
                    if col_clean == possible.lower() or col_clean in possible.lower() or possible.lower() in col_clean:
                        found_columns[key] = col_name
                        print(f"✅ Mapped '{col_name}' -> {key}")
                        break
                if key in found_columns:
                    break
        
        print(f"📋 Final column mapping: {found_columns}")
        
        # Check required columns
        required_keys = ['particulars', 'outward_qty', 'outward_rate']
        missing_keys = [key for key in required_keys if key not in found_columns]
        
        if missing_keys:
            return {
                "success": False,
                "message": f"Missing required columns: {', '.join(missing_keys)}. Found: {', '.join(df.columns)}",
                "products_created": 0,
                "sales_created": 0,
                "parsed_data": [],
                "errors": [f"Missing columns: {', '.join(missing_keys)}"]
            }
        
        parsed_data = []
        errors = []
        products_created = 0
        sales_created = 0
        rows_processed = 0
        rows_split = 0  # Track how many rows were split into multiple records
        
        print(f"🔄 Processing {len(df)} rows from Excel file...")
        
        for index, row in df.iterrows():
            try:
                # Extract and clean data
                month = str(row[found_columns['month']]).strip() if found_columns.get('month') else "2025-04"
                particulars = str(row[found_columns['particulars']]).strip()
                product_type = str(row[found_columns['type']]).strip() if found_columns.get('type') else "Outsourced"
                
                # Skip empty rows
                if not particulars or particulars.lower() in ['', 'nan', 'none']:
                    print(f"⚠️  Skipping row {index + 2}: Empty particulars")
                    continue
                
                rows_processed += 1  # Count non-empty rows
                
                # Extract quantities with unit detection
                inward_qty_raw = row[found_columns['inward_qty']] if found_columns.get('inward_qty') else ""
                outward_qty_raw = row[found_columns['outward_qty']] if found_columns.get('outward_qty') else ""
                
                # Parse quantities and detect units
                inward_qty, inward_unit = parse_quantity_with_unit(inward_qty_raw)
                outward_qty, outward_unit = parse_quantity_with_unit(outward_qty_raw)
                
                # Extract rates and values
                inward_rate = parse_numeric(row[found_columns['inward_rate']]) if found_columns.get('inward_rate') else 0.0
                inward_value = parse_numeric(row[found_columns['inward_value']]) if found_columns.get('inward_value') else 0.0
                outward_rate = parse_numeric(row[found_columns['outward_rate']]) if found_columns.get('outward_rate') else 0.0
                outward_value = parse_numeric(row[found_columns['outward_value']]) if found_columns.get('outward_value') else 0.0
                
                # Skip rows with no meaningful data
                if outward_qty <= 0 and inward_qty <= 0:
                    print(f"⚠️  Skipping row {index + 2}: {particulars} - No quantity data")
                    continue
                
                # Handle missing outward data (use inward as outward)
                if outward_qty <= 0 and inward_qty > 0:
                    outward_qty = inward_qty
                    outward_rate = inward_rate
                    outward_value = inward_value
                    outward_unit = inward_unit
                    print(f"🔄 Row {index + 2}: Using inward as outward for {particulars}")
                
                # Handle missing inward data (set to 0)
                if inward_qty <= 0:
                    inward_qty = 0.0
                    inward_rate = 0.0
                    inward_value = 0.0
                
                print(f"✅ Processing row {index + 2}: {particulars}")
                print(f"   📦 Inward: {inward_qty} {inward_unit} @ ₹{inward_rate}")
                print(f"   📤 Outward: {outward_qty} {outward_unit} @ ₹{outward_rate}")
                
                # Calculate production and wastage
                diff = outward_qty - inward_qty
                inhouse_production = max(0, diff)
                wastage = max(0, -diff)
                
                # Normalize product type
                source = "inhouse" if product_type.lower() in ["in-house", "inhouse", "in house"] else "outsourced"
                
                # Apply split logic for OutwardQty > InwardQty
                if diff > 0 and source == "outsourced":
                    # Only create the inhouse portion (the excess production)
                    print(f"   🔄 Splitting {particulars}: Only creating inhouse portion ({diff} kg), skipping outsourced portion ({inward_qty} kg)")
                    rows_split += 1  # Track that this row was split
                    
                    # Create only the inhouse portion
                    product_name = f"{particulars} (Inhouse)"
                    
                    # Create or get product
                    product = db.query(Product).filter(Product.name == product_name).first()
                    if not product:
                        product = Product(
                            name=product_name,
                            source="inhouse",
                            unit=outward_unit if outward_unit else "kg"
                        )
                        db.add(product)
                        db.commit()
                        db.refresh(product)
                        products_created += 1
                        print(f"   📦 Created product: {product_name}")
                    
                    # Create monthly sale record for inhouse portion only
                    monthly_sale = MonthlySale(
                        product_id=product.id,
                        month=month,
                        quantity=diff,  # Only the inhouse production amount
                        sale_price=outward_rate,
                        direct_cost=0.0,  # No direct cost for inhouse production
                        inward_quantity=0.0,  # No inward for inhouse production
                        inward_rate=0.0,
                        inward_value=0.0,
                        inhouse_production=diff,
                        wastage=0.0
                    )
                    
                    db.add(monthly_sale)
                    sales_created += 1
                    print(f"   💰 Created sale (inhouse only): {diff}{outward_unit} @ ₹{outward_rate}")
                    
                    # Add to parsed data
                    parsed_data.append(ExcelRowData(
                        month=month,
                        particulars=particulars,
                        type="Inhouse",
                        inward_quantity=0.0,
                        inward_rate=0.0,
                        inward_value=0.0,
                        outward_quantity=diff,
                        outward_rate=outward_rate,
                        outward_value=diff * outward_rate,
                        inhouse_production=diff,
                        wastage=0.0
                        ))
                else:
                    # Single record (no split needed)
                    product_name = f"{particulars} ({source.title()})"
                    
                    # Create or get product
                    product = db.query(Product).filter(Product.name == product_name).first()
                    if not product:
                        product = Product(
                            name=product_name,
                            source=source,
                            unit=outward_unit if outward_unit else "kg"
                        )
                        db.add(product)
                        db.commit()
                        db.refresh(product)
                        products_created += 1
                        print(f"   📦 Created product: {product_name}")
                    
                    # Create monthly sale record
                    monthly_sale = MonthlySale(
                        product_id=product.id,
                        month=month,
                        quantity=outward_qty,
                        sale_price=outward_rate,
                        direct_cost=inward_value if inward_value > 0 else (inward_qty * inward_rate),
                        inward_quantity=inward_qty,
                        inward_rate=inward_rate,
                        inward_value=inward_value,
                        inhouse_production=inhouse_production,
                        wastage=wastage
                    )
                    
                    db.add(monthly_sale)
                    sales_created += 1
                    print(f"   💰 Created sale: {outward_qty}{outward_unit} @ ₹{outward_rate}")
                    
                    # Add to parsed data
                    parsed_data.append(ExcelRowData(
                        month=month,
                        particulars=particulars,
                        type=product_type,
                        inward_quantity=inward_qty,
                        inward_rate=inward_rate,
                        inward_value=inward_value,
                        outward_quantity=outward_qty,
                        outward_rate=outward_rate,
                        outward_value=outward_value,
                        inhouse_production=inhouse_production,
                        wastage=wastage
                    ))
                
            except Exception as e:
                error_msg = f"Row {index + 2}: {str(e)}"
                errors.append(error_msg)
                print(f"❌ Error processing row {index + 2}: {error_msg}")
                continue
        
        db.commit()
        
        print(f"✅ BULLETPROOF upload completed!")
        print(f"   📋 Excel rows processed: {rows_processed} (from {len(df)} total rows)")
        if rows_split > 0:
            print(f"   🔄 Rows split into multiple records: {rows_split} (created {rows_split} extra records)")
        print(f"   📦 Products created: {products_created}")
        print(f"   💰 Sales records created: {sales_created}")
        print(f"   📊 Total records in parsed_data: {len(parsed_data)}")
        
        # Create a more informative message
        if rows_split > 0:
            message = f"Successfully processed {rows_processed} Excel rows. {rows_split} rows were split (OutwardQty > InwardQty), creating {sales_created} total sales records."
        else:
            message = f"Successfully processed {rows_processed} Excel rows, creating {sales_created} sales records."
        
        return {
            "success": True,
            "message": message,
            "excel_rows_processed": rows_processed,
            "rows_split": rows_split,
            "products_created": products_created,
            "sales_created": sales_created,
            "parsed_data": [data.model_dump() for data in parsed_data],
            "errors": errors
        }
        
    except Exception as e:
        print(f"💥 BULLETPROOF upload failed: {str(e)}")
        return {
            "success": False,
            "message": f"Upload failed: {str(e)}",
            "products_created": 0,
            "sales_created": 0,
            "parsed_data": [],
            "errors": [str(e)]
        }

def parse_quantity_with_unit(value):
    """Parse quantity and extract unit from string like '53.500 Kg' or '855 EA'"""
    if pd.isna(value) or value == "" or str(value).strip() == "":
        return 0.0, "kg"
    
    value_str = str(value).strip()
    
    # OPTIMIZED: Use pre-compiled regex pattern
    match = REGEX_PATTERNS['quantity_unit'].match(value_str)
    
    if match:
        quantity_str = match.group(1).replace(',', '')
        unit = match.group(2).strip().upper()
        
        try:
            quantity = float(quantity_str)
            return quantity, unit if unit else "kg"
        except ValueError:
            return 0.0, "kg"
    
    # Try to parse as pure number
    try:
        quantity = float(value_str)
        return quantity, "kg"
    except ValueError:
        return 0.0, "kg"

def parse_numeric(value):
    """Parse numeric value, handling empty cells and various formats"""
    if pd.isna(value) or value == "" or str(value).strip() == "":
        return 0.0
    
    try:
        # Remove commas and convert to float
        value_str = str(value).replace(',', '').strip()
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0

def parse_numeric_robust(value):
    """
    EXTREMELY ROBUST number parsing - handles all edge cases:
    - ₹ symbols, commas, spaces
    - Indian number format (1,03,134.10 → 103134.10)
    - Decimals (1907988.5 → 1907988.5)
    - Merged cells, empty rows, text mixed with numbers
    - Never loses or rounds any value
    OPTIMIZED: Uses pre-compiled regex patterns
    Time Complexity: O(n) where n = string length
    """
    if pd.isna(value) or value == "" or str(value).strip() == "":
        return 0.0
    
    value_str = str(value).strip()
    
    # OPTIMIZED: Use pre-compiled regex pattern
    value_str = REGEX_PATTERNS['currency'].sub('', value_str)
    
    # Remove all spaces
    value_str = value_str.replace(' ', '')
    
    # OPTIMIZED: Use pre-compiled regex pattern
    number_match = REGEX_PATTERNS['indian_number'].search(value_str)
    if number_match:
        number_str = number_match.group(0)
        # Remove all commas
        number_str = number_str.replace(',', '')
        try:
            return float(number_str)
        except ValueError:
            return 0.0
    
    # Try direct conversion if no commas found
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0

def detect_purple_patch_format(df):
    """
    Auto-detect if Excel file is Purple Patch Farms format by scanning for keywords.
    Returns True if detected, False otherwise.
    OPTIMIZED: Early exit, single-pass scan, avoid full DataFrame conversion.
    Time Complexity: O(n*m*k) where n=rows, m=cols, k=keywords (but early exit)
    """
    keywords = [
        "PURPLE PATCH FARMS",
        "COST ANALYSIS",
        "TOTAL QTY SOLD",
        "FIXED COST CAT",
        "VARIABLE COST",
        "Open Field",
        "LETTUCE",
        "STRAWBERRY",
        "RASPBERRY&BLUBERRY",
        "PACKING",
        "AGGREGATION",
        "Production Kg",
        "Damage Kg",
        "Sales Kg"
    ]
    
    # Pre-compile keywords to uppercase for faster comparison
    keywords_upper = [k.upper() for k in keywords]
    
    # OPTIMIZED: Scan only first 100 rows and columns (most headers are at top)
    # Early exit on first match - O(n*m*k) worst case, but typically O(1) with early exit
    max_rows = min(100, len(df))
    max_cols = min(20, len(df.columns))
    
    for idx in range(max_rows):
        row = df.iloc[idx]
        # Convert row to string only when needed (lazy evaluation)
        row_str = ' '.join(str(cell).upper() for cell in row.iloc[:max_cols] if pd.notna(cell))
        
        for keyword in keywords_upper:
            if keyword in row_str:
                print(f"✅ Auto-detection: Found keyword '{keyword}' - Switching to Auto Mode")
                return True
    
    print("ℹ️  Auto-detection: No Purple Patch keywords found - Using standard format")
    return False

def parse_purple_patch_auto_mode(df, db, month="2025-04"):
    """
    Parse Purple Patch Farms Excel format in Auto Mode.
    Handles the actual Excel structure with:
    - FIXED COST CAT - I (with individual line items and total)
    - FIXED COST CAT - II (with apportionment: Strawberry 60%, Greens 25%, Aggregation 15%)
    - VARIABLE COST sections (A-F: Open Field, Lettuce, Strawberry, Raspberry&Blueberry, Packing, Aggregation)
    - Production table at bottom (Production Kg, Damage Kg, Sales Kg)
    - TOTAL QTY SOLD
    """
    print(f"🚀 Starting Auto Mode parsing for Purple Patch format...")
    
    parsed_data = []
    products_created = 0
    sales_created = 0
    costs_created = 0
    errors = []
    
    # Convert all cells to string for searching (case-insensitive)
    df_str = df.astype(str)
    
    # Print first few rows for debugging
    print(f"📋 Excel structure preview (first 10 rows):")
    for i in range(min(10, len(df_str))):
        row_preview = [str(df_str.iloc[i, j])[:40] if j < len(df_str.columns) and pd.notna(df_str.iloc[i, j]) else '' for j in range(min(8, len(df_str.columns)))]
        print(f"   Row {i+1}: {row_preview}")
    
    # Product category mappings
    category_mapping = {
        'OPEN FIELD': 'Open Field',
        'LETTUCE': 'Polyhouse Greens',  # C+D+E combined
        'STRAWBERRY': 'Strawberry',
        'RASPBERRY&BLUBERRY': 'Other Berries',
        'RASPBERRY': 'Other Berries',
        'BLUBERRY': 'Other Berries',
        'BLUEBERRY': 'Other Berries',
        'PACKING': 'Packing',
        'AGGREGATION': 'Aggregation'
    }
    
    # ============================================
    # STEP 1: Extract TOTAL QTY SOLD
    # OPTIMIZED: Limited row scan with early exit
    # Time Complexity: O(n*m) worst case, but early exit (typically O(1))
    # ============================================
    print("📊 Step 1: Extracting TOTAL QTY SOLD...")
    total_qty_sold = 0.0
    # OPTIMIZED: Scan only first 100 rows (headers are usually at top)
    max_scan_rows = min(100, len(df_str))
    for idx in range(max_scan_rows):
        row = df_str.iloc[idx]
        row_len = len(row)
        # OPTIMIZED: Limit column scan to first 20 columns
        max_cols = min(20, row_len)
        for col_idx in range(max_cols):
            cell_upper = str(row.iloc[col_idx]).upper()
            if 'TOTAL QTY SOLD' in cell_upper or 'TOTAL QTY' in cell_upper:
                # Look for number in same row or next cells (limit to 5 columns ahead)
                for next_col in range(col_idx, min(col_idx + 5, row_len)):
                    try:
                        val = parse_numeric_robust(row.iloc[next_col])
                        if val > 0:
                            total_qty_sold = val
                            print(f"   ✅ Found TOTAL QTY SOLD: {total_qty_sold} kg")
                            break
                    except (IndexError, KeyError):
                        continue
                if total_qty_sold > 0:
                    break
        if total_qty_sold > 0:
            break
    
    # ============================================
    # STEP 2: Extract FIXED COST CAT - I (with individual line items and total)
    # ============================================
    print("📊 Step 2: Extracting FIXED COST CAT - I...")
    fixed_cost_1_items = []  # List of individual cost items
    fixed_cost_1_total = 0.0
    fixed_cost_1_per_kg = 0.0
    
    # OPTIMIZED: Find FIXED COST CAT - I section with limited scan
    fixed_cost_1_start = None
    fixed_cost_1_end = None
    
    # OPTIMIZED: Scan only first 200 rows (cost sections are usually in first half)
    max_scan_rows = min(200, len(df_str))
    for idx in range(max_scan_rows):
        row = df_str.iloc[idx]
        # OPTIMIZED: Only check first 10 columns for header
        row_str = ' '.join([str(row.iloc[c]).upper() for c in range(min(10, len(row))) if pd.notna(row.iloc[c])])
        if 'FIXED COST CAT' in row_str and ('I' in row_str or '1' in row_str) and 'II' not in row_str and '2' not in row_str:
            fixed_cost_1_start = idx
            print(f"   📍 Found FIXED COST CAT - I at row {idx + 1}")
            break
    
    if fixed_cost_1_start is not None:
        # OPTIMIZED: Look for individual cost items and total
        # Find the "TOTAL" row which marks the end of this section
        # Limit scan to 50 rows after start (sections are usually compact)
        for idx in range(fixed_cost_1_start, min(fixed_cost_1_start + 50, len(df_str))):
            row = df_str.iloc[idx]
            row_str = ' '.join([str(c).upper() for c in row])
            
            # Check if this is the TOTAL row
            if 'TOTAL' in row_str and fixed_cost_1_end is None:
                # Extract total from this row
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    if val > 1000:  # Total should be a large number
                        fixed_cost_1_total = val
                        fixed_cost_1_end = idx
                        print(f"   ✅ Found FIXED COST CAT - I Total: ₹{fixed_cost_1_total:,.2f} at row {idx + 1}")
                        
                        # Also look for per kg in same row (usually in KG or COP column)
                        for col2 in range(len(row)):
                            val2 = parse_numeric_robust(row.iloc[col2])
                            if 0 < val2 < 100:  # Per kg should be small
                                fixed_cost_1_per_kg = val2
                                print(f"   ✅ Found FIXED COST CAT - I Per Kg: ₹{fixed_cost_1_per_kg:,.2f}")
                        break
            
            # Extract individual cost items (before the TOTAL row)
            if fixed_cost_1_end is None and idx > fixed_cost_1_start:
                # Look for cost item names in first few columns and amounts
                item_name = None
                item_amount = 0.0
                
                # Check if row has a cost item (not empty, not a header)
                for col in range(min(3, len(row))):
                    cell_val = str(row.iloc[col]).strip()
                    if cell_val and cell_val.upper() not in ['NAN', '', 'SL.NO', 'PARTICULARS', 'TOTAL', 'APPORTIONMENT']:
                        # This might be a cost item name
                        if len(cell_val) > 3 and not cell_val.replace('.', '').replace(',', '').isdigit():
                            item_name = cell_val
                            break
                
                # If we found an item name, look for amount in TOTAL column
                if item_name:
                    # Look for amount in columns (usually 2-4 columns after name)
                    for col in range(2, min(6, len(row))):
                        val = parse_numeric_robust(row.iloc[col])
                        if val > 0:
                            item_amount = val
                            fixed_cost_1_items.append({
                                'name': item_name,
                                'amount': item_amount
                            })
                            print(f"   📊 Found cost item: {item_name} = ₹{item_amount:,.2f}")
                            break
    
    # If total not found but we have items, sum them
    if fixed_cost_1_total == 0 and fixed_cost_1_items:
        fixed_cost_1_total = sum(item['amount'] for item in fixed_cost_1_items)
        print(f"   ✅ Calculated FIXED COST CAT - I Total from items: ₹{fixed_cost_1_total:,.2f}")
    
    # ============================================
    # STEP 3: Extract FIXED COST CAT - II (with total and apportionment)
    # ============================================
    print("📊 Step 3: Extracting FIXED COST CAT - II (apportioned)...")
    fixed_cost_2_items = []  # Individual cost items
    fixed_cost_2_strawberry = 0.0
    fixed_cost_2_greens = 0.0
    fixed_cost_2_aggregation = 0.0
    fixed_cost_2_total = 0.0
    
    # Find FIXED COST CAT - II section
    fixed_cost_2_start = None
    fixed_cost_2_end = None
    
    for idx, row in df_str.iterrows():
        row_str = ' '.join([str(c).upper() for c in row])
        if 'FIXED COST CAT' in row_str and ('II' in row_str or '2' in row_str):
            fixed_cost_2_start = idx
            print(f"   📍 Found FIXED COST CAT - II at row {idx + 1}")
            break
    
    if fixed_cost_2_start is not None:
        # Look for the TOTAL row and apportionment rows
        for idx in range(fixed_cost_2_start, min(fixed_cost_2_start + 30, len(df_str))):
            row = df_str.iloc[idx]
            row_str = ' '.join([str(c).upper() for c in row])
            
            # Find the main TOTAL row (should be a large number)
            if 'TOTAL' in row_str and fixed_cost_2_total == 0:
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    if val > 100000:  # Total should be large
                        fixed_cost_2_total = val
                        print(f"   ✅ Found FIXED COST CAT - II Total: ₹{fixed_cost_2_total:,.2f} at row {idx + 1}")
                        break
            
            # Look for apportionment rows (STRAWBERRY -60%, GREENS -25%, AGGREGATION -15%)
            if 'STRAWBERRY' in row_str and ('60' in row_str or '%' in row_str):
                # Extract the apportioned amount (usually a number in the row)
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    if 1000 < val < 100000:  # Apportioned amount should be reasonable
                        fixed_cost_2_strawberry = val
                        print(f"   ✅ Found FIXED COST CAT - II Strawberry (60%): ₹{fixed_cost_2_strawberry:,.2f}")
                        break
            
            if ('GREENS' in row_str or 'GREEN' in row_str) and ('25' in row_str or '%' in row_str):
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    if 1000 < val < 100000:
                        fixed_cost_2_greens = val
                        print(f"   ✅ Found FIXED COST CAT - II Greens (25%): ₹{fixed_cost_2_greens:,.2f}")
                        break
            
            if 'AGGREGATION' in row_str and ('15' in row_str or '%' in row_str):
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    if 1000 < val < 1000000:  # Aggregation can be larger
                        fixed_cost_2_aggregation = val
                        print(f"   ✅ Found FIXED COST CAT - II Aggregation (15%): ₹{fixed_cost_2_aggregation:,.2f}")
                        break
    
    # ============================================
    # STEP 4: Extract VARIABLE COST blocks (A-F)
    # ============================================
    print("📊 Step 4: Extracting VARIABLE COST blocks...")
    variable_costs = {
        'Open Field': 0.0,
        'Polyhouse Greens': 0.0,  # LETTUCE (C+D+E combined)
        'Strawberry': 0.0,
        'Other Berries': 0.0,  # RASPBERRY&BLUBERRY
        'Packing': 0.0,
        'Aggregation': 0.0
    }
    
    # Find VARIABLE COST section
    variable_cost_start_idx = None
    for idx, row in df_str.iterrows():
        for cell in row:
            if 'VARIABLE COST' in str(cell).upper():
                variable_cost_start_idx = idx
                break
        if variable_cost_start_idx is not None:
            break
    
    if variable_cost_start_idx is not None:
        print(f"   📍 Found VARIABLE COST section at row {variable_cost_start_idx + 1}")
        
        # Variable cost sections are labeled A) B) C) D) E) F)
        # Each section has a total at the end
        current_section = None
        section_totals = {}  # Track totals for each section
        
        for search_idx in range(variable_cost_start_idx, min(variable_cost_start_idx + 100, len(df_str))):
            row = df_str.iloc[search_idx]
            row_str = ' '.join([str(c).upper() for c in row])
            
            # Detect section headers: A) OPEN FIELD, B) LETTUCE, etc.
            if ') OPEN FIELD' in row_str or 'A) OPEN FIELD' in row_str or 'OPEN FIELD :' in row_str:
                current_section = 'Open Field'
                print(f"   📍 Found section A) OPEN FIELD at row {search_idx + 1}")
            elif ') LETTUCE' in row_str or 'B) LETTUCE' in row_str or 'LETTUCE:' in row_str:
                current_section = 'Polyhouse Greens'
                print(f"   📍 Found section B) LETTUCE at row {search_idx + 1}")
            elif ') STRAWBERRY' in row_str or 'C) STRAWBERRY' in row_str or 'STRAWBERRY:' in row_str:
                current_section = 'Strawberry'
                print(f"   📍 Found section C) STRAWBERRY at row {search_idx + 1}")
            elif ') RASPBERRY' in row_str or 'D) RASPBERRY' in row_str or 'RASPBERRY&BLUBERRY:' in row_str:
                current_section = 'Other Berries'
                print(f"   📍 Found section D) RASPBERRY&BLUBERRY at row {search_idx + 1}")
            elif ') PACKING' in row_str or 'E) PACKING' in row_str or 'PACKING:' in row_str:
                current_section = 'Packing'
                print(f"   📍 Found section E) PACKING at row {search_idx + 1}")
            elif ') AGGREGATION' in row_str or 'F) AGGREGATION' in row_str or 'AGGREGATION' in row_str and current_section != 'Other Berries':
                current_section = 'Aggregation'
                print(f"   📍 Found section F) AGGREGATION at row {search_idx + 1}")
            
            # Look for totals in each section (usually a large number on its own row or at end of section)
            if current_section:
                # Check if this row has a large number that could be the section total
                for col in range(len(row)):
                    val = parse_numeric_robust(row.iloc[col])
                    # Section totals are usually large numbers (100k+)
                    if val > 10000 and val > section_totals.get(current_section, 0):
                        # Verify it's not part of a cost item name
                        row_has_text = any(len(str(c).strip()) > 5 and not str(c).replace('.', '').replace(',', '').isdigit() 
                                         for c in row if pd.notna(c))
                        if not row_has_text or val > 100000:  # If it's a large number, it's likely the total
                            section_totals[current_section] = val
                            variable_costs[current_section] = val
                            print(f"   ✅ {current_section} total: ₹{val:,.2f}")
            
            # Check if we've moved to next major section (like DISTRIBUTION COST)
            if 'DISTRIBUTION COST' in row_str or 'MARKETING EXPENSES' in row_str or 'VEHICLE RUNNING COST' in row_str or 'WASTAGE' in row_str or 'PURCHASE ACCOUNTS' in row_str:
                current_section = None
    
    # ============================================
    # STEP 4B: Extract additional cost sections (DISTRIBUTION, MARKETING, VEHICLE, OTHERS, WASTAGE, PURCHASE)
    # ============================================
    print("📊 Step 4B: Extracting additional cost sections...")
    additional_costs = {
        'Distribution Cost': {'amount': 0.0, 'allocation': 'both'},
        'Marketing Expenses': {'amount': 0.0, 'allocation': 'both'},
        'Vehicle Running Cost': {'amount': 0.0, 'allocation': 'both'},
        'Others': {'amount': 0.0, 'allocation': 'both'},
        'Wastage & Shortage': {'amount': 0.0, 'allocation': 'split'},  # Split by inhouse/outsourced
        'Purchase Accounts': {'amount': 0.0, 'allocation': 'outsourced'}
    }
    
    # Find each section and extract totals
    for idx, row in df_str.iterrows():
        row_str = ' '.join([str(c).upper() for c in row])
        
        # DISTRIBUTION COST
        if 'DISTRIBUTION COST' in row_str:
            # Look for total in this row or next few rows
            for search_idx in range(idx, min(idx + 15, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                # Check if this is a total row (has large number and possibly "TOTAL" or is after all items)
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if val > 1000000 and val < 5000000:  # Distribution cost range
                        # Verify it's not part of a line item name
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['DRIVER', 'TRANSPORT', 'LOADING', 'DELIVERY', 'HAMPER', 'PARKING']):
                            additional_costs['Distribution Cost']['amount'] = val
                            print(f"   ✅ Found DISTRIBUTION COST: ₹{val:,.2f} at row {search_idx + 1}")
                            break
                if additional_costs['Distribution Cost']['amount'] > 0:
                    break
        
        # MARKETING EXPENSES
        if 'MARKETING EXPENSES' in row_str:
            for search_idx in range(idx, min(idx + 15, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if 100000 < val < 1000000:  # Marketing expenses range
                        # Verify it's not part of a line item name
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['TRAVELLING', 'SALES TEAM', 'ADVERTISMENT', 'INSTAGRAM']):
                            if val > additional_costs['Marketing Expenses']['amount']:
                                additional_costs['Marketing Expenses']['amount'] = val
                                print(f"   ✅ Found MARKETING EXPENSES: ₹{val:,.2f} at row {search_idx + 1}")
                                break
                if additional_costs['Marketing Expenses']['amount'] > 0:
                    break
        
        # VEHICLE RUNNING COST
        if 'VEHICLE RUNNING COST' in row_str:
            for search_idx in range(idx, min(idx + 15, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if val > 2000000 and val < 5000000:  # Vehicle costs are very large
                        # Verify it's not part of a line item name
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['VEHICLE DIESEL', 'VEHICLE MAINTANANCE', 'VEHICLE PERMIT', 'VEHICLE INSURANCE']):
                            if val > additional_costs['Vehicle Running Cost']['amount']:
                                additional_costs['Vehicle Running Cost']['amount'] = val
                                print(f"   ✅ Found VEHICLE RUNNING COST: ₹{val:,.2f} at row {search_idx + 1}")
                                break
                if additional_costs['Vehicle Running Cost']['amount'] > 0:
                    break
        
        # OTHERS (section 6)
        if (row_str.strip() == 'OTHERS' or (row_str.startswith('6') and 'OTHERS' in row_str)) and additional_costs['Others']['amount'] == 0:
            for search_idx in range(idx, min(idx + 25, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                # Look for total - either explicit TOTAL or a large number after all items
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if 100000 < val < 1000000:
                        # Check if this row is after all OTHERS items (no item names)
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['BANKING', 'COURIER', 'DEBTORS', 'DISCOUNT', 'FINANCE', 'FINE', 'FREE', 'FREIGHT', 'MISCELLANEOUS', 'OFFICE', 'ROUND', 'TEA', 'TRAVELLING']):
                            additional_costs['Others']['amount'] = val
                            print(f"   ✅ Found OTHERS: ₹{val:,.2f} at row {search_idx + 1}")
                            break
                if additional_costs['Others']['amount'] > 0:
                    break
        
        # WASTAGE & SHORTAGE (section 7)
        if ('WASTAGE' in row_str and 'SHORTAGE' in row_str) or (row_str.startswith('7') and 'WASTAGE' in row_str):
            for search_idx in range(idx, min(idx + 15, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if val > 500000 and val < 2000000:  # Wastage range
                        # Verify it's not part of a line item name
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['WASTAGE-OWN FARM', 'WASTAGE-DISPATCH', 'WASTAGE- FARM']):
                            if val > additional_costs['Wastage & Shortage']['amount']:
                                additional_costs['Wastage & Shortage']['amount'] = val
                                print(f"   ✅ Found WASTAGE & SHORTAGE: ₹{val:,.2f} at row {search_idx + 1}")
                                break
                if additional_costs['Wastage & Shortage']['amount'] > 0:
                    break
        
        # PURCHASE ACCOUNTS (section 8)
        if ('PURCHASE ACCOUNTS' in row_str or (row_str.startswith('8') and 'PURCHASE' in row_str)) and additional_costs['Purchase Accounts']['amount'] == 0:
            for search_idx in range(idx, min(idx + 15, len(df_str))):
                search_row = df_str.iloc[search_idx]
                search_row_str = ' '.join([str(c).upper() for c in search_row])
                for col in range(len(search_row)):
                    val = parse_numeric_robust(search_row.iloc[col])
                    if val > 10000000 and val < 20000000:  # Purchase accounts are very large
                        # Verify it's not part of a line item name
                        row_text = ' '.join([str(c).upper() for c in search_row if not str(c).replace('.', '').replace(',', '').isdigit()])
                        if not any(keyword in row_text for keyword in ['PURCHASE VEGETABLES', 'PURCHASE OTHERS']):
                            if val > additional_costs['Purchase Accounts']['amount']:
                                additional_costs['Purchase Accounts']['amount'] = val
                                print(f"   ✅ Found PURCHASE ACCOUNTS: ₹{val:,.2f} at row {search_idx + 1}")
                                break
                if additional_costs['Purchase Accounts']['amount'] > 0:
                    break
    
    # ============================================
    # STEP 5: Extract Production table (bottom section)
    # ============================================
    print("📊 Step 5: Extracting Production table...")
    production_data = {}  # {product_name: {'production_kg': 0, 'damage_kg': 0, 'sales_kg': 0}}
    
    # Find production section (look for "Production Kg", "Damage Kg", "Sales Kg")
    production_start_idx = None
    for idx, row in df_str.iterrows():
        row_str = ' '.join([str(c).upper() for c in row])
        if 'PRODUCTION' in row_str and ('KG' in row_str or 'QTY' in row_str):
            production_start_idx = idx
            print(f"   📍 Found Production header at row {idx + 1}: {row_str[:100]}")
            break
        elif 'DAMAGE' in row_str and ('KG' in row_str or 'QTY' in row_str):
            production_start_idx = idx
            print(f"   📍 Found Damage header at row {idx + 1}: {row_str[:100]}")
            break
        elif 'SALES' in row_str and ('KG' in row_str or 'QTY' in row_str):
            production_start_idx = idx
            print(f"   📍 Found Sales header at row {idx + 1}: {row_str[:100]}")
            break
    
    if production_start_idx is not None:
        print(f"   📍 Using Production table starting at row {production_start_idx + 1}")
        
        # Find column indices for Production, Damage, Sales, Av price
        header_row = df_str.iloc[production_start_idx]
        prod_col = None
        damage_col = None
        sales_col = None
        price_col = None
        
        for col_idx, cell in enumerate(header_row):
            cell_upper = str(cell).upper()
            if ('PRODUCTION' in cell_upper or 'PROD' in cell_upper) and ('KG' in cell_upper or 'QTY' in cell_upper):
                prod_col = col_idx
                print(f"   ✅ Found Production column at index {col_idx}")
            if 'DAMAGE' in cell_upper and ('KG' in cell_upper or 'QTY' in cell_upper):
                damage_col = col_idx
                print(f"   ✅ Found Damage column at index {damage_col}")
            if 'SALES' in cell_upper and ('KG' in cell_upper or 'QTY' in cell_upper):
                sales_col = col_idx
                print(f"   ✅ Found Sales column at index {sales_col}")
            if 'AV PRICE' in cell_upper or 'AVG PRICE' in cell_upper or 'PRICE' in cell_upper:
                price_col = col_idx
                print(f"   ✅ Found Average Price column at index {price_col}")
        
        # Extract data rows - try multiple product name variations
        product_names_variations = {
            'Strawberry': ['STRAWBERRY', 'STRAW', 'STRAWBERRY'],
            'Greens PH C': ['GREENS PH C', 'GREENS C', 'GREEN C', 'PH C', 'POLYHOUSE C'],
            'Greens PH D': ['GREENS PH D', 'GREENS D', 'GREEN D', 'PH D', 'POLYHOUSE D'],
            'Greens PH E': ['GREENS PH E', 'GREENS E', 'GREEN E', 'PH E', 'POLYHOUSE E'],
            'Open farm': ['OPEN FARM', 'OPEN FIELD', 'OPEN', 'FARM'],
            'Raspberry and Blueberry': ['RASPBERRY', 'BLUEBERRY', 'BLUBERRY', 'RASPBERRY AND BLUEBERRY', 'RASPBERRY&BLUEBERRY'],
            'Aggregation': ['AGGREGATION', 'AGG', 'AGGREGATE']
        }
        
        # Also try to find product names in first column
        for search_idx in range(production_start_idx + 1, min(production_start_idx + 30, len(df_str))):
            try:
                row = df_str.iloc[search_idx]
                row_str = ' '.join([str(c).upper() for c in row])
                
                # Skip empty rows
                if not row_str.strip() or row_str.strip() == 'NAN':
                    continue
                
                # Check if this row contains a product name
                for prod_name, variations in product_names_variations.items():
                    for variation in variations:
                        if variation in row_str:
                            # Try to extract numbers from this row
                            prod_kg = 0.0
                            dmg_kg = 0.0
                            sales_kg = 0.0
                            
                            # Try to find numbers in the row
                            for col_idx in range(len(row)):
                                try:
                                    cell_val = str(row.iloc[col_idx]).strip()
                                    num_val = parse_numeric_robust(cell_val)
                                    
                                    # Assign based on column position or header
                                    if prod_col is not None and col_idx == prod_col:
                                        prod_kg = num_val
                                    elif damage_col is not None and col_idx == damage_col:
                                        dmg_kg = num_val
                                    elif sales_col is not None and col_idx == sales_col:
                                        sales_kg = num_val
                                    # Fallback: if columns not found, try to infer from position
                                    elif prod_col is None and damage_col is None and sales_col is None:
                                        # First number after product name might be production
                                        if num_val > 0 and prod_kg == 0:
                                            prod_kg = num_val
                                        elif num_val > 0 and prod_kg > 0 and sales_kg == 0:
                                            sales_kg = num_val
                                        elif num_val > 0 and sales_kg > 0:
                                            dmg_kg = num_val
                                except (IndexError, KeyError, ValueError):
                                    continue
                            
                            # Also extract average price if available
                            avg_price = 0.0
                            if price_col is not None:
                                try:
                                    price_val = parse_numeric_robust(row.iloc[price_col])
                                    if 50 < price_val < 500:  # Reasonable price range
                                        avg_price = price_val
                                except:
                                    pass
                            
                            if prod_kg > 0 or sales_kg > 0:
                                if prod_name not in production_data:
                                    production_data[prod_name] = {
                                        'production_kg': 0.0,
                                        'damage_kg': 0.0,
                                        'sales_kg': 0.0,
                                        'avg_price': 0.0
                                    }
                                production_data[prod_name]['production_kg'] += prod_kg
                                production_data[prod_name]['damage_kg'] += dmg_kg
                                production_data[prod_name]['sales_kg'] += sales_kg
                                if avg_price > 0:
                                    production_data[prod_name]['avg_price'] = avg_price
                                print(f"   ✅ {prod_name}: Production={prod_kg}kg, Damage={dmg_kg}kg, Sales={sales_kg}kg, Price=₹{avg_price}")
                            break
                    else:
                        continue
                    break
            except (IndexError, KeyError) as e:
                print(f"   ⚠️  Error processing row {search_idx + 1}: {e}")
                continue
    else:
        print("   ⚠️  Production table header not found - trying fallback extraction...")
        # Fallback: Try to find any rows with product names and numbers
        for idx, row in df_str.iterrows():
            row_str = ' '.join([str(c).upper() for c in row])
            for prod_name, variations in product_names_variations.items():
                for variation in variations:
                    if variation in row_str:
                        # Extract any numbers from this row
                        numbers = []
                        for col_idx in range(len(row)):
                            try:
                                num = parse_numeric_robust(row.iloc[col_idx])
                                if num > 0:
                                    numbers.append(num)
                            except:
                                continue
                        
                        if len(numbers) >= 2:  # At least production and sales
                            if prod_name not in production_data:
                                production_data[prod_name] = {
                                    'production_kg': numbers[0] if len(numbers) > 0 else 0.0,
                                    'damage_kg': numbers[2] if len(numbers) > 2 else 0.0,
                                    'sales_kg': numbers[1] if len(numbers) > 1 else 0.0
                                }
                                print(f"   ✅ Fallback: {prod_name}: Production={production_data[prod_name]['production_kg']}kg, Sales={production_data[prod_name]['sales_kg']}kg")
                        break
    
    # ============================================
    # STEP 6: Create MonthlySales records from Production data
    # ============================================
    print("📊 Step 6: Creating MonthlySales records...")
    print(f"   📋 Found {len(production_data)} products in production data")
    
    # Map production product names to our categories
    production_to_category = {
        'Strawberry': 'Strawberry',
        'Greens PH C': 'Polyhouse Greens',
        'Greens PH D': 'Polyhouse Greens',
        'Greens PH E': 'Polyhouse Greens',
        'Open farm': 'Open Field',
        'Raspberry and Blueberry': 'Other Berries',
        'Aggregation': 'Aggregation'
    }
    
    # Aggregate Greens PH C/D/E into single Polyhouse Greens
    greens_total_sales = 0.0
    greens_total_prod = 0.0
    greens_total_damage = 0.0
    for prod_name in ['Greens PH C', 'Greens PH D', 'Greens PH E']:
        if prod_name in production_data:
            greens_total_sales += production_data[prod_name]['sales_kg']
            greens_total_prod += production_data[prod_name]['production_kg']
            greens_total_damage += production_data[prod_name]['damage_kg']
    
    # Create sales records
    for prod_name, data in production_data.items():
        if prod_name in ['Greens PH C', 'Greens PH D', 'Greens PH E']:
            continue  # Skip individual greens, will create aggregated
        
        category = production_to_category.get(prod_name, prod_name)
        sales_kg = data['sales_kg']
        production_kg = data['production_kg']
        damage_kg = data['damage_kg']
        
        print(f"   🔍 Processing {prod_name}: sales={sales_kg}kg, production={production_kg}kg, damage={damage_kg}kg")
        
        if sales_kg > 0:
            # Determine source: if production > 0, it's inhouse; otherwise outsourced
            source = "inhouse" if production_kg > 0 else "outsourced"
            
            # Create or get product
            product_name = f"{category} ({source.title()})"
            product = db.query(Product).filter(Product.name == product_name).first()
            if not product:
                product = Product(
                    name=product_name,
                    source=source,
                    unit="kg"
                )
                db.add(product)
                db.commit()
                db.refresh(product)
                products_created += 1
                print(f"   📦 Created product: {product_name}")
            
            # Calculate wastage and production
            wastage = damage_kg
            inhouse_production = max(0, production_kg - sales_kg) if production_kg > sales_kg else 0
            
            # Use average price from production data if available
            sale_price = data.get('avg_price', 0.0)
            if sale_price == 0:
                sale_price = 50.0  # Default fallback
            print(f"   💵 Using sale price: ₹{sale_price} for {prod_name}")
            
            # Create monthly sale
            monthly_sale = MonthlySale(
                product_id=product.id,
                month=month,
                quantity=sales_kg,
                sale_price=sale_price,
                direct_cost=0.0,
                inward_quantity=production_kg,
                inward_rate=0.0,
                inward_value=0.0,
                inhouse_production=inhouse_production,
                wastage=wastage
            )
            db.add(monthly_sale)
            sales_created += 1
            
            parsed_data.append(ExcelRowData(
                month=month,
                particulars=category,
                type=source.title(),
                inward_quantity=production_kg,
                inward_rate=0.0,
                inward_value=0.0,
                outward_quantity=sales_kg,
                outward_rate=sale_price,
                outward_value=sales_kg * sale_price,
                inhouse_production=inhouse_production,
                wastage=wastage
            ))
    
    # Create aggregated Polyhouse Greens if we have data
    if greens_total_sales > 0 or greens_total_prod > 0:
        print(f"   📦 Creating aggregated Polyhouse Greens: sales={greens_total_sales}kg, production={greens_total_prod}kg")
        product_name = "Polyhouse Greens (Inhouse)"
        product = db.query(Product).filter(Product.name == product_name).first()
        if not product:
            product = Product(
                name=product_name,
                source="inhouse",
                unit="kg"
            )
            db.add(product)
            db.commit()
            db.refresh(product)
            products_created += 1
            print(f"   📦 Created product: {product_name}")
        
        # Use sales_kg if available, otherwise use production_kg
        quantity = greens_total_sales if greens_total_sales > 0 else greens_total_prod
        
        monthly_sale = MonthlySale(
            product_id=product.id,
            month=month,
            quantity=quantity,
            sale_price=50.0,
            direct_cost=0.0,
            inward_quantity=greens_total_prod,
            inward_rate=0.0,
            inward_value=0.0,
            inhouse_production=max(0, greens_total_prod - greens_total_sales) if greens_total_sales > 0 else 0,
            wastage=greens_total_damage
        )
        db.add(monthly_sale)
        sales_created += 1
        print(f"   💰 Created sale: {quantity}kg for Polyhouse Greens")
    
    # ============================================
    # STEP 7: Create Cost records
    # ============================================
    print("📊 Step 7: Creating Cost records...")
    
    # FIXED COST CAT - I: Allocate to all products (B classification)
    if fixed_cost_1_total > 0:
        cost = Cost(
            name="Fixed Cost Category I",
            amount=fixed_cost_1_total,
            applies_to="both",
            cost_type="common",
            basis="hybrid",
            month=month,
            is_fixed="fixed",
            category="fixed_cost_1",
            pl_classification="B",
            original_amount=fixed_cost_1_total,
            allocation_ratio=None,
            source_file="auto_mode_upload"
        )
        db.add(cost)
        costs_created += 1
        print(f"   💰 Created Fixed Cost I: ₹{fixed_cost_1_total:,.2f}")
    
    # FIXED COST CAT - II: Apportioned costs
    if fixed_cost_2_strawberry > 0:
        cost = Cost(
            name="Fixed Cost Category II - Strawberry",
            amount=fixed_cost_2_strawberry,
            applies_to="inhouse",  # Strawberry is inhouse
            cost_type="common",
            basis="hybrid",
            month=month,
            is_fixed="fixed",
            category="fixed_cost_2_strawberry",
            pl_classification="I",
            original_amount=fixed_cost_2_strawberry,
            allocation_ratio=1.0,
            source_file="auto_mode_upload"
        )
        db.add(cost)
        costs_created += 1
    
    if fixed_cost_2_greens > 0:
        cost = Cost(
            name="Fixed Cost Category II - Greens",
            amount=fixed_cost_2_greens,
            applies_to="inhouse",
            cost_type="common",
            basis="hybrid",
            month=month,
            is_fixed="fixed",
            category="fixed_cost_2_greens",
            pl_classification="I",
            original_amount=fixed_cost_2_greens,
            allocation_ratio=1.0,
            source_file="auto_mode_upload"
        )
        db.add(cost)
        costs_created += 1
    
    if fixed_cost_2_aggregation > 0:
        cost = Cost(
            name="Fixed Cost Category II - Aggregation",
            amount=fixed_cost_2_aggregation,
            applies_to="both",
            cost_type="common",
            basis="hybrid",
            month=month,
            is_fixed="fixed",
            category="fixed_cost_2_aggregation",
            pl_classification="B",
            original_amount=fixed_cost_2_aggregation,
            allocation_ratio=None,
            source_file="auto_mode_upload"
        )
        db.add(cost)
        costs_created += 1
    
    # VARIABLE COSTS: Create cost records for each category with correct allocation
    variable_cost_allocation = {
        'Open Field': 'inhouse',  # "only to open field products"
        'Polyhouse Greens': 'inhouse',  # "only lettuce related cost"
        'Strawberry': 'inhouse',  # "only strawberry related cost"
        'Other Berries': 'inhouse',  # "only rasberry and bluebbery related cost"
        'Packing': 'both',  # "both"
        'Aggregation': 'outsourced'  # "outsourced"
    }
    
    for category_name, amount in variable_costs.items():
        if amount > 0:
            applies_to = variable_cost_allocation.get(category_name, 'both')
            pl_class = "I" if applies_to == "inhouse" else ("O" if applies_to == "outsourced" else "B")
            
            cost = Cost(
                name=f"Variable Cost - {category_name}",
                amount=amount,
                applies_to=applies_to,
                cost_type="common",
                basis="hybrid",
                month=month,
                is_fixed="variable",
                category=f"variable_cost_{category_name.lower().replace(' ', '_')}",
                pl_classification=pl_class,
                original_amount=amount,
                allocation_ratio=1.0 if pl_class != "B" else None,
                source_file="auto_mode_upload"
            )
            db.add(cost)
            costs_created += 1
            print(f"   💰 Created Variable Cost - {category_name}: ₹{amount:,.2f} ({applies_to})")
    
    # ADDITIONAL COSTS: Create cost records for Distribution, Marketing, Vehicle, Others, Wastage, Purchase
    for cost_name, cost_info in additional_costs.items():
        if cost_info['amount'] > 0:
            allocation = cost_info['allocation']
            if allocation == 'split':
                # Wastage: Split by inhouse/outsourced ratio (use dynamic ratio)
                pl_class = "B"
                applies_to = "both"
            elif allocation == 'outsourced':
                pl_class = "O"
                applies_to = "outsourced"
            else:  # 'both'
                pl_class = "B"
                applies_to = "both"
            
            cost = Cost(
                name=cost_name,
                amount=cost_info['amount'],
                applies_to=applies_to,
                cost_type="common",
                basis="hybrid",
                month=month,
                is_fixed="variable" if cost_name != "Purchase Accounts" else "variable",
                category=cost_name.lower().replace(' ', '_').replace('&', '_'),
                pl_classification=pl_class,
                original_amount=cost_info['amount'],
                allocation_ratio=1.0 if pl_class != "B" else None,
                source_file="auto_mode_upload"
            )
            db.add(cost)
            costs_created += 1
            print(f"   💰 Created {cost_name}: ₹{cost_info['amount']:,.2f} ({applies_to})")
    
    db.commit()
    
    print(f"✅ Auto Mode parsing completed!")
    print(f"   📦 Products created: {products_created}")
    print(f"   💰 Sales created: {sales_created}")
    print(f"   💵 Costs created: {costs_created}")
    print(f"   📊 Rows processed: {len(parsed_data)}")
    print(f"   📋 Production data found: {len(production_data)} products")
    print(f"   💵 Variable costs found: {sum(1 for v in variable_costs.values() if v > 0)} categories")
    
    # If no data was extracted, provide helpful debugging info
    if sales_created == 0 and costs_created == 0:
        print("⚠️  WARNING: No data was extracted from the file!")
        print("   This could mean:")
        print("   1. The file format is different than expected")
        print("   2. The data sections weren't found (check for 'Production Kg', 'Sales Kg', etc.)")
        print("   3. Product names don't match expected patterns")
        print("   💡 Tip: Check the server logs above for what was found during parsing")
        errors.append("No data extracted - file format may differ from expected Purple Patch format")
    
    return {
        "success": True,
        "message": f"Successfully processed Purple Patch format: {sales_created} sales, {costs_created} costs",
        "products_created": products_created,
        "sales_created": sales_created,
        "costs_created": costs_created,
        "parsed_data": [data.model_dump() for data in parsed_data],
        "errors": errors,
        "mode": "auto",
        "debug_info": {
            "production_data_found": len(production_data),
            "variable_costs_found": sum(1 for v in variable_costs.values() if v > 0),
            "total_qty_sold": total_qty_sold,
            "fixed_cost_1_total": fixed_cost_1_total,
            "fixed_cost_2_strawberry": fixed_cost_2_strawberry,
            "fixed_cost_2_greens": fixed_cost_2_greens,
            "fixed_cost_2_aggregation": fixed_cost_2_aggregation
        }
    }

def split_inhouse_outsourced(row):
    """Split a product into outsourced and inhouse portions when OutwardQty > InwardQty"""
    records = []
    
    diff = row['outward_qty'] - row['inward_qty']
    
    if diff > 0:
        # 1️⃣ Outsourced portion
        outsourced_part = {
            'month': row['month'],
            'particulars': row['particulars'],
            'type': 'Outsourced',
            'inward_qty': row['inward_qty'],
            'outward_qty': row['inward_qty'],  # same as inward
            'inward_rate': row['inward_rate'],
            'outward_rate': row['outward_rate'],
            'inward_value': row['inward_qty'] * row['inward_rate'],
            'outward_value': row['inward_qty'] * row['outward_rate'],
            'inhouse_production': 0,
            'wastage': 0,
            'unit': row['outward_unit']
        }
        
        # 2️⃣ Inhouse portion
        inhouse_part = {
            'month': row['month'],
            'particulars': row['particulars'],
            'type': 'Inhouse',
            'inward_qty': 0,
            'outward_qty': diff,  # the excess produced internally
            'inward_rate': row['inward_rate'],  # use same cost rate for now
            'outward_rate': row['outward_rate'],
            'inward_value': 0,
            'outward_value': diff * row['outward_rate'],
            'inhouse_production': diff,
            'wastage': 0,
            'unit': row['outward_unit']
        }
        
        records.extend([outsourced_part, inhouse_part])
        print(f"   🔄 Split: {row['inward_qty']} outsourced + {diff} inhouse")
    else:
        # No split needed - single record
        records.append({
            'month': row['month'],
            'particulars': row['particulars'],
            'type': 'Outsourced',
            'inward_qty': row['inward_qty'],
            'outward_qty': row['outward_qty'],
            'inward_rate': row['inward_rate'],
            'outward_rate': row['outward_rate'],
            'inward_value': row['inward_qty'] * row['inward_rate'],
            'outward_value': row['outward_qty'] * row['outward_rate'],
            'inhouse_production': 0,
            'wastage': abs(diff) if diff < 0 else 0,
            'unit': row['outward_unit']
        })
    
    return records

def parse_purple_patch_pl(file_path, db):
    """Parse Purple Patch P&L Excel and create enhanced Cost records"""
    
    print(f"📊 Parsing Purple Patch P&L: {file_path}")
    
    # Items to EXCLUDE (revenue/trading account items)
    exclude_items = {
        # Sales/Revenue items
        'Hamper Sales (B to C)', 'Karnataka Sales', 'Kerala Sales B', 'Tamilnadu Sales B', 
        'Complement Sales', 'Complement Sales B', 'Customer Quality Issue and Damage B to B', 
        'Customer Quality Issue and Damage B to B  B', 'Customer Quality Issue and Damage(B to C) B',
        'Customer Quality Issue and Damage (B to C)', 'Sales Return',
        'Discount Rate( B to B Rate) B', 'Discount Rate (B to B Rate)', 'DISCOUNT', 'Free Hamper',
        # Trading Account items (NOT actual expenses to allocate)
        'Opening Stock', 'Add: Purchase Accounts', 'Less: Closing Stock', 'Direct Expenses'
    }
    
    # Fixed template mapping
    template_mapping = {
        'Cultivation Expenses I': 'I',
        'Rejection Own Farm Harvest I': 'I',
        'Wastage-in Farm (Quality Check) I': 'I',
        'Entry Fee- Ooty Market O': 'O',
        'Loading and Unloading - Vegetable Purchase & Fruits O': 'O',
        'Drivers Betta B': 'B',
        'ELECTRICITY CHARGES B': 'B',
        'Employee Benefits Expenses B': 'B',
        'Freight Charges B': 'B',
        'Office & Administrative Expenses B': 'B',
        'Running & Maintanance B': 'B',
        'Software Maintananace B': 'B',
        'Transportation Exp B': 'B',
        'Travelling Allowance -Staff B': 'B',
        'Vehicle Fuels B': 'B',
        'Vehicle Maintanance B': 'B',
        'Vehicle Taxes &Insurance B': 'B',
        'Loading Charges Others B': 'B',
        'Miscellaneous Exp B': 'B',
        'Packing Materials Issued A/c B': 'B',
        'Staff House Rent B': 'B',
        'Tea and Food Exp-Staff B': 'B',
        'Delivery Charges': 'B',
        'INTEREST ON INCOME TAX REFUND': 'B',
        'Packing & Forwarding Charges': 'B',
        'Banking Charges': 'B',
        'Distribution Expenses': 'B',
        'Employee Cost': 'B',
        'Finance Cost': 'B',
        'Rates & Taxes': 'B',
        'Rent': 'B',
        'Sales Expenditure': 'B',
        'CDSL DEMAT Charges': 'B',
        'Company Secretary & MCA Filing Charges': 'B',
        'Courier and Postage Charges': 'B',
        'DEMAT of Shares Charges': 'B',
        'Depreciation A/c': 'B',
        'DISCOUNT': 'B',
        'Free Hamper': 'B',
        'FSSAI License Fees': 'B',
        'Interest on Late Payment of TDS': 'B',
        'Interest on Loan From Feroke Boards': 'B',
        'Interest on MA Ashraf Loan': 'B',
        'Interest on MP Cherian Loan': 'B',
        'Land Subdivision Fee': 'B',
        'Legal Expenses': 'B',
        'Loading and Unloading - Sales': 'B',
        'Round Off': 'B',
        'Salary and Allowances': 'B',
        'TDS Filing Charges': 'B',
        'TDS Service Charges': 'B',
        'Trade Mark Registration Consultancy Fee': 'B',
        'Trade Mark Registration Fee': 'B',
        'Wastage - in Dispatch': 'B',
        'Wastage-in Dispatch': 'B'
    }
    
    try:
        # Read Excel file
        df = pd.read_excel(file_path, header=None)
        print(f"📋 Excel loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Print first few rows for debugging
        print(f"📋 First 5 rows preview:")
        for i in range(min(5, len(df))):
            row_preview = [str(df.iloc[i, j])[:30] if pd.notna(df.iloc[i, j]) else 'NaN' for j in range(min(5, len(df.columns)))]
            print(f"   Row {i+1}: {row_preview}")
        
        # OPTIMIZED: Find the period from the data - early exit, pre-compiled patterns
        # Time Complexity: O(n*m) worst case, but early exit on first match (typically O(1))
        period = "Unknown"
        period_patterns_compiled = [
            REGEX_PATTERNS['period_date'],      # 1-Apr-24, 01-Apr-2024
            REGEX_PATTERNS['period_month'],     # Apr-24, Apr-2024
            REGEX_PATTERNS['period_iso'],      # 2024-04
        ]
        
        # OPTIMIZED: Scan only first 100 rows (periods are usually at top)
        max_scan_rows = min(100, len(df))
        for idx in range(max_scan_rows):
            row = df.iloc[idx]
            for col_idx in range(min(10, len(row))):  # Check first 10 columns
                if pd.notna(row.iloc[col_idx]):
                    cell_str = str(row.iloc[col_idx])
                    # OPTIMIZED: Use pre-compiled patterns
                    for pattern in period_patterns_compiled:
                        match = pattern.search(cell_str, re.IGNORECASE)
                        if match:
                            period = match.group(0)
                            print(f"📅 Period detected: {period} (from row {idx+1}, col {col_idx+1})")
                            break
                    if period != "Unknown":
                        break
            if period != "Unknown":
                break
        
        if period == "Unknown":
            print("⚠️  Period not detected, using default")
            period = "2025-04"
        
        # Extract data rows - try multiple column combinations
        data_rows = []
        print("📊 Extracting data rows...")
        
        # Strategy 1: First two columns (original approach)
        for idx, row in df.iterrows():
            try:
                if len(row) >= 2:
                    particulars_raw = row.iloc[0]
                    amount_raw = row.iloc[1] if len(row) > 1 else None
                    
                    if pd.notna(particulars_raw) and pd.notna(amount_raw):
                        particulars = str(particulars_raw).strip()
                        amount_str = str(amount_raw).strip()
                        
                        # Skip empty or header rows
                        skip_patterns = ['', 'nan', 'PURPLE PATCH FARMS', 'Particulars', 'Trading Account', 
                                       'Income Statement', 'NAN', 'NONE', 'N/A']
                        if any(pattern.upper() in particulars.upper() for pattern in skip_patterns):
                            continue
                        
                        # Use robust number parser
                        amount = parse_numeric_robust(amount_raw)
                        
                        if particulars and amount != 0:
                            # Skip revenue/trading account items
                            if particulars in exclude_items:
                                print(f"   ⏭️  Skipped revenue item: {particulars}")
                                continue
                            
                            # Only include actual expenses (costs)
                            data_rows.append({
                                'particulars': particulars,
                                'amount': amount,
                                'type': template_mapping.get(particulars, 'B')  # Default to B if not found
                            })
                            print(f"   📊 Found: {particulars} = ₹{amount:,.2f} ({template_mapping.get(particulars, 'B')})")
            except (IndexError, KeyError, ValueError) as e:
                continue
        
        # Strategy 2: If no data found, try to find "Particulars" header and use that column
        if len(data_rows) == 0:
            print("   🔄 Trying alternative column detection...")
            particulars_col = None
            amount_col = None
            
            # Find header row
            for idx, row in df.iterrows():
                row_str = ' '.join([str(c).upper() if pd.notna(c) else '' for c in row])
                if 'PARTICULARS' in row_str or 'PARTICULAR' in row_str:
                    # Found header row, identify columns
                    for col_idx, cell in enumerate(row):
                        cell_str = str(cell).upper() if pd.notna(cell) else ''
                        if 'PARTICULAR' in cell_str:
                            particulars_col = col_idx
                        elif any(keyword in cell_str for keyword in ['AMOUNT', 'VALUE', 'TOTAL', 'COST', 'EXPENSE']):
                            if amount_col is None:
                                amount_col = col_idx
                    
                    if particulars_col is not None:
                        print(f"   ✅ Found header row at {idx+1}, Particulars col: {particulars_col}, Amount col: {amount_col}")
                        # Extract data from rows below header
                        for data_idx in range(idx + 1, len(df)):
                            try:
                                row = df.iloc[data_idx]
                                if particulars_col < len(row) and (amount_col is None or amount_col < len(row)):
                                    particulars_raw = row.iloc[particulars_col] if particulars_col is not None else None
                                    amount_raw = row.iloc[amount_col] if amount_col is not None else (row.iloc[particulars_col + 1] if particulars_col + 1 < len(row) else None)
                                    
                                    if pd.notna(particulars_raw):
                                        particulars = str(particulars_raw).strip()
                                        if particulars and particulars.upper() not in ['NAN', 'NONE', '']:
                                            amount = parse_numeric_robust(amount_raw) if pd.notna(amount_raw) else 0.0
                                            
                                            if amount != 0:
                                                if particulars not in exclude_items:
                                                    data_rows.append({
                                                        'particulars': particulars,
                                                        'amount': amount,
                                                        'type': template_mapping.get(particulars, 'B')
                                                    })
                                                    print(f"   📊 Found (alt): {particulars} = ₹{amount:,.2f}")
                            except (IndexError, KeyError, ValueError):
                                continue
                    break
        
        # Strategy 3: Try to find any row with text in first column and number in any other column
        if len(data_rows) == 0:
            print("   🔄 Trying fallback: any text + number pattern...")
            for idx, row in df.iterrows():
                try:
                    if len(row) >= 2:
                        first_col = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                        # Skip if first column looks like a header or is empty
                        if not first_col or first_col.upper() in ['NAN', 'NONE', 'PARTICULARS', '']:
                            continue
                        
                        # Look for a number in remaining columns
                        for col_idx in range(1, min(len(row), 10)):  # Check up to 10 columns
                            amount = parse_numeric_robust(row.iloc[col_idx])
                            if amount > 0:
                                if first_col not in exclude_items:
                                    data_rows.append({
                                        'particulars': first_col,
                                        'amount': amount,
                                        'type': template_mapping.get(first_col, 'B')
                                    })
                                    print(f"   📊 Found (fallback): {first_col} = ₹{amount:,.2f}")
                                break
                except (IndexError, KeyError, ValueError):
                    continue
        
        print(f"📊 Found {len(data_rows)} data rows")
        
        # Calculate dynamic ratio based on ACTUAL SALES DATA (weight + value hybrid)
        # alpha = 0.5 means 50% weight, 50% value
        inhouse_ratio, outsourced_ratio = compute_inhouse_outsourced_ratios(db, alpha=0.5)
        
        # Create Cost records
        costs_created = 0
        for row in data_rows:
            particulars = row['particulars']
            amount = row['amount']
            item_type = row['type']
            
            if item_type == 'I':
                # 100% inhouse
                cost = Cost(
                    name=particulars,
                    amount=amount,
                    applies_to="inhouse",
                    cost_type="common",
                    basis="hybrid",
                    month="2025-04-24 00:00:00",  # Match sales data format
                    is_fixed="variable",
                    category="pl_import",
                    pl_classification="I",
                    original_amount=amount,
                    allocation_ratio=1.0,
                    source_file="pl_upload",
                    pl_period=period
                )
                db.add(cost)
                costs_created += 1
                print(f"   📦 Created I cost: {particulars} = ₹{amount:,.2f} (100% inhouse)")
                
            elif item_type == 'O':
                # 100% outsourced
                cost = Cost(
                    name=particulars,
                    amount=amount,
                    applies_to="outsourced",
                    cost_type="common",
                    basis="hybrid",
                    month="2025-04",
                    is_fixed="variable",
                    category="pl_import",
                    pl_classification="O",
                    original_amount=amount,
                    allocation_ratio=1.0,
                    source_file="pl_upload",
                    pl_period=period
                )
                db.add(cost)
                costs_created += 1
                print(f"   📦 Created O cost: {particulars} = ₹{amount:,.2f} (100% outsourced)")
                
            else:  # B - single pooled cost; allocate later by hybrid across all products
                cost_both = Cost(
                    name=particulars,
                    amount=amount,
                    applies_to="both",
                    cost_type="common",
                    basis="hybrid",  # allocate by hybrid (weight + value), alpha set in allocator
                    month="2025-04",
                    is_fixed="variable",
                    category="pl_import",
                    pl_classification="B",
                    original_amount=amount,
                    allocation_ratio=None,
                    source_file="pl_upload",
                    pl_period=period
                )
                db.add(cost_both)
                costs_created += 1
                print(f"   📦 Created B cost (single): {particulars} = ₹{amount:,.2f} (applies_to=both, basis=weight)")
        
        db.commit()
        
        print(f"✅ P&L parsing completed!")
        print(f"   📦 Costs created: {costs_created}")
        print(f"   📊 Period: {period}")
        print(f"   📈 Ratios: Inhouse {inhouse_ratio:.2%}, Outsourced {outsourced_ratio:.2%}")
        
        return {
            "success": True,
            "message": f"Successfully processed P&L with {costs_created} cost records",
            "costs_created": costs_created,
            "period": period,
            "ratios": {
                "inhouse": inhouse_ratio,
                "outsourced": outsourced_ratio
            },
            "data_rows": len(data_rows)
        }
        
    except Exception as e:
        print(f"💥 Error parsing P&L: {str(e)}")
        return {
            "success": False,
            "message": f"Error parsing P&L: {str(e)}",
            "costs_created": 0
        }

@app.post("/api/upload-pl")
async def upload_pl(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Upload and parse Purple Patch P&L Excel file"""
    
    print(f"🚀 Starting P&L upload for file: {file.filename}")
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return {
            "success": False,
            "message": "File must be an Excel file (.xlsx or .xls)",
            "costs_created": 0
        }
    
    try:
        # Save uploaded file temporarily
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_file_path = tmp_file.name
        
        # Parse P&L file
        result = parse_purple_patch_pl(tmp_file_path, db)
        
        # Clean up temp file
        os.unlink(tmp_file_path)
        
        return result
        
    except Exception as e:
        print(f"💥 P&L upload failed: {str(e)}")
        return {
            "success": False,
            "message": f"P&L upload failed: {str(e)}",
            "costs_created": 0
        }

@app.get("/api/wastage")
async def get_wastage_data(db: Session = Depends(get_db)):
    """Get wastage data for all products"""
    try:
        # Get all sales with wastage > 0
        wastage_sales = db.query(MonthlySale).filter(MonthlySale.wastage > 0).all()
        
        wastage_data = []
        for sale in wastage_sales:
            product = db.query(Product).filter(Product.id == sale.product_id).first()
            if product:
                wastage_percentage = (sale.wastage / sale.inward_quantity * 100) if sale.inward_quantity > 0 else 0
                
                wastage_data.append({
                    "id": sale.id,
                    "product_name": product.name,
                    "product_type": product.source,
                    "month": sale.month,
                    "inward_quantity": sale.inward_quantity,
                    "outward_quantity": sale.quantity,
                    "wastage_quantity": sale.wastage,
                    "wastage_percentage": round(wastage_percentage, 2),
                    "wastage_value": sale.wastage * sale.inward_rate,
                    "unit": product.unit
                })
        
        # Sort by wastage percentage descending
        wastage_data.sort(key=lambda x: x['wastage_percentage'], reverse=True)
        
        return {
            "success": True,
            "wastage_data": wastage_data,
            "total_wastage_items": len(wastage_data),
            "total_wastage_quantity": sum(item['wastage_quantity'] for item in wastage_data),
            "total_wastage_value": sum(item['wastage_value'] for item in wastage_data)
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Error fetching wastage data: {str(e)}",
            "wastage_data": []
        }

@app.get("/api/excel-preview", response_model=ExcelPreviewData)
async def get_excel_preview(month: str, db: Session = Depends(get_db)):
    """Get preview of parsed Excel data for a specific month"""
    
    # Get products and sales for the month
    products = db.query(Product).filter(Product.is_active == True).all()
    sales = db.query(MonthlySale).filter(MonthlySale.month == month).all()
    
    # Format products data
    products_data = []
    for product in products:
        products_data.append({
            "id": product.id,
            "name": product.name,
            "source": product.source,
            "unit": product.unit,
            "is_active": product.is_active
        })
    
    # Format sales data
    sales_data = []
    for sale in sales:
        sales_data.append({
            "id": sale.id,
            "product_id": sale.product_id,
            "product_name": sale.product.name,
            "month": sale.month,
            "quantity": sale.quantity,
            "sale_price": sale.sale_price,
            "direct_cost": sale.direct_cost,
            "inward_quantity": sale.inward_quantity,
            "inward_rate": sale.inward_rate,
            "inward_value": sale.inward_value,
            "inhouse_production": sale.inhouse_production,
            "wastage": sale.wastage
        })
    
    # Calculate summary
    total_products = len(products)
    total_sales = len(sales)
    total_revenue = sum(sale.quantity * sale.sale_price for sale in sales)
    total_inhouse_production = sum(sale.inhouse_production for sale in sales)
    total_wastage = sum(sale.wastage for sale in sales)
    
    summary = {
        "total_products": total_products,
        "total_sales": total_sales,
        "total_revenue": total_revenue,
        "total_inhouse_production": total_inhouse_production,
        "total_wastage": total_wastage
    }
    
    return ExcelPreviewData(
        products=products_data,
        sales=sales_data,
        summary=summary
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
