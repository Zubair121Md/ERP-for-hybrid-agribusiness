"""
Cost Allocation Engine
Applies cost allocation rules to distribute costs across products
"""
from typing import Dict, List, Any
import math


def identify_product_type(product_name: str) -> str:
    """Identify if product is inhouse or outsourced"""
    product_lower = product_name.lower()
    
    # Aggregation products are outsourced
    if 'aggregation' in product_lower:
        return 'outsourced'
    
    # Inhouse products
    inhouse_keywords = ['strawberry', 'raspberry', 'blueberry', 'greens', 'lettuce', 'open farm', 'open field']
    for keyword in inhouse_keywords:
        if keyword in product_lower:
            return 'inhouse'
    
    # Default to inhouse if not clear
    return 'inhouse'


def identify_product_category(product_name: str) -> str:
    """Identify product category for variable cost allocation"""
    product_lower = product_name.lower()
    
    if 'strawberry' in product_lower:
        return 'strawberry'
    elif 'greens' in product_lower or 'lettuce' in product_lower:
        return 'greens'
    elif 'raspberry' in product_lower or 'blueberry' in product_lower:
        return 'raspberry_blueberry'
    elif 'open' in product_lower or 'farm' in product_lower:
        return 'open_field'
    elif 'aggregation' in product_lower:
        return 'aggregation'
    
    return 'other'


def allocate_costs(
    expenses: Dict[str, Any],
    products: List[Dict[str, Any]],
    percentage_splits: Dict[str, float]
) -> Dict[str, Any]:
    """
    Apply cost allocation rules to distribute costs across products
    
    Allocation Rules:
    1. FIXED COST CAT - I: Proportional to ALL products based on sales_kg
    2. FIXED COST CAT - II: 
       - 60% to Strawberry (InHouse)
       - 25% to Greens (InHouse)
       - 15% to Aggregation (Outsourced)
    3. VARIABLE COST:
       - OPEN FIELD → Only inhouse open field products
       - LETTUCE → Only inhouse lettuce/greens
       - STRAWBERRY → Only inhouse strawberry
       - RASPBERRY & BLUEBERRY → Only inhouse
       - PACKING → Only inhouse
       - AGGREGATION → Only outsourced
    4. DISTRIBUTION, MARKETING, VEHICLE, OTHERS: Proportional based on sales_kg
    5. PURCHASE ACCOUNTS: 100% Outsourced only
    6. WASTAGE:
       - OWN FARM → InHouse
       - DISPATCH → Both proportional
       - FARM → InHouse
    """
    
    # Calculate total sales_kg for proportional allocation
    total_sales_kg = sum(p.get('sales_kg', 0) for p in products)
    total_sales_kg_inhouse = sum(
        p.get('sales_kg', 0) for p in products 
        if identify_product_type(p.get('product_name', '')) == 'inhouse'
    )
    total_sales_kg_outsourced = sum(
        p.get('sales_kg', 0) for p in products 
        if identify_product_type(p.get('product_name', '')) == 'outsourced'
    )
    
    # Initialize allocation results
    allocation_results = {
        'products': [],
        'category_totals': {
            'fixed_cost_cat_i': expenses.get('fixed_cost_cat_i', {}).get('total', 0.0),
            'fixed_cost_cat_ii': expenses.get('fixed_cost_cat_ii', {}).get('total', 0.0),
            'variable_cost': expenses.get('variable_cost', {}).get('total', 0.0),
            'distribution_cost': expenses.get('distribution_cost', {}).get('total', 0.0),
            'marketing_expenses': expenses.get('marketing_expenses', {}).get('total', 0.0),
            'vehicle_running_cost': expenses.get('vehicle_running_cost', {}).get('total', 0.0),
            'others': expenses.get('others', {}).get('total', 0.0),
            'wastage_shortage': expenses.get('wastage_shortage', {}).get('total', 0.0),
            'purchase_accounts': expenses.get('purchase_accounts', {}).get('total', 0.0)
        },
        'total_allocated_cost': 0.0
    }
    
    # Process each product
    for product in products:
        product_name = product.get('product_name', '')
        sales_kg = product.get('sales_kg', 0)
        product_type = identify_product_type(product_name)
        product_category = identify_product_category(product_name)
        
        product_allocation = {
            'product_name': product_name,
            'product_type': product_type,
            'product_category': product_category,
            'sales_kg': sales_kg,
            'allocated_costs': {
                'fixed_cost_cat_i': 0.0,
                'fixed_cost_cat_ii': 0.0,
                'variable_cost': 0.0,
                'distribution_cost': 0.0,
                'marketing_expenses': 0.0,
                'vehicle_running_cost': 0.0,
                'others': 0.0,
                'wastage_shortage': 0.0,
                'purchase_accounts': 0.0
            },
            'total_allocated_cost': 0.0,
            'cop': 0.0
        }
        
        # 1. FIXED COST CAT - I: Proportional to ALL products based on sales_kg
        if total_sales_kg > 0 and sales_kg > 0:
            fixed_cat_i_total = expenses.get('fixed_cost_cat_i', {}).get('total', 0.0)
            product_allocation['allocated_costs']['fixed_cost_cat_i'] = (
                fixed_cat_i_total * (sales_kg / total_sales_kg)
            )
        
        # 2. FIXED COST CAT - II: Based on percentage splits
        fixed_cat_ii_total = expenses.get('fixed_cost_cat_ii', {}).get('total', 0.0)
        if product_type == 'inhouse':
            if 'strawberry' in product_category:
                # 60% of FIXED COST CAT - II to Strawberry
                # Need to find total strawberry sales_kg
                strawberry_sales_kg = sum(
                    p.get('sales_kg', 0) for p in products 
                    if identify_product_category(p.get('product_name', '')) == 'strawberry'
                    and identify_product_type(p.get('product_name', '')) == 'inhouse'
                )
                if strawberry_sales_kg > 0 and sales_kg > 0:
                    strawberry_portion = fixed_cat_ii_total * percentage_splits.get('strawberry', 0.60)
                    product_allocation['allocated_costs']['fixed_cost_cat_ii'] = (
                        strawberry_portion * (sales_kg / strawberry_sales_kg)
                    )
            elif 'greens' in product_category:
                # 25% of FIXED COST CAT - II to Greens
                greens_sales_kg = sum(
                    p.get('sales_kg', 0) for p in products 
                    if identify_product_category(p.get('product_name', '')) == 'greens'
                    and identify_product_type(p.get('product_name', '')) == 'inhouse'
                )
                if greens_sales_kg > 0 and sales_kg > 0:
                    greens_portion = fixed_cat_ii_total * percentage_splits.get('greens', 0.25)
                    product_allocation['allocated_costs']['fixed_cost_cat_ii'] = (
                        greens_portion * (sales_kg / greens_sales_kg)
                    )
        elif product_type == 'outsourced' and 'aggregation' in product_category:
            # 15% of FIXED COST CAT - II to Aggregation
            aggregation_sales_kg = sum(
                p.get('sales_kg', 0) for p in products 
                if identify_product_category(p.get('product_name', '')) == 'aggregation'
                and identify_product_type(p.get('product_name', '')) == 'outsourced'
            )
            if aggregation_sales_kg > 0 and sales_kg > 0:
                aggregation_portion = fixed_cat_ii_total * percentage_splits.get('aggregation', 0.15)
                product_allocation['allocated_costs']['fixed_cost_cat_ii'] = (
                    aggregation_portion * (sales_kg / aggregation_sales_kg)
                )
        
        # 3. VARIABLE COST: Category-specific allocation
        # Note: Variable cost breakdown by subcategory would need to be extracted separately
        # For now, we'll allocate proportionally based on product category
        variable_cost_total = expenses.get('variable_cost', {}).get('total', 0.0)
        if product_type == 'inhouse':
            # Allocate variable cost to inhouse products proportionally
            if total_sales_kg_inhouse > 0 and sales_kg > 0:
                product_allocation['allocated_costs']['variable_cost'] = (
                    variable_cost_total * (sales_kg / total_sales_kg_inhouse)
                )
        elif product_type == 'outsourced' and 'aggregation' in product_category:
            # Aggregation variable cost only to outsourced
            if total_sales_kg_outsourced > 0 and sales_kg > 0:
                product_allocation['allocated_costs']['variable_cost'] = (
                    variable_cost_total * (sales_kg / total_sales_kg_outsourced)
                )
        
        # 4. DISTRIBUTION, MARKETING, VEHICLE, OTHERS: Proportional based on sales_kg
        if total_sales_kg > 0 and sales_kg > 0:
            distribution_total = expenses.get('distribution_cost', {}).get('total', 0.0)
            marketing_total = expenses.get('marketing_expenses', {}).get('total', 0.0)
            vehicle_total = expenses.get('vehicle_running_cost', {}).get('total', 0.0)
            others_total = expenses.get('others', {}).get('total', 0.0)
            
            product_allocation['allocated_costs']['distribution_cost'] = (
                distribution_total * (sales_kg / total_sales_kg)
            )
            product_allocation['allocated_costs']['marketing_expenses'] = (
                marketing_total * (sales_kg / total_sales_kg)
            )
            product_allocation['allocated_costs']['vehicle_running_cost'] = (
                vehicle_total * (sales_kg / total_sales_kg)
            )
            product_allocation['allocated_costs']['others'] = (
                others_total * (sales_kg / total_sales_kg)
            )
        
        # 5. PURCHASE ACCOUNTS: 100% Outsourced only
        if product_type == 'outsourced':
            purchase_total = expenses.get('purchase_accounts', {}).get('total', 0.0)
            if total_sales_kg_outsourced > 0 and sales_kg > 0:
                product_allocation['allocated_costs']['purchase_accounts'] = (
                    purchase_total * (sales_kg / total_sales_kg_outsourced)
                )
        
        # 6. WASTAGE: 
        # OWN FARM → InHouse, DISPATCH → Both proportional, FARM → InHouse
        wastage_total = expenses.get('wastage_shortage', {}).get('total', 0.0)
        if product_type == 'inhouse':
            # Allocate wastage to inhouse products
            if total_sales_kg_inhouse > 0 and sales_kg > 0:
                # Assuming wastage is split: 50% to inhouse (OWN FARM + FARM), 50% to dispatch (both)
                # For simplicity, allocate proportionally to inhouse
                product_allocation['allocated_costs']['wastage_shortage'] = (
                    wastage_total * 0.7 * (sales_kg / total_sales_kg_inhouse)  # 70% to inhouse
                )
        # Also allocate dispatch portion (30%) proportionally to all
        if total_sales_kg > 0 and sales_kg > 0:
            product_allocation['allocated_costs']['wastage_shortage'] += (
                wastage_total * 0.3 * (sales_kg / total_sales_kg)  # 30% to dispatch (all products)
            )
        
        # Calculate total allocated cost
        product_allocation['total_allocated_cost'] = sum(
            product_allocation['allocated_costs'].values()
        )
        
        # Calculate COP (Cost of Production) per kg
        if sales_kg > 0:
            product_allocation['cop'] = product_allocation['total_allocated_cost'] / sales_kg
        
        allocation_results['products'].append(product_allocation)
    
    # Calculate total allocated cost
    allocation_results['total_allocated_cost'] = sum(
        p['total_allocated_cost'] for p in allocation_results['products']
    )
    
    return allocation_results
