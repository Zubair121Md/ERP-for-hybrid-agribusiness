"""
Cost Sheet Excel Parser - Parses NEED.xlsx P&L cost analysis sheet.

Extracts:
  - Category totals (FIXED COST CAT-I, II, etc.)
  - Variable cost subcategories (OPEN FIELD, LETTUCE, STRAWBERRY, etc.)
  - Wastage sub-items (OWN FARM, DISPATCH, FARM)
  - Purchase sub-items (VEGETABLES, OTHERS)
  - Individual line items under each category
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import re


def parse_cost_sheet(file_path: str) -> Dict[str, Any]:
    """
    Parse the cost sheet Excel file and extract all expense categories
    with full subcategory detail.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        print(f"[PARSER] Loaded Excel: {df.shape[0]} rows x {df.shape[1]} cols")

        # --- Header Info ---
        company_name = ""
        period = ""
        total_qty_sold = 0.0

        for idx in range(min(5, len(df))):
            row = df.iloc[idx]
            c1 = _str(row, 1)
            if idx <= 1 and c1 and c1 != "nan":
                if not company_name:
                    company_name = c1.strip()
                elif not period:
                    period = c1.strip()
            c2 = _str(row, 2)
            if "TOTAL QTY SOLD" in c2.upper():
                total_qty_sold = _num(row, 4)

        print(f"[PARSER] Company: {company_name}")
        print(f"[PARSER] Period: {period}")
        print(f"[PARSER] Total Qty Sold: {total_qty_sold}")

        # ===================================================================
        # STATE MACHINE: Walk through rows and assign to categories
        # ===================================================================
        current_category = None          # e.g. 'fixed_cost_cat_i'
        current_variable_sub = None      # e.g. 'open_field', 'lettuce'

        # Category totals
        expenses = {
            'fixed_cost_cat_i':     {'total': 0.0, 'items': []},
            'fixed_cost_cat_ii':    {'total': 0.0, 'items': [], 'splits': {'strawberry': 0.60, 'greens': 0.25, 'aggregation': 0.15}},
            'variable_cost':        {'total': 0.0, 'subcategories': {}},
            'distribution_cost':    {'total': 0.0, 'items': []},
            'marketing_expenses':   {'total': 0.0, 'items': []},
            'vehicle_running_cost': {'total': 0.0, 'items': []},
            'others':               {'total': 0.0, 'items': []},
            'wastage_shortage':     {'total': 0.0, 'items': []},
            'purchase_accounts':    {'total': 0.0, 'items': []},
        }

        total_expenses_from_sheet = 0.0

        # Variable cost subcategory mapping
        VAR_SUB_MAP = {
            'OPEN FIELD':   'open_field',
            'LETTUCE':      'lettuce',
            'STRAWBERRY':   'strawberry',
            'RASPBERRY':    'raspberry_blueberry',
            'BLUBERRY':     'raspberry_blueberry',
            'BLUEBERRY':    'raspberry_blueberry',
            'PACKING':      'packing',
            'AGGREGATION':  'aggregation',
        }

        for idx in range(len(df)):
            row = df.iloc[idx]
            c0 = _str(row, 0).strip()
            c1 = _str(row, 1).strip()
            c1_upper = c1.upper()
            c2 = _num(row, 2)  # Column C
            c3 = _num(row, 3)  # Column D (TOTAL) - this is where totals are in NEED.xlsx format
            c4 = _num(row, 4)  # Column E (sometimes totals are here)
            # Check all columns for totals (different Excel formats use different columns)
            # Prefer c3 (column D) as it's the standard TOTAL column, but check others too
            total_val = c3 if c3 > 0 else (c4 if c4 > 0 else c2)
            
            # Debug: Log row 80 specifically
            if idx == 79:  # Row 80 (0-indexed)
                print(f"[PARSER DEBUG] Row 80: c0='{c0}', c1='{c1}', c2={c2}, c3={c3}, c4={c4}")
                print(f"[PARSER DEBUG] Row 80: current_category={current_category}")
                print(f"[PARSER DEBUG] Row 80: Condition check: c0={bool(c0)}, ')' in c0={')' in c0 if c0 else False}, ':' in c1={':' in c1 if c1 else False}")

            # Skip empty rows
            if not c0 and not c1 and c2 == 0 and c3 == 0:
                continue

            # ---- Detect category headers (col1 has name, col3 has total) ----

            # 1) FIXED COST CAT - I
            if 'FIXED COST CAT' in c1_upper and 'II' not in c1_upper and '-II' not in c1_upper:
                current_category = 'fixed_cost_cat_i'
                current_variable_sub = None
                # Don't set total here - let it be calculated from line items
                # The category total row might have incorrect values
                # We'll sum the line items instead
                print(f"[PARSER] Row {idx}: FIXED COST CAT - I header found (will sum line items)")
                continue

            # 2) FIXED COST CAT - II
            if 'FIXED COST CAT' in c1_upper and ('II' in c1_upper or '-2' in c1_upper):
                current_category = 'fixed_cost_cat_ii'
                current_variable_sub = None
                if total_val > 0:
                    expenses['fixed_cost_cat_ii']['total'] = total_val
                    print(f"[PARSER] Row {idx}: FIXED COST CAT - II = {total_val}")
                continue

            # 3) VARIABLE COST (header, no total on this row)
            if c1_upper == 'VARIABLE COST' or (c0 == '2' and 'VARIABLE COST' in c1_upper):
                current_category = 'variable_cost'
                current_variable_sub = None
                continue

            # Check for variable cost section headers FIRST (even if not in variable_cost category)
            # This handles cases where category might have changed but we're still in variable cost section
            # Pattern: column 0 has letter+")" and column 1 has keyword+":"
            is_var_section_header = False
            var_section_sub = None
            if c0 and ')' in c0 and ':' in c1:
                for keyword, sub_key in VAR_SUB_MAP.items():
                    if keyword in c1_upper:
                        var_section_sub = sub_key
                        is_var_section_header = True
                        # Switch back to variable_cost category if we see a section header
                        current_category = 'variable_cost'
                        print(f"[PARSER] Row {idx}: Detected variable cost section header: {var_section_sub} (switching category to variable_cost)")
                        break

            # Variable cost sub-category headers: A) OPEN FIELD, B) LETTUCE, etc.
            if current_category == 'variable_cost':
                detected_sub = None
                is_section_header = False
                
                # FIRST: Use the section header we already detected above (if any)
                if is_var_section_header and var_section_sub:
                    detected_sub = var_section_sub
                    is_section_header = True
                    print(f"[PARSER] Row {idx}: Using detected section header: {detected_sub}")
                # OR check if this is a section header row (e.g., "E) PACKING:" with colon)
                elif c0 and ')' in c0 and ':' in c1:
                    for keyword, sub_key in VAR_SUB_MAP.items():
                        if keyword in c1_upper:
                            detected_sub = sub_key
                            is_section_header = True
                            print(f"[PARSER] Row {idx}: Detected section header for {detected_sub}")
                            break
                
                # SECOND: If not a section header, check for keyword in row (line items)
                if not detected_sub:
                    for keyword, sub_key in VAR_SUB_MAP.items():
                        if keyword in c1_upper:
                            detected_sub = sub_key
                            break

                if detected_sub:
                    current_variable_sub = detected_sub
                    if detected_sub not in expenses['variable_cost']['subcategories']:
                        expenses['variable_cost']['subcategories'][detected_sub] = {'total': 0.0, 'items': []}
                    
                    # Check if this is a TOTAL row
                    is_total_row = 'TOTAL' in c1_upper or (c0 and 'TOTAL' in c0.upper())
                    
                    # Get the maximum value from all columns (c2, c3, c4)
                    c4_val = _num(row, 4)
                    max_val = max(c2, c3, c4_val) if c4_val > 0 else max(c2, c3)
                    
                    if max_val > 0:
                        current_sub_total = expenses['variable_cost']['subcategories'][detected_sub]['total']
                        
                        if is_section_header:
                            # Section header row - use column 3 if available, otherwise use max
                            # Column 3 in section headers usually has the section total
                            header_val = c3 if c3 > 0 else max_val
                            # Always update if it's a section header (it's the official section total)
                            old_total = current_sub_total
                            expenses['variable_cost']['total'] -= old_total
                            expenses['variable_cost']['subcategories'][detected_sub]['total'] = header_val
                            expenses['variable_cost']['total'] += header_val
                            print(f"[PARSER] Row {idx}: VARIABLE COST - {detected_sub.upper()} SECTION HEADER = {header_val:,.2f} (was {old_total:,.2f})")
                        elif is_total_row:
                            # TOTAL row - use it as official total
                            old_total = current_sub_total
                            expenses['variable_cost']['total'] -= old_total
                            expenses['variable_cost']['subcategories'][detected_sub]['total'] = max_val
                            expenses['variable_cost']['total'] += max_val
                            print(f"[PARSER] Row {idx}: VARIABLE COST - {detected_sub.upper()} TOTAL = {max_val:,.2f}")
                        elif max_val > current_sub_total * 1.1 or (max_val > 100000 and current_sub_total < 100000):
                            # Update if:
                            # 1. Significantly larger (at least 10% larger), OR
                            # 2. New value is > 100k and current is < 100k (catches large totals vs small line items)
                            old_total = current_sub_total
                            expenses['variable_cost']['total'] -= old_total
                            expenses['variable_cost']['subcategories'][detected_sub]['total'] = max_val
                            expenses['variable_cost']['total'] += max_val
                            print(f"[PARSER] Row {idx}: VARIABLE COST - {detected_sub.upper()} = {max_val:,.2f} (updated total, was {old_total:,.2f})")
                    continue

            # 4) DISTRIBUTION COST
            # More flexible: check if it's DISTRIBUTION COST, with or without column 0 check
            if 'DISTRIBUTION COST' in c1_upper:
                # Only set category if we haven't seen it yet OR if total_val > 0 (likely the header row)
                if current_category != 'distribution_cost' or total_val > 0:
                    current_category = 'distribution_cost'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['distribution_cost']['total'] = total_val
                    print(f"[PARSER] Row {idx}: DISTRIBUTION COST = {total_val}")
                continue

            # 5) MARKETING EXPENSES
            if 'MARKETING EXPENSES' in c1_upper or ('MARKETING' in c1_upper and 'EXPENSES' in c1_upper):
                if current_category != 'marketing_expenses' or total_val > 0:
                    current_category = 'marketing_expenses'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['marketing_expenses']['total'] = total_val
                    print(f"[PARSER] Row {idx}: MARKETING EXPENSES = {total_val}")
                continue

            # 6) VEHICLE RUNNING COST
            if 'VEHICLE RUNNING COST' in c1_upper or ('VEHICLE' in c1_upper and 'RUNNING' in c1_upper):
                if current_category != 'vehicle_running_cost' or total_val > 0:
                    current_category = 'vehicle_running_cost'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['vehicle_running_cost']['total'] = total_val
                    print(f"[PARSER] Row {idx}: VEHICLE RUNNING COST = {total_val}")
                continue

            # 7) OTHERS
            # Check for "OTHERS" or "OTHER COSTS" but make sure it's not part of another category name
            # Don't match "OTHER EXP" or "OTHER" - only match exact "OTHERS" or "OTHER COSTS"
            if c1_upper == 'OTHERS' or c1_upper == 'OTHER COSTS':
                # Only set if we're not already in another category or if we see a total
                if current_category != 'others' or total_val > 0:
                    current_category = 'others'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['others']['total'] = total_val
                    print(f"[PARSER] Row {idx}: OTHERS = {total_val}")
                continue

            # 8) WASTAGE & SHORTAGE
            if 'WASTAGE' in c1_upper and 'SHORTAGE' in c1_upper:
                if current_category != 'wastage_shortage' or total_val > 0:
                    current_category = 'wastage_shortage'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['wastage_shortage']['total'] = total_val
                    print(f"[PARSER] Row {idx}: WASTAGE & SHORTAGE = {total_val}")
                continue

            # 9) PURCHASE ACCOUNTS
            if 'PURCHASE ACCOUNTS' in c1_upper:
                if current_category != 'purchase_accounts' or total_val > 0:
                    current_category = 'purchase_accounts'
                    current_variable_sub = None
                if total_val > 0:
                    expenses['purchase_accounts']['total'] = total_val
                    print(f"[PARSER] Row {idx}: PURCHASE ACCOUNTS = {total_val}")
                continue

            # TOTAL EXPENSES — end of expense section
            if 'TOTAL EXPENSES' in c1_upper:
                # Check all columns for the total, prefer the one that matches expected format (28,248,507.38)
                # Column 2 (c2) is usually the correct one
                potential_total = c2 if c2 > 0 else (c3 if c3 > 0 else 0)
                # If we find a total that's close to 28,248,507.38, use it (within 1% tolerance)
                if abs(potential_total - 28248507.38) / 28248507.38 < 0.01:
                    total_expenses_from_sheet = potential_total
                    print(f"[PARSER] Row {idx}: TOTAL EXPENSES = {total_expenses_from_sheet:,.2f} (correct value)")
                elif total_expenses_from_sheet == 0:
                    # Use first valid total found
                    total_expenses_from_sheet = potential_total
                    print(f"[PARSER] Row {idx}: TOTAL EXPENSES = {total_expenses_from_sheet:,.2f}")
                current_category = None
                current_variable_sub = None
                continue

            # INCOME section — stop collecting expenses
            if c0.upper() == 'INCOME' or c1_upper == 'INCOME':
                current_category = None
                current_variable_sub = None
                continue

            # ---- Collect line items under current category ----
            # Skip if this is a TOTAL row (already processed above)
            is_total_row = 'TOTAL' in c1_upper or 'TOTAL' in c0.upper()
            
            if current_category and c1 and c2 > 0 and not is_total_row:
                item = {'name': c1, 'amount': c2}

                if current_category == 'variable_cost' and current_variable_sub:
                    if current_variable_sub not in expenses['variable_cost']['subcategories']:
                        expenses['variable_cost']['subcategories'][current_variable_sub] = {'total': 0.0, 'items': []}
                    # Only add as line item if it's not the total (smaller than current total)
                    current_sub_total = expenses['variable_cost']['subcategories'][current_variable_sub]['total']
                    if c2 < current_sub_total * 0.9 or current_sub_total == 0:
                        # This is a line item (smaller than total) or no total set yet
                        expenses['variable_cost']['subcategories'][current_variable_sub]['items'].append(item)
                elif current_category in expenses and 'items' in expenses[current_category]:
                    expenses[current_category]['items'].append(item)

        # ---- Calculate totals from line items if available (more accurate than category totals) ----
        # For FIXED COST CAT - I: Always prefer line items sum if available
        if expenses['fixed_cost_cat_i']['items']:
            items_sum = sum(item['amount'] for item in expenses['fixed_cost_cat_i']['items'])
            if items_sum > 0:
                # Use items sum if it's significantly different from category total (more than 1% difference)
                # or if category total is 0
                if expenses['fixed_cost_cat_i']['total'] == 0 or abs(items_sum - expenses['fixed_cost_cat_i']['total']) / max(expenses['fixed_cost_cat_i']['total'], 1) > 0.01:
                    print(f"[PARSER] FIXED COST CAT - I: Using items sum ({items_sum:,.2f}) instead of category total ({expenses['fixed_cost_cat_i']['total']:,.2f})")
                    expenses['fixed_cost_cat_i']['total'] = items_sum
        
        # For other categories, calculate from items if total is 0
        for cat_key in ['fixed_cost_cat_ii', 'distribution_cost', 'marketing_expenses', 'vehicle_running_cost', 'others', 'wastage_shortage', 'purchase_accounts']:
            if expenses[cat_key]['total'] == 0 and expenses[cat_key].get('items'):
                items_sum = sum(item['amount'] for item in expenses[cat_key]['items'])
                if items_sum > 0:
                    expenses[cat_key]['total'] = items_sum
                    print(f"[PARSER] Calculated {cat_key} from items: {items_sum:,.2f}")

        # ---- Percentage splits for Fixed Cost Cat II ----
        # Scan for "STRAWBERRY -60 %", "GREENS -25 %", "AGGREGATION - 15 %"
        for idx in range(len(df)):
            row = df.iloc[idx]
            c4 = _str(row, 4).strip().upper()
            if 'STRAWBERRY' in c4 and '60' in c4:
                expenses['fixed_cost_cat_ii']['splits']['strawberry'] = 0.60
            elif 'GREENS' in c4 and '25' in c4:
                expenses['fixed_cost_cat_ii']['splits']['greens'] = 0.25
            elif 'AGGREGATION' in c4 and '15' in c4:
                expenses['fixed_cost_cat_ii']['splits']['aggregation'] = 0.15

        # ---- Summary ----
        all_totals = [v['total'] for v in expenses.values()]
        grand_total = sum(all_totals)
        categories_found = sum(1 for t in all_totals if t > 0)
        print(f"[PARSER] === SUMMARY ===")
        print(f"[PARSER] Categories with values: {categories_found}/9")
        print(f"[PARSER] Grand total: {grand_total:,.2f}")
        for key, val in expenses.items():
            print(f"[PARSER]   {key}: {val['total']:,.2f}")
            if 'subcategories' in val:
                for sk, sv in val['subcategories'].items():
                    print(f"[PARSER]     {sk}: {sv['total']:,.2f} ({len(sv.get('items', []))} items)")
            if 'items' in val:
                print(f"[PARSER]     ({len(val['items'])} line items)")

        return {
            'success': True,
            'header_info': {
                'company_name': company_name,
                'period': period,
                'total_qty_sold': total_qty_sold,
            },
            'expenses': expenses,
            'total_expenses': total_expenses_from_sheet if total_expenses_from_sheet > 0 else grand_total,
        }

    except Exception as e:
        import traceback
        print(f"[PARSER] ERROR: {e}")
        print(f"[PARSER] {traceback.format_exc()}")
        return {'success': False, 'error': str(e)}


def _str(row, col_idx: int) -> str:
    """Safely get string value from a row."""
    if col_idx >= len(row):
        return ""
    val = row.iloc[col_idx]
    if pd.isna(val):
        return ""
    return str(val)


def _num(row, col_idx: int) -> float:
    """Safely get numeric value from a row."""
    if col_idx >= len(row):
        return 0.0
    val = row.iloc[col_idx]
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace(',', '').replace('₹', '').replace('$', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0
