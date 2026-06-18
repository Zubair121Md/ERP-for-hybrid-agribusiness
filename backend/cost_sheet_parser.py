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
from typing import Dict, Any, List, Optional
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

        label_col, total_col = _detect_pl_columns(df)
        print(f"[PARSER] Column layout: label_col={label_col}, total_col={total_col}")

        # ===================================================================
        # STATE MACHINE: Walk through rows and assign to categories
        # ===================================================================
        current_category = None          # e.g. 'fixed_cost_cat_i'
        current_variable_sub = None      # e.g. 'open_field', 'lettuce'
        expenses_closed = False

        # Category totals
        expenses = {
            'fixed_cost_cat_i':     {'total': 0.0, 'items': []},
            # Defaults match classic 3-way sheet (60/25/15); open_field & % updated when rows say so.
            'fixed_cost_cat_ii':    {'total': 0.0, 'items': [], 'splits': {'strawberry': 0.60, 'greens': 0.25, 'open_field': 0.10, 'aggregation': 0.15}},
            'variable_cost':        {'total': 0.0, 'subcategories': {}},
            'distribution_cost':    {'total': 0.0, 'items': []},
            'marketing_expenses':   {'total': 0.0, 'items': []},
            'vehicle_running_cost': {'total': 0.0, 'items': []},
            'others':               {'total': 0.0, 'items': []},
            'wastage_shortage':     {'total': 0.0, 'items': []},
            'purchase_accounts':    {'total': 0.0, 'items': []},
        }

        total_expenses_from_sheet = 0.0

        for idx in range(len(df)):
            row = df.iloc[idx]
            label = _str(row, label_col).strip()
            label_upper = re.sub(r'\s+', ' ', label.upper())
            amounts = _scan_row_amounts(row, label_col, total_col)
            c_total = amounts["total_col_val"]
            line_amt = amounts["line_amt"]
            max_val = amounts["max_val"]
            total_val = amounts["total_val"]
            c2 = amounts.get("legacy_c2", 0.0)
            c3 = c_total
            c4 = amounts.get("legacy_c4", 0.0)
            # Legacy aliases for debug blocks
            c0 = _str(row, 0).strip()
            c1 = label
            c1_upper = label_upper
            
            # Debug: Log row 80 specifically
            if idx == 79:  # Row 80 (0-indexed)
                print(f"[PARSER DEBUG] Row 80: c0='{c0}', c1='{c1}', c2={c2}, c3={c3}, c4={c4}")
                print(f"[PARSER DEBUG] Row 80: current_category={current_category}")
                print(f"[PARSER DEBUG] Row 80: Condition check: c0={bool(c0)}, ')' in c0={')' in c0 if c0 else False}, ':' in c1={':' in c1 if c1 else False}")

            # Skip empty rows (allow subtotal rows with blank label but TOTAL value)
            if not label and total_val <= 0 and line_amt <= 0:
                if not c0 and not c1:
                    continue

            # FC-I / FC-II subtotal rows (blank particulars, total in TOTAL column)
            if (
                not label
                and not expenses_closed
                and c_total > 0
                and current_category in ('fixed_cost_cat_i', 'fixed_cost_cat_ii')
            ):
                expenses[current_category]['total'] = c_total
                print(f"[PARSER] Row {idx}: {current_category.upper()} SUBTOTAL = {c_total:,.2f}")
                continue

            # Subtotal row: blank label, value in TOTAL column while inside a variable sub
            if (
                not label
                and not expenses_closed
                and current_category == 'variable_cost'
                and current_variable_sub
                and c_total > 0
            ):
                sub_key = current_variable_sub
                expenses['variable_cost']['subcategories'].setdefault(sub_key, {'total': 0.0, 'items': []})
                old_total = expenses['variable_cost']['subcategories'][sub_key]['total']
                expenses['variable_cost']['total'] -= old_total
                expenses['variable_cost']['subcategories'][sub_key]['total'] = c_total
                expenses['variable_cost']['total'] += c_total
                print(f"[PARSER] Row {idx}: VARIABLE COST - {sub_key.upper()} SUBTOTAL = {c_total:,.2f}")
                current_variable_sub = None
                continue

            # ---- Detect category headers ----

            # 1) FIXED COST CAT - I (normalize spaces: "FIXED COST  CAT - I")
            if label and 'FIXED COST CAT' in label_upper and 'II' not in label_upper and '-II' not in label_upper:
                current_category = 'fixed_cost_cat_i'
                current_variable_sub = None
                # Don't set total here - let it be calculated from line items
                # The category total row might have incorrect values
                # We'll sum the line items instead
                print(f"[PARSER] Row {idx}: FIXED COST CAT - I header found (will sum line items)")
                continue

            # 2) FIXED COST CAT - II
            if label and 'FIXED COST CAT' in label_upper and ('II' in label_upper or '-2' in label_upper):
                current_category = 'fixed_cost_cat_ii'
                current_variable_sub = None
                if total_val > 0:
                    expenses['fixed_cost_cat_ii']['total'] = total_val
                    print(f"[PARSER] Row {idx}: FIXED COST CAT - II = {total_val}")
                continue

            # 3) VARIABLE COST (header, no total on this row)
            if not expenses_closed and (
                label_upper == 'VARIABLE COST' or (label_upper.startswith('VARIABLE COST') and len(label_upper) < 30)
            ):
                current_category = 'variable_cost'
                current_variable_sub = None
                continue

            # Variable cost section headers: OPEN FIELD :, LETTUCE:, A) OPEN FIELD:, AGGREGATION, etc.
            is_var_section_header = False
            var_section_sub = None
            if not expenses_closed and label:
                var_section_sub = _detect_var_subcategory(label_upper)
                if var_section_sub:
                    is_var_section_header = True
                    current_category = 'variable_cost'
                    print(f"[PARSER] Row {idx}: Detected variable cost section header: {var_section_sub}")

            # Variable cost sub-category processing
            if not expenses_closed and current_category == 'variable_cost':
                detected_sub = None
                is_section_header = False
                
                if is_var_section_header and var_section_sub:
                    detected_sub = var_section_sub
                    is_section_header = True
                elif label:
                    bare = re.sub(r'\s*:\s*$', '', label_upper.strip())
                    if bare in VAR_SUB_MAP and total_val > 0:
                        detected_sub = VAR_SUB_MAP[bare]
                        is_section_header = True
                    elif current_variable_sub is None:
                        detected_sub = _detect_var_subcategory(label_upper)

                if detected_sub:
                    current_variable_sub = detected_sub
                    if detected_sub not in expenses['variable_cost']['subcategories']:
                        expenses['variable_cost']['subcategories'][detected_sub] = {'total': 0.0, 'items': []}
                    
                    is_total_row = 'TOTAL' in label_upper
                    if total_val > 0:
                        current_sub_total = expenses['variable_cost']['subcategories'][detected_sub]['total']
                        if is_section_header or is_total_row:
                            header_val = total_val
                        elif total_val > current_sub_total + 0.01:
                            header_val = total_val
                        else:
                            header_val = 0.0
                        if header_val > 0:
                            expenses['variable_cost']['total'] -= current_sub_total
                            expenses['variable_cost']['subcategories'][detected_sub]['total'] = header_val
                            expenses['variable_cost']['total'] += header_val
                            print(f"[PARSER] Row {idx}: VARIABLE COST - {detected_sub.upper()} = {header_val:,.2f} (was {current_sub_total:,.2f})")
                    if is_section_header and total_val > 0:
                        current_variable_sub = None
                    continue

                # Line within current section that carries the section total (TOTAL column on rollup row)
                if current_variable_sub and label and not _detect_var_subcategory(label_upper):
                    rollup_val = c_total if c_total > 0 else 0.0
                    if rollup_val <= 0:
                        pass
                    else:
                        sub_key = current_variable_sub
                        expenses['variable_cost']['subcategories'].setdefault(sub_key, {'total': 0.0, 'items': []})
                        current_sub_total = expenses['variable_cost']['subcategories'][sub_key]['total']
                        if rollup_val > current_sub_total + 0.01:
                            expenses['variable_cost']['total'] -= current_sub_total
                            expenses['variable_cost']['subcategories'][sub_key]['total'] = rollup_val
                            expenses['variable_cost']['total'] += rollup_val
                            print(f"[PARSER] Row {idx}: VARIABLE COST - {sub_key.upper()} section total = {rollup_val:,.2f}")
                            current_variable_sub = None

            # FC-I / FC-II running total from TOTAL column on line rows
            if (
                not expenses_closed
                and current_category in ('fixed_cost_cat_i', 'fixed_cost_cat_ii')
                and c_total > expenses[current_category]['total']
            ):
                expenses[current_category]['total'] = c_total

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

            # 7) OTHERS — top-level category only (not line items under variable cost sections)
            if c1_upper == 'OTHERS' or c1_upper == 'OTHER COSTS':
                if current_category == 'variable_cost' and current_variable_sub:
                    pass
                else:
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
                potential_total = _num(row, label_col + 1) if label_col + 1 < len(row) else 0
                if potential_total <= 0:
                    potential_total = total_val if total_val > 0 else c2
                if potential_total > 0:
                    total_expenses_from_sheet = potential_total
                    print(f"[PARSER] Row {idx}: TOTAL EXPENSES = {total_expenses_from_sheet:,.2f}")
                current_category = None
                current_variable_sub = None
                expenses_closed = True
                continue

            # INCOME section — stop collecting expenses
            if c0.upper() == 'INCOME' or c1_upper == 'INCOME':
                current_category = None
                current_variable_sub = None
                expenses_closed = True
                continue

            # ---- Collect line items under current category ----
            is_total_row = 'TOTAL' in label_upper if label else False
            
            if current_category and label and line_amt > 0 and not is_total_row and not expenses_closed:
                item = {'name': label, 'amount': line_amt}

                if current_category == 'variable_cost' and current_variable_sub:
                    if current_variable_sub not in expenses['variable_cost']['subcategories']:
                        expenses['variable_cost']['subcategories'][current_variable_sub] = {'total': 0.0, 'items': []}
                    # Only add as line item if it's not the total (smaller than current total)
                    current_sub_total = expenses['variable_cost']['subcategories'][current_variable_sub]['total']
                    if line_amt < current_sub_total * 0.9 or current_sub_total == 0:
                        expenses['variable_cost']['subcategories'][current_variable_sub]['items'].append(item)
                elif current_category in expenses and 'items' in expenses[current_category]:
                    expenses[current_category]['items'].append(item)

        # FC-I subtotal row (blank label, total in TOTAL column)
        for idx in range(len(df)):
            row = df.iloc[idx]
            label = _str(row, label_col).strip()
            if label:
                continue
            amounts = _scan_row_amounts(row, label_col, total_col)
            if amounts["total_val"] > 0 and expenses['fixed_cost_cat_i']['total'] == 0:
                if expenses['fixed_cost_cat_i']['items']:
                    items_sum = sum(i['amount'] for i in expenses['fixed_cost_cat_i']['items'])
                    if abs(items_sum - amounts["total_val"]) / max(amounts["total_val"], 1) < 0.05:
                        expenses['fixed_cost_cat_i']['total'] = amounts["total_val"]

        # Recalculate variable_cost total from subcategories
        expenses['variable_cost']['total'] = sum(
            v.get('total', 0.0) for v in expenses['variable_cost']['subcategories'].values()
        )

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

        # ---- Percentage splits for Fixed Cost Cat II (read from sheet; defaults 60/25/15 + open_field 0) ----
        for idx in range(len(df)):
            row = df.iloc[idx]
            c4 = _str(row, 4).strip().upper()
            if 'STRAWBERRY' in c4 and '50' in c4:
                expenses['fixed_cost_cat_ii']['splits']['strawberry'] = 0.50
            elif 'STRAWBERRY' in c4 and '60' in c4:
                expenses['fixed_cost_cat_ii']['splits']['strawberry'] = 0.60
            elif 'GREENS' in c4 and '25' in c4:
                expenses['fixed_cost_cat_ii']['splits']['greens'] = 0.25
            elif 'OPEN FIELD' in c4 and '10' in c4:
                expenses['fixed_cost_cat_ii']['splits']['open_field'] = 0.10
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


def _detect_pl_columns(df: pd.DataFrame) -> tuple:
    """Detect which column holds particulars labels and which holds section TOTAL."""
    label_col = 1
    total_col = 3
    for idx in range(min(20, len(df))):
        row = df.iloc[idx]
        for ci in range(min(8, len(row))):
            cell = _str(row, ci).strip().upper()
            if cell == 'PARTICULARS':
                label_col = ci
            if cell == 'TOTAL':
                total_col = ci
    return label_col, total_col


VAR_SUB_MAP = {
    'OPEN FIELD': 'open_field',
    'LETTUCE': 'lettuce',
    'STRAWBERRY': 'strawberry',
    'RASPBERRY': 'raspberry_blueberry',
    'BLUBERRY': 'raspberry_blueberry',
    'BLUEBERRY': 'raspberry_blueberry',
    'RASPBERRY&BLUBERRY': 'raspberry_blueberry',
    'RASPBERRY & BLUEBERRY': 'raspberry_blueberry',
    'CITRUS': 'citrus',
    'PACKING': 'packing',
    'AGGREGATION': 'aggregation',
    'COMMON EXPENSES -FARM': 'common_expenses_farm',
    'COMMON EXPENSES - FARM': 'common_expenses_farm',
    'COMMON EXPENSES FARM': 'common_expenses_farm',
}


def _detect_var_subcategory(label_upper: str) -> Optional[str]:
    """Match section header rows only (OPEN FIELD :, LETTUCE:, AGGREGATION, etc.)."""
    if not label_upper:
        return None
    lu = re.sub(r'\s*:\s*$', '', label_upper.strip())
    lu = re.sub(r'^[A-Z]\)\s*', '', lu).strip()
    if not lu:
        return None

    exact_sections = [
        ('OPEN FIELD', 'open_field'),
        ('LETTUCE', 'lettuce'),
        ('STRAWBERRY', 'strawberry'),
        ('RASPBERRY&BLUBERRY', 'raspberry_blueberry'),
        ('RASPBERRY & BLUEBERRY', 'raspberry_blueberry'),
        ('RASPBERRY AND BLUEBERRY', 'raspberry_blueberry'),
        ('CITRUS', 'citrus'),
        ('PACKING', 'packing'),
        ('AGGREGATION', 'aggregation'),
        ('COMMON EXPENSES -FARM', 'common_expenses_farm'),
        ('COMMON EXPENSES - FARM', 'common_expenses_farm'),
        ('COMMON EXPENSES FARM', 'common_expenses_farm'),
    ]
    for pattern, sub_key in exact_sections:
        if lu == pattern:
            return sub_key
        if pattern == 'PACKING' and lu.startswith('PACKING') and lu != 'PACKING':
            continue
        if lu.startswith(pattern) and len(lu) <= len(pattern) + 2:
            return sub_key
    return None


def _scan_row_amounts(row, label_col: int, total_col: int) -> Dict[str, float]:
    """Collect TOTAL column value, line amount, and max numeric in row."""
    total_col_val = _num(row, total_col)
    nums: List[float] = []
    line_amt = 0.0
    for ci in range(len(row)):
        if ci == label_col:
            continue
        v = _num(row, ci)
        if v > 0:
            nums.append(v)
    max_val = max(nums) if nums else 0.0
    total_val = total_col_val if total_col_val > 0 else max_val
    for ci in range(label_col + 1, len(row)):
        if ci == total_col:
            continue
        v = _num(row, ci)
        if v > 0:
            line_amt = v
            break
    if line_amt <= 0 and label_col + 1 < len(row) and (label_col + 1) != total_col:
        line_amt = _num(row, label_col + 1)
    legacy_c2 = _num(row, 2)
    legacy_c4 = _num(row, 4)
    return {
        "total_col_val": total_col_val,
        "line_amt": line_amt,
        "max_val": max_val,
        "total_val": total_val,
        "legacy_c2": legacy_c2,
        "legacy_c4": legacy_c4,
    }


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
