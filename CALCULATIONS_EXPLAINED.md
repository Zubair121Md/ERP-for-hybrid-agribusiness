# ERP Calculations Explained

This document explains the core calculations used in this repository and how each value is derived from your monthly sheet format.

It is aligned to the project logic and documentation in:
- `backend/app.py`
- `backend/cost_sheet_parser.py`
- `backend/COST_ALLOCATION.md`

---

## 1) Row-Level Sales Sheet Calculations

Your input format (example columns):

`Month | Particulars | Type | Inward Quantity | Inward Eff. Rate | Inward Value | Inward Quantity (OS) | Inward Value (OS) | Outward Quantity | Outward Eff. Rate | Outward Value`

### Formulas

- `Inward Value = Inward Quantity x Inward Eff. Rate`
- `Inward Value (OS) = Inward Quantity (OS) x Inward Eff. Rate` (when OS quantity is present)
- `Outward Value = Outward Quantity x Outward Eff. Rate`
- `Outward Eff. Rate = Outward Value / Outward Quantity` (if rate is not given explicitly)
- `Inward Eff. Rate = Inward Value / Inward Quantity` (if rate is not given explicitly)

### Worked examples from your sample

#### Example A: Arugula (Rocket Lettuce)

Input row:
- Month: `26_FEB`
- Type: `both`
- Inward Quantity: `41.5`
- Inward Eff. Rate: `150`
- Inward Quantity (OS): `36.2`
- Outward Quantity: `31.6`
- Outward Value: `10569.8`

Calculation:
- `Inward Value = 41.5 x 150 = 6225`
- `Inward Value (OS) = 36.2 x 150 = 5430`
- `Outward Eff. Rate = 10569.8 / 31.6 = 334.49`

Final row values match:
- Inward Value = `6225`
- Inward Value (OS) = `5430`
- Outward Eff. Rate = `334.49`
- Outward Value = `10569.8`

#### Example B: Asparagus

Input row:
- Type: `Outsourced`
- Inward Quantity: `17`
- Inward Eff. Rate: `230`
- Outward Quantity: `12.95`
- Outward Value: `7243`

Calculation:
- `Inward Value = 17 x 230 = 3910`
- `Outward Eff. Rate = 7243 / 12.95 = 559.31`

Final row values match:
- Inward Value = `3910`
- Outward Eff. Rate = `559.31`
- Outward Value = `7243`

#### Example C: Baby Bokchoy (Inhouse only)

Input row has no inward purchase values and only outward sale:
- Outward Quantity: `4.2`
- Outward Eff. Rate: `1000`

Calculation:
- `Outward Value = 4.2 x 1000 = 4200`

This is expected for inhouse lines where direct inward purchase may be blank.

---

## 2) Product Classification Behavior (`Type` column)

- `Inhouse`: grown internally; direct purchase value may be blank/zero.
- `Outsourced`: purchased from vendors; inward purchase values are the direct cost base.
- `Both`: split into inhouse and outsourced portions in system allocation logic (using harvest/production context when available).

---

## 3) Cost Allocation Engine (Overhead Distribution)

After monthly sales and P&L costs are uploaded, the engine distributes category-level costs across products.

High-level allocation rule (proportional allocation):

`Product Allocation = (Product Basis / Total Basis of Applicable Products) x Category Amount`

Where basis depends on category (sales value, sales kg, production kg, handled kg, etc.).

### Main category mapping used in this project

- `FIXED COST CAT - I`: allocated by **Sales Value**
- `FIXED COST CAT - II`: split buckets (Strawberry / Greens / Open Field / Aggregation) then proportional inside each bucket
- `VARIABLE COST` subcategories:
  - Open Field, Lettuce/Polyhouse, Strawberry, Berries: production-based for relevant inhouse set
  - Packing: handled quantity based
  - Aggregation: outsourced purchase quantity based
- `DISTRIBUTION COST`: sales quantity based
- `MARKETING EXPENSES`: sales value based
- `VEHICLE RUNNING COST`: handled quantity based
- `OTHERS`: sales quantity based
- `WASTAGE` components:
  - own farm/farm: inhouse production based
  - dispatch: sales quantity based
- `PURCHASE ACCOUNTS`: **not overhead allocated**; treated as direct purchase cost for outsourced products

---

## 4) Profitability Calculations

Per product:

- `Revenue = Outward Value`
- `Direct Cost = Inward Value (mainly outsourced purchase cost)`
- `Allocated Overheads = Sum of all allocated category amounts`
- `Total Cost = Direct Cost + Allocated Overheads`
- `Net Profit = Revenue - Total Cost`
- `Margin % = (Net Profit / Revenue) x 100`

---

## 5) Quick End-to-End Example (Using Arugula row)

From row:
- Revenue = `10569.8`
- Direct inward purchase value = `6225` (or OS part `5430`, depending on split handling)

Then system adds allocated overhead shares (illustrative):
- Fixed Cat I share
- Variable/Packing share
- Distribution share
- Marketing share
- Vehicle share
- Others/Wastage share

If allocated overhead sum is `2100` (example only), then:
- `Total Cost = 6225 + 2100 = 8325`
- `Net Profit = 10569.8 - 8325 = 2244.8`
- `Margin % = 2244.8 / 10569.8 x 100 = 21.24%`

---

## 6) Important Notes

- Unit conversion to KG (for EA/PCS style units) is handled in backend code when mapping exists.
- For `Type = both`, internal split logic affects how inhouse vs outsourced portions absorb costs.
- P&L parser reads category totals and subcategory totals from uploaded cost sheet and then feeds allocation.
- `PURCHASE ACCOUNTS` is treated as direct cost logic (not pooled overhead allocation).

---

## 7) One-Page Formula Reference

- `Inward Value = Inward Qty x Inward Eff. Rate`
- `Inward Value (OS) = Inward Qty (OS) x Inward Eff. Rate`
- `Outward Value = Outward Qty x Outward Eff. Rate`
- `Outward Eff. Rate = Outward Value / Outward Qty`
- `Allocation Share = (Product Basis / Total Basis) x Cost Category Total`
- `Total Cost = Direct Cost + Sum(Allocated Costs)`
- `Net Profit = Revenue - Total Cost`
- `Profit Margin % = Net Profit / Revenue x 100`

