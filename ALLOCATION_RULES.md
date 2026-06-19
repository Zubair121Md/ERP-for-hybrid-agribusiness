# Cost Allocation Rules

This document describes how the ERP allocates P&L cost pools to product lines. It mirrors the management P&L sheet (`cost analysis/pnl.md`) and harvest template (`harvest.md`).

## Core formula

Every allocatable pool uses the same flat COP logic:

```
Pool rate (₹/kg) = Pool total ÷ Section denominator kg
Product share    = Pool rate × Product basis kg
                 = Pool total × (Product basis kg ÷ Section denominator kg)
```

**Product basis** is almost always **sold kg** from the sales upload. Exceptions are noted below.

## Section denominator kg (priority)

For inhouse section pools, the denominator is chosen in this order:

1. **Official P&L kg** — fixed values from the management P&L (authoritative)
2. **Harvest template kg** — sum from uploaded harvest data for mapped products
3. **Mapped sales kg** — sum of sold kg for products in the Product–Section mapping
4. **Stored / lookup / sum of applicable basis** — fallback for costs without a section pool

### Official section kg (from P&L)

| Section / pool | Denominator kg | Products included |
|---|---:|---|
| Strawberry (inhouse) | 4,688.20 | Premium, A, B, C — all mapped Strawberry lines |
| Greens / Lettuce (Polyhouse C + E) | 2,539.63 | All mapped Polyhouse C & E products |
| Open Field | 509.60 | Dill, Iceberg, Spring Onion, Zucchini, etc. |
| Common expenses – farm | 7,883.08 | **All inhouse** farm products (strawberry + greens + open field) |
| Aggregation (outsourced) | 12,798.665 | All outsourced purchase/stock lines |
| Packing | 20,536.10 | All products (inhouse + outsourced) |
| Packing materials (others) | 20,536.095 | All products |
| FC-I, Distribution, Marketing, Vehicle, Others | 20,536.10 | All products with sales |

## Cost category rules

### Fixed Cost Cat I
- **Applies to:** All products (inhouse + outsourced)
- **Basis:** Sold kg
- **Denominator:** 20,536.10 (total qty sold)

### Fixed Cost Cat II (split on upload)
The P&L FC-II total is split into four buckets, then each bucket is allocated separately:

| Bucket | % of FC-II | Denominator kg | Products |
|---|---:|---:|---|
| Strawberry | 50% | 4,688.20 | Inhouse strawberry lines (mapping) |
| Greens | 25% | 2,539.63 | Inhouse Polyhouse C/E lines (mapping) |
| Open Field | 10% | 509.60 | Inhouse open-field lines (mapping) |
| Aggregation | 15% | 12,798.665 | Outsourced lines only |

### Variable cost — section pools (inhouse)

| Pool | Denominator | Products (via Product–Section mapping) |
|---|---:|---|
| Open Field | 509.60 | Open Field section |
| Lettuce / Greens | 2,539.63 | Polyhouse C + Polyhouse E |
| Strawberry | 4,688.20 | Strawberry section (Premium, A, B, C) |
| Raspberry & Blueberry | Sales kg in section | Berry products (if any sales) |
| Citrus | Sales kg in section | Citrus products (if any sales) |
| Common expenses – farm | 7,883.08 | **All inhouse** products |

### Variable cost — shared pools

| Pool | Denominator | Products |
|---|---:|---|
| Packing | 20,536.10 | All (inhouse + outsourced) |
| Packing materials (others) | 20,536.095 | All |
| Aggregation | 12,798.665 | Outsourced only |

### Distribution, Marketing, Vehicle, Others
- **Applies to:** All products with sales
- **Basis:** Sold kg
- **Denominator:** 20,536.10

### Wastage & shortage
- **Applies to:** Outsourced products only
- **Basis:** Wastage kg (WD + WF from sales upload), not sold kg
- **Denominator:** Sum of outsourced wastage kg (or manual month override); P&L reference COP uses 12,798.665 kg

### Purchase accounts (outsourced)
Two modes on the Allocation tab:

| Mode | Behaviour |
|---|---|
| **Standard** | Each outsourced line keeps its own `direct_cost` from the sales upload; pool is not allocated |
| **By sales kg** | PURCHASE ACCOUNTS pool split across outsourced lines by sold kg |

## Product–Section mapping

Mappings define which products belong to each P&L section (Strawberry, Polyhouse C, Open Field, etc.).

- Default mappings are **auto-seeded on startup** from `backend/default_product_section_mappings.json`
- Mappings **persist through database reset** (reset clears sales, costs, products — not mappings)
- Missing mappings cause products to be **excluded** from section pools (denominator and allocation both shrink)

**Strawberry Premium** is included in the default Strawberry mapping so it receives the same variable and FC-II strawberry costs as A, B, and C.

## What is *not* double-counted

- Variable cost **line items** (spraying, wages, etc.) are display-only; only the **parent pool row** is allocated
- FC-II **bucket rows** are allocated; the parent FC-II total row is not allocated again
- Purchase cost in **Standard** mode stays on `direct_cost` and is not added from the PURCHASE ACCOUNTS pool

## Typical workflow

1. Upload **harvest** template (optional — improves fallback denominators)
2. Upload **sales** Excel (creates products + monthly sales)
3. Upload **P&L** cost sheet (creates cost pools)
4. Review **Product–Section mapping** (defaults pre-loaded)
5. **Run Allocation** for the target month
6. Check per-product COP and margin on the Allocation tab / Excel export

## Reference files

- P&L denominators and COP: `cost analysis/pnl.md`
- Harvest quantities by section: `harvest.md`
- Default mappings: `backend/default_product_section_mappings.json`
