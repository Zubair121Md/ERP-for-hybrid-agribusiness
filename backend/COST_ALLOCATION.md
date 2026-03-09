# Complete Cost Allocation Guide

**Comprehensive guide for cost allocation system - Summary and Detailed documentation combined.**

This document explains **exactly** how each cost category is allocated to products using different allocation bases depending on the cost type. It includes quick reference tables, detailed calculation examples, and system architecture diagrams.

---

## 📊 **Sample Sales Data (April 2024)**

| Month | Particulars | Type | Inward Qty | Inward Rate | Inward Value | Outward Qty (kg) | Outward Rate | Outward Value |
|-------|-------------|------|------------|-------------|--------------|------------------|--------------|--------------|
| 24-full | Baby Corn | Outsourced | 6,798 EA | 23.57 | ₹160,203 | 6,783 EA | 42.57 | ₹288,730 |
| 24-full | Tree Tomato | Outsourced | 24.5 kg | 100.41 | ₹2,460 | 31.0 kg | 151.13 | ₹4,685 |
| 24-full | Arugula(Rocket Lettuce) | Both | 409.35 kg | 100.57 | ₹41,170 | 409.35 kg | 271.8 | ₹111,260 |
| 24-full | Basil | Both | 883.14 kg | 104.35 | ₹92,156 | 883.14 kg | 277.1 | ₹244,714 |
| 24-full | Bok Choy | Both | 836.25 kg | 71.13 | ₹59,485 | 840.75 kg | 145.93 | ₹122,695 |
| 24-full | Celery | Both | 3,658.7 kg | 22.86 | ₹83,645 | 3,656.7 kg | 96.59 | ₹353,215 |
| 24-full | Chinese Cabbage | Both | 7,703.0 kg | 31.54 | ₹242,925 | 7,722.0 kg | 65.76 | ₹507,830 |
| 24-full | Curly Parsley | Both | 918.18 kg | 86.17 | ₹79,120 | 918.18 kg | 318.64 | ₹292,565 |
| 24-full | Dill Leaves | Both | 30.35 kg | 74.14 | ₹2,250 | 32.65 kg | 155.02 | ₹5,061 |
| 24-full | Fennel Leaves | Both | 34.67 kg | 84.22 | ₹2,920 | 30.67 kg | 211.48 | ₹6,486 |
| 24-full | Iceberg Lettuce | Both | 21,473.0 kg | 59.73 | ₹1,282,615 | 21,434.0 kg | 126.01 | ₹2,700,875 |
| 24-full | Kale Leaves | Both | 349.58 kg | 50.9 | ₹17,792 | 343.58 kg | 241.13 | ₹82,848 |
| 24-full | Leeks | Both | 829.0 kg | 70.04 | ₹58,060 | 829.0 kg | 179.31 | ₹148,649 |
| 24-full | Lettuce Curly Green | Both | 9,886.25 kg | 32.2 | ₹318,337 | 9,916.25 kg | 143.39 | ₹1,421,859 |
| 24-full | Lettuce Red | Both | 806.75 kg | 8.57 | ₹6,910 | 809.75 kg | 123.21 | ₹99,772 |
| 24-full | Romaine Lettuce | Both | 3,395.05 kg | 35.62 | ₹120,915 | 3,395.05 kg | 137.73 | ₹467,603 |
| 24-full | Spring Onion | Both | 3,365.0 kg | 59.66 | ₹200,758 | 3,365.0 kg | 125.09 | ₹420,931 |
| 24-full | Baby Bokchoy | Inhouse | 11.9 kg | - | - | 11.9 kg | 975.63 | ₹11,610 |
| 24-full | BABY SPINACH BOX | Inhouse | 61.1 kg | - | - | 61.1 kg | 549.61 | ₹33,581 |
| 24-full | Chives | Inhouse | 31.25 kg | - | - | 31.25 kg | 404.34 | ₹12,636 |
| 24-full | Edible Flowers | Inhouse | 25 EA | - | - | 25 EA | 96.04 | ₹2,401 |
| 24-full | Flat Parsely | Inhouse | 0.1 kg | - | - | 0.1 kg | 450 | ₹45 |
| 24-full | Kale Red | Inhouse | 30.7 kg | 7.82 | ₹240 | 30.7 kg | 98.86 | ₹3,035 |
| 24-full | Lollo Bionda Lettuce | Inhouse | 1,466.2 kg | - | - | 1,466.2 kg | 146.64 | ₹214,998 |
| 24-full | Lollo Rosa | Inhouse | 7.05 kg | - | - | 7.05 kg | 125.11 | ₹882 |
| 24-full | Micro Greens-Radish | Inhouse | 7.25 kg | - | - | 7.25 kg | 1,964.14 | ₹14,240 |
| 24-full | MICROGREENS (SUNFLOWER) | Inhouse | 0.75 kg | - | - | 0.75 kg | 2,000 | ₹1,500 |
| 24-full | MIXED SALAD GREENS | Inhouse | 244.6 kg | - | - | 244.6 kg | 902.39 | ₹220,724 |
| 24-full | Oregano | Inhouse | 25.42 kg | - | - | 25.42 kg | 320.61 | ₹8,150 |

**Note:** Products with "Both" type will be split into Inhouse and Outsourced portions using harvest data.

---

## 📖 **Key Definitions**

- **Sales KG** = Outward Quantity (quantity actually sold)
- **Production KG** = Inward Quantity (total quantity produced or handled)
- **Handled KG** = Inward Quantity (total quantity handled = inhouse production + outsourced purchases)
- **Purchase KG** = Inward Quantity for outsourced products (quantity purchased)
- **Purchase Value** = Inward Value = inward_quantity × inward_rate (total purchase cost)
- **Sales Value** = Outward Value = quantity × sale_price (revenue)

---

## 📊 **Quick Reference Table**

| Cost Category | Basis | Applies To | Key Point |
|--------------|-------|------------|-----------|
| **FIXED COST CAT - I** | Sales Value | ALL | Revenue-based |
| **FIXED COST CAT - II** | Production KG | Split by type | Production-based |
| **VARIABLE COST - OPEN FIELD** | Production KG | Open Field (inhouse) | Production-based |
| **VARIABLE COST - LETTUCE(PH)** | Production KG | Polyhouse (inhouse) | Production-based |
| **VARIABLE COST - STRAWBERRY** | Production KG | Strawberry (inhouse) | Production-based |
| **VARIABLE COST - RASPBERRY&BLUBERRY** | Production KG | Raspberry/Blueberry (inhouse) | Production-based |
| **VARIABLE COST - PACKING** | Handled KG | ALL | Handled quantity-based |
| **VARIABLE COST - AGGREGATION** | Purchase KG | All outsourced | Purchase quantity-based |
| **DISTRIBUTION COST** | Sales KG | ALL | Sales volume-based |
| **MARKETING EXPENSES** | Sales Value | ALL | Revenue-based |
| **VEHICLE RUNNING COST** | Handled KG | ALL | Handled quantity-based (trucks move weight, not revenue) |
| **OTHERS** | Sales KG | ALL | Sales volume-based |
| **WASTAGE-OWN FARM** | Production KG | Inhouse only | Production-based |
| **WASTAGE-DISPATCH** | Sales KG | ALL | Sales volume-based |
| **WASTAGE- FARM** | Production KG | Inhouse only | Production-based |
| **PURCHASE ACCOUNTS** | Direct Cost (No Allocation) | All outsourced | Direct purchase value |

---

## 🎯 **Allocation Rules by Category (Summary)**

### **1. FIXED COST CAT - I** (₹393,350)

**Basis:** Sales Value (revenue)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Sales Value / Total Sales Value) × ₹393,350`

**Line Items:**
- ACCOUNTING CHARGES(AUDIT FEE)
- CDSL DEMAT CHARGES
- COMPANY SECRECTORY & MCA FILLING FEES
- COMPLIANCE COUSULTANCY CHARGES
- DEMAT OF SHARES CHARGES
- EMPOLYEE REFRESHMENT
- FSSAI FEE
- INTEREST ON MP CHERIAN LOAN
- INTEREST ON FEROKE BOARDS
- INTEREST ON LATE PAYMENT TDS
- INTEREST ON MA ASRAF LOAN
- INTERNAL AUDIT FEE
- LAND DOCUMENTS CHARGES
- LEGAL CHARGES
- MISCELLANEOUS EXP
- PACKING ROOM RENT
- PROVISION FOR DOUBTFUL DEBTS
- RATE & TAXES
- RTA FEE
- SOFTWARE DEVELOPMENT & MAINTANANCE
- SOIL TEST & LEAF ANALYSIS
- TDS SERVICE CHARGES
- TRADE MARK CONSULTANCY FEE/OTHERS
- VEHICLE ACCIDENT

**What This Means:** Higher revenue products pay more. If a product generates 10% of total revenue, it gets 10% of this cost.

---

### **2. FIXED COST CAT - II** (₹2,815,164)

**Basis:** Production KG (inward quantity)  
**Applies To:** Split into 3 portions

#### **2A. Strawberry 60%** (₹1,689,098)
- **Applies To:** Products with "strawberry" in name AND `source="inhouse"`
- **Formula:** `(Product Production_kg / Total Strawberry Production_kg) × ₹1,689,098`

#### **2B. Greens 25%** (₹703,791)
- **Applies To:** Products with "greens" or "lettuce" in name AND `source="inhouse"`
- **Formula:** `(Product Production_kg / Total Greens Production_kg) × ₹703,791`

#### **2C. Aggregation 15%** (₹422,275)
- **Applies To:** All products with `source="outsourced"` (any outsourced product)
- **Formula:** `(Product Production_kg / Total Outsourced Production_kg) × ₹422,275`

**Line Items:**
- ELECTRICITY CHARGES
- RUNNING & MAINTANACE OTHERS
- STAFF BASIC SALARY
- STAFF HOUSE RENT
- STAFF OTHER ALLOWANCE
- STAFF PHONE ALLOWANCE
- STAFF SALARY & INCENTIVES
- TOOLS & IMPLEMENTS

**What This Means:** Production volume determines allocation. Higher production = more cost allocation.

---

### **3. VARIABLE COST**

#### **3A. OPEN FIELD** (₹344,751)

**Basis:** Production KG (inward quantity)  
**Applies To:** Inhouse products mapped to "Open Field" section  
**Formula:** `(Product Production_kg / Total Open Field Production_kg) × ₹344,751`

**Line Items:**
- LEASE LAND
- SPRAYING MANURING
- FUELS
- WORKERSWAGES
- WORKERS OVERTIME
- CULTIVATION OTHERS
- TILLING & PLOUGHING
- SEEDS PURCHASE/OTHERS
- COWDUNG MANURE

**What This Means:** Only Open Field inhouse products share these costs based on production volume.

---

#### **3B. LETTUCE(PH)** (₹704,051)

**Basis:** Production KG (inward quantity)  
**Applies To:** Inhouse products mapped to Polyhouse sections (C, D, E)  
**Formula:** `(Product Production_kg / Total Polyhouse Production_kg) × ₹704,051`

**Line Items:**
- SPRAYING MANURING
- FUELS
- WORKERSWAGES
- WORKERS OVERTIME
- SEEDLINGS PURCHASE
- PACKING MATERIALS
- OTHERS
- NURSARY SEEDS PURCHASE/OTHERS

**What This Means:** Only Polyhouse inhouse products share these costs based on production volume.

---

#### **3C. STRAWBERRY** (₹1,715,301)

**Basis:** Production KG (inward quantity)  
**Applies To:** Products with "strawberry" in name AND `source="inhouse"`  
**Formula:** `(Product Production_kg / Total Strawberry Production_kg) × ₹1,715,301`

**Line Items:**
- SPRAYING MANURING
- FUELS
- WORKERSWAGES
- WORKERS OVERTIME
- PACKING MATERIALS
- CONSULTANT FEE/OTHERS
- REPLANTING/OTHERS

**What This Means:** Only Strawberry inhouse products share these costs based on production volume.

---

#### **3D. RASPBERRY&BLUBERRY** (₹18,671)

**Basis:** Production KG (inward quantity)  
**Applies To:** Products with "raspberry" or "blueberry" in name AND `source="inhouse"`  
**Formula:** `(Product Production_kg / Total Raspberry/Blueberry Production_kg) × ₹18,671`

**Line Items:**
- SPRAYING MANURING
- FUELS
- WORKERSWAGES
- WORKERS OVERTIME
- PACKING MATERIALS
- OTHER EXP

**What This Means:** Only Raspberry/Blueberry inhouse products share these costs based on production volume.

---

#### **3E. PACKING** (₹813,443)

**Basis:** Handled KG (inward quantity)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Handled_kg / Total Handled_kg) × ₹813,443`

**Line Items:**
- WORKERSWAGES
- WORKERS OVERTIME
- QC SALARY
- PACKING TEAM SALARY
- PACKING- TRAVELLING ALLOWANCE
- PACKING ALLOWANCE
- PACKING MATERIALS
- PACKING MATERIALS

**What This Means:** All products (both inhouse and outsourced) share packing costs based on handled quantity (inward quantity). This includes both inhouse production and outsourced purchases that need to be packed.

---

#### **3F. AGGREGATION** (₹1,556,118)

**Basis:** Purchase KG (inward quantity for outsourced)  
**Applies To:** All products with `source="outsourced"` (any outsourced product)  
**Formula:** `(Product Purchase_kg / Total Purchase_kg) × ₹1,556,118`

**Line Items:**
- TRAVELLING ALLOWANCE
- PURCHASE EXECUTIVE SALARY
- LOADING & UNLOADING-PURCHASE
- FREIGHT CHARGES-VEGETABLES

**What This Means:** All outsourced products share these costs based on purchase quantity (inward quantity).

---

### **4. DISTRIBUTION COST** (₹1,518,289)

**Basis:** Sales KG (outward quantity)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Sales_kg / Total Sales_kg) × ₹1,518,289`

**Line Items:**
- DRIVER BETTA
- DRIVER SALARY
- DRIVER INCENTIVES
- TRANSPORT EXPENSES
- PARKING FEE
- LOADING & UNLOADING-SALES
- LOADING OTHERS
- DELIVERY CHARGES
- HAMPER DISTRIBTION COST

**What This Means:** All products share distribution costs based on sales volume (quantity sold).

---

### **5. MARKETING EXPENSES** (₹221,594)

**Basis:** Sales Value (revenue)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Sales Value / Total Sales Value) × ₹221,594`

**Line Items:**
- TRAVELLING EXP AND OTHERS
- SALES TEAM INCENTIVES
- ADVERTISMENT & INSTAGRAM

**What This Means:** Higher revenue products pay more for marketing. If a product generates 15% of revenue, it gets 15% of marketing costs.

---

### **6. VEHICLE RUNNING COST** (₹3,144,393)

**Basis:** Handled KG (quantity sold/dispatched)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Handled_kg / Total Handled_kg) × ₹3,144,393`

**Line Items:**
- VEHICLE DIESEL
- VEHICLE MAINTANANCE
- VEHICLE PERMIT & INSURANCE

**What This Means:** All products share vehicle costs based on handled quantity (weight moved). Trucks move weight, not revenue, so allocation is based on the quantity handled/dispatched rather than sales value.

---

### **7. OTHERS** (₹410,219)

**Basis:** Sales KG (outward quantity)  
**Applies To:** ALL products (inhouse + outsourced)  
**Formula:** `(Product Sales_kg / Total Sales_kg) × ₹410,219`

**Line Items:**
- BANKING CHARGES(ONLINE HAMPER COMMISION)
- COURIER AND POSTAGE
- DEBTORS WRITTEN OFF
- DISCOUNT
- FINANCE COST
- FINE OR PENALITY
- FREE HAMPER
- FREIGHT CHARGES
- MISCELLANEOUS
- OFFICE & ADMINISTRATION EXP
- ROUND OFF
- TEA AND FOOD
- TRAVELLING EXP-STAFF

**What This Means:** All products share these miscellaneous costs based on sales volume.

---

### **8. WASTAGE & SHORTAGE** (₹1,069,586)

#### **8A. WASTAGE-OWN FARM** (₹8)
- **Basis:** Production KG (inward quantity)
- **Applies To:** Inhouse products only
- **Formula:** `(Product Production_kg / Total Inhouse Production_kg) × ₹8`

#### **8B. WASTAGE-DISPATCH** (₹90,560)
- **Basis:** Sales KG (outward quantity)
- **Applies To:** ALL products (inhouse + outsourced)
- **Formula:** `(Product Sales_kg / Total Sales_kg) × ₹90,560`

#### **8C. WASTAGE- FARM** (₹979,018)
- **Basis:** Production KG (inward quantity)
- **Applies To:** Inhouse products only
- **Formula:** `(Product Production_kg / Total Inhouse Production_kg) × ₹979,018`

**What This Means:** 
- **OWN FARM & FARM wastage**: Allocated based on production volume (inhouse only)
- **DISPATCH wastage**: Allocated based on sales volume (all products) - this represents wastage during delivery/dispatch

---

### **9. PURCHASE ACCOUNTS** (₹13,523,576)

**Basis:** DO NOT ALLOCATE - Use Direct Purchase Value  
**Applies To:** All products with `source="outsourced"` (any outsourced product)  
**Formula:** Direct cost = `inward_value` (inward_quantity × inward_rate)

**Line Items:**
- PURCHASE VEGETABLES
- PURCHASE OTHERS

**What This Means:** Purchase Accounts are NOT allocated as overhead costs. Instead, each outsourced product uses its direct purchase value (inward_value) as a direct cost. This means:
- If Tree Tomato (Outsourced) has inward_value = ₹2,460, then its direct purchase cost = ₹2,460
- If Baby Corn (Outsourced) has inward_value = ₹160,203, then its direct purchase cost = ₹160,203
- No allocation needed - each product bears its own purchase cost directly

**Why Direct Cost?** Purchase costs are direct costs that should be attributed directly to the product purchased, not allocated as overhead. This provides accurate cost tracking per product.

---

## 🔑 **Key Concepts**

### **Production KG vs Sales KG**
- **Production KG** (Inward Quantity): Total quantity produced or handled
  - Used for: Variable Costs, Fixed Cost Cat II, Wastage
  - Example: If you produced 1,000 kg but only sold 800 kg, Production KG = 1,000 kg

- **Sales KG** (Outward Quantity): Quantity actually sold
  - Used for: Distribution, Vehicle Running, Others
  - Example: If you produced 1,000 kg but only sold 800 kg, Sales KG = 800 kg

### **Sales Value vs Purchase Value**
- **Sales Value**: Revenue from sales = quantity × sale_price
  - Used for: Fixed Cost Cat I, Marketing
  - Example: 100 kg × ₹50/kg = ₹5,000

- **Purchase Value**: Cost of purchase = inward_quantity × inward_rate
  - Used for: Purchase Accounts (as direct cost, not allocated)
  - Example: 100 kg × ₹30/kg = ₹3,000 (direct cost for that product)

- **Handled KG**: Total quantity handled (inhouse production + outsourced purchases)
  - Used for: Packing costs
  - Example: Inhouse produced 500 kg + Outsourced purchased 300 kg = 800 kg handled

### **Aggregation Identification**
- **Aggregation** = Any product with `source="outsourced"`
- **NOT** identified by name containing "aggregation"
- All outsourced products are considered aggregation for cost allocation purposes

---

## 📋 **Detailed Allocation Rules with Examples**

### **Rule 1: FIXED COST CAT - I** (₹393,350)

**Applies To:** ALL products (both inhouse + outsourced)  
**Basis:** Sales Value (revenue = quantity × sale_price)  
**Allocation Method:** Proportional to total sales value

#### **Calculation Example:**

**Step 1: Calculate Total Sales Value**
```
Total Sales Value = Sum of all Outward Value (quantity × sale_price)

Example:
- Tree Tomato: 31.0 kg × ₹151.13 = ₹4,685
- Arugula: 409.35 kg × ₹271.8 = ₹111,260
- Basil: 883.14 kg × ₹277.1 = ₹244,714
- Chinese Cabbage: 7,722.0 kg × ₹65.76 = ₹507,830
... (sum all)

Total Sales Value = ₹4,685 + ₹111,260 + ₹244,714 + ₹507,830 + ... = ₹X
```

**Step 2: Calculate Allocation for Each Product**
```
Product Allocation = (Product Sales Value / Total Sales Value) × ₹393,350

Example for Tree Tomato:
- Tree Tomato Sales Value = ₹4,685
- Total Sales Value = ₹X
- Tree Tomato Allocation = (₹4,685 / ₹X) × ₹393,350

Example for Iceberg Lettuce:
- Iceberg Lettuce Sales Value = ₹2,700,875
- Iceberg Lettuce Allocation = (₹2,700,875 / ₹X) × ₹393,350
```

**Step 3: Result**
- Each product receives a proportional share based on its sales value (revenue)
- Higher revenue products get more allocation
- Lower revenue products get less allocation

---

### **Rule 2: FIXED COST CAT - II** (₹2,815,164)

**Applies To:** Split by product type  
**Basis:** Production KG (inward quantity)  
**Allocation Method:** Fixed percentage splits

#### **Split Breakdown:**

**2A. FIXED COST CAT - II (Strawberry 60%)** = ₹1,689,098

**Applies To:** Only products with "strawberry" in name AND `source="inhouse"`  
**Basis:** Production KG (inward quantity)  
**Allocation:** 60% of ₹2,815,164 = ₹1,689,098

**Calculation:**
```
Step 1: Find all Strawberry products (inhouse only)
- Filter products where name contains "strawberry" AND source="inhouse"
- Calculate total Strawberry production_kg (inward_quantity)

Step 2: Allocate proportionally
- Product Allocation = (Product Production_kg / Total_Strawberry_Production_kg) × ₹1,689,098

Example:
- Strawberry A Grade: Production_kg = 15,766.5 kg
- Strawberry B Grade: Production_kg = 7,278.7 kg
- Total Strawberry Production: 23,045.2 kg

Strawberry A Grade Allocation = (15,766.5 / 23,045.2) × ₹1,689,098 = ₹1,156,234
Strawberry B Grade Allocation = (7,278.7 / 23,045.2) × ₹1,689,098 = ₹532,864
```

**2B. FIXED COST CAT - II (Greens 25%)** = ₹703,791

**Applies To:** Only products with "greens" or "lettuce" in name AND `source="inhouse"`  
**Basis:** Production KG (inward quantity)  
**Allocation:** 25% of ₹2,815,164 = ₹703,791

**Calculation:**
```
Step 1: Find all Greens/Lettuce products (inhouse only)
- Filter products where (name contains "greens" OR "lettuce") AND source="inhouse"
- Examples: Lollo Bionda Lettuce, MIXED SALAD GREENS, Kale Red, etc.

Step 2: Calculate total Greens/Lettuce production_kg (inward_quantity)
- Lollo Bionda Lettuce: 1,466.2 kg
- MIXED SALAD GREENS: 244.6 kg
- Kale Red: 30.7 kg
- ... (sum all inhouse greens/lettuce production_kg)

Step 3: Allocate proportionally
- Lollo Bionda Allocation = (1,466.2 / Total_Greens_Production_kg) × ₹703,791
- MIXED SALAD GREENS Allocation = (244.6 / Total_Greens_Production_kg) × ₹703,791
- ... (for each greens/lettuce product)
```

**2C. FIXED COST CAT - II (Aggregation 15%)** = ₹422,275

**Applies To:** All products with `source="outsourced"` (any outsourced product is aggregation)  
**Basis:** Production KG (inward quantity for outsourced products)  
**Allocation:** 15% of ₹2,815,164 = ₹422,275

**Calculation:**
```
Step 1: Find all outsourced products
- Filter products where source="outsourced"
- Note: Aggregation is identified by source, not by name

Step 2: Calculate total outsourced production_kg
- Sum inward_quantity for all outsourced products
- Example: Baby Corn: 6,798 EA (convert to kg), Tree Tomato: 24.5 kg, etc.

Step 3: Allocate proportionally
- Product Allocation = (Product Production_kg / Total_Outsourced_Production_kg) × ₹422,275

Example:
- Tree Tomato (Outsourced): Production_kg = 24.5 kg
- Baby Corn (Outsourced): Production_kg = Y kg (after EA conversion)
- Total Outsourced Production: Z kg

Tree Tomato Allocation = (24.5 / Z) × ₹422,275
Baby Corn Allocation = (Y / Z) × ₹422,275
```

---

### **Rule 3: VARIABLE COST** (Multiple Subcategories)

**Applies To:** Product-type specific (inhouse or outsourced)  
**Basis:** Production KG (inward quantity) for production costs, Purchase KG for aggregation  
**Allocation Method:** Proportional within each subcategory

#### **3A. VARIABLE COST - OPEN FIELD** (₹344,751)

**Applies To:** Only inhouse products mapped to "Open Field" section  
**Basis:** Production KG (inward quantity)  
**Allocation:** Proportional to Open Field products only

**Calculation:**
```
Step 1: Find products mapped to "Open Field" section (inhouse only)
- Query ProductSectionMapping where section="Open Field"
- Filter products where source="inhouse"
- Examples: Chinese Cabbage (if mapped to Open Field), Spring Onion (if mapped)

Step 2: Calculate total Open Field production_kg
- Sum inward_quantity for all Open Field products

Step 3: Allocate proportionally
- Product Allocation = (Product Production_kg / Total_OpenField_Production_kg) × ₹344,751

Example:
- Chinese Cabbage (Open Field): Production_kg = 7,703.0 kg
- Spring Onion (Open Field): Production_kg = 3,365.0 kg
- Total Open Field Production: 11,068.0 kg

Chinese Cabbage Allocation = (7,703.0 / 11,068.0) × ₹344,751 = ₹239,856
Spring Onion Allocation = (3,365.0 / 11,068.0) × ₹344,751 = ₹104,895
```

#### **3B. VARIABLE COST - LETTUCE(PH)** (₹704,051)

**Applies To:** Only inhouse products mapped to "Polyhouse C", "Polyhouse D", or "Polyhouse E" sections  
**Basis:** Production KG (inward quantity)  
**Allocation:** Proportional to Polyhouse lettuce products only

**Calculation:**
```
Step 1: Find products mapped to Polyhouse sections (inhouse only)
- Query ProductSectionMapping where section LIKE "Polyhouse%"
- Filter products where source="inhouse"
- Examples: Arugula (Polyhouse C), Basil (Polyhouse C), Lettuce Curly Green (Polyhouse C), etc.

Step 2: Calculate total Polyhouse production_kg
- Sum inward_quantity for all Polyhouse products

Step 3: Allocate proportionally
- Product Allocation = (Product Production_kg / Total_Polyhouse_Production_kg) × ₹704,051

Example:
- Arugula (Polyhouse C): Production_kg = 409.35 kg
- Basil (Polyhouse C): Production_kg = 883.14 kg
- Lettuce Curly Green (Polyhouse C): Production_kg = 9,886.25 kg
- Lollo Bionda Lettuce (Polyhouse C): Production_kg = 1,466.2 kg
- Total Polyhouse Production: 12,644.94 kg

Arugula Allocation = (409.35 / 12,644.94) × ₹704,051 = ₹22,789
Basil Allocation = (883.14 / 12,644.94) × ₹704,051 = ₹49,193
Lettuce Curly Green Allocation = (9,886.25 / 12,644.94) × ₹704,051 = ₹549,456
Lollo Bionda Allocation = (1,466.2 / 12,644.94) × ₹704,051 = ₹81,613
```

#### **3E. VARIABLE COST - PACKING** (₹813,443)

**Applies To:** ALL products (inhouse + outsourced)  
**Basis:** Handled KG (inward quantity)  
**Allocation:** Proportional to all products based on handled quantity

**Calculation:**
```
Step 1: Find all products (inhouse + outsourced)
- Filter all products (both source="inhouse" and source="outsourced")
- Sum handled_kg (inward_quantity) for all products

Step 2: Allocate proportionally
- Product Allocation = (Product Handled_kg / Total_Handled_kg) × ₹813,443

Example:
- Baby Bokchoy (Inhouse): Handled_kg = 11.9 kg
- BABY SPINACH BOX (Inhouse): Handled_kg = 61.1 kg
- Tree Tomato (Outsourced): Handled_kg = 24.5 kg
- Baby Corn (Outsourced): Handled_kg = Y kg (after EA conversion)
- ... (sum all handled_kg for all products)

Baby Bokchoy Allocation = (11.9 / Total_Handled_kg) × ₹813,443
BABY SPINACH BOX Allocation = (61.1 / Total_Handled_kg) × ₹813,443
Tree Tomato Allocation = (24.5 / Total_Handled_kg) × ₹813,443
... (for each product, both inhouse and outsourced)
```

#### **3F. VARIABLE COST - AGGREGATION** (₹1,556,118)

**Applies To:** All products with `source="outsourced"` (any outsourced product is aggregation)  
**Basis:** Purchase KG (inward quantity for outsourced products)  
**Allocation:** Proportional to outsourced products only

**Calculation:**
```
Step 1: Find all outsourced products
- Filter products where source="outsourced"
- Note: Aggregation is identified by source, not by name

Step 2: Calculate total purchase_kg (inward_quantity for outsourced)
- Sum inward_quantity for all outsourced products
- Example: Baby Corn: 6,798 EA (convert to kg), Tree Tomato: 24.5 kg

Step 3: Allocate proportionally
- Product Allocation = (Product Purchase_kg / Total_Purchase_kg) × ₹1,556,118

Example:
- Tree Tomato (Outsourced): Purchase_kg = 24.5 kg
- Baby Corn (Outsourced): Purchase_kg = Y kg (after EA conversion)
- Total Purchase_kg = Z kg

Tree Tomato Allocation = (24.5 / Z) × ₹1,556,118
Baby Corn Allocation = (Y / Z) × ₹1,556,118
```

---

### **Rule 9: PURCHASE ACCOUNTS** (₹13,523,576)

**Applies To:** All products with `source="outsourced"` (any outsourced product)  
**Basis:** DO NOT ALLOCATE - Use Direct Purchase Value  
**Allocation Method:** Direct cost assignment (no allocation)

**What This Means:** Purchase Accounts are NOT allocated as overhead costs. Instead, each outsourced product uses its direct purchase value (inward_value) as a direct cost.

**Example:**
- Tree Tomato (Outsourced): inward_value = 24.5 × ₹100.41 = ₹2,460 → Direct Purchase Cost = ₹2,460
- Baby Corn (Outsourced): inward_value = 6,798 EA × ₹23.57 = ₹160,203 → Direct Purchase Cost = ₹160,203
- No allocation needed - each product bears its own purchase cost directly

**Why Direct Cost?** Purchase costs are direct costs that should be attributed directly to the product purchased, not allocated as overhead. This provides accurate cost tracking per product.

---

## 💡 **Example Calculation**

**Product: Iceberg Lettuce (Both type, split using harvest data)**
- Inhouse portion: 1,682.30 kg (Production), 1,682.30 kg (Sales), ₹211,955 (Sales Value)
- Outsourced portion: 19,791.0 kg (Production), 19,751.70 kg (Sales), ₹2,488,920 (Sales Value), ₹1,182,131 (Purchase Value)

**Allocations:**
1. **FIXED COST CAT - I**: Based on Sales Value → (₹211,955 + ₹2,488,920) / Total Sales Value × ₹393,350
2. **FIXED COST CAT - II (Greens)**: Based on Production KG → 1,682.30 / Total Greens Production × ₹703,791
3. **FIXED COST CAT - II (Aggregation)**: Based on Production KG → 19,791.0 / Total Outsourced Production × ₹422,275
4. **VARIABLE COST - OPEN FIELD**: Based on Production KG → 1,682.30 / Total Open Field Production × ₹344,751
5. **VARIABLE COST - PACKING**: Based on Handled KG → (1,682.30 + 19,791.0) / Total Handled KG × ₹813,443
6. **VARIABLE COST - AGGREGATION**: Based on Purchase KG → 19,791.0 / Total Purchase KG × ₹1,556,118
7. **DISTRIBUTION COST**: Based on Sales KG → (1,682.30 + 19,751.70) / Total Sales KG × ₹1,518,289
8. **MARKETING**: Based on Sales Value → (₹211,955 + ₹2,488,920) / Total Sales Value × ₹221,594
9. **VEHICLE RUNNING**: Based on Handled KG → (1,682.30 + 19,751.70) / Total Handled KG × ₹3,144,393
10. **OTHERS**: Based on Sales KG → (1,682.30 + 19,751.70) / Total Sales KG × ₹410,219
11. **WASTAGE**: 
    - OWN FARM: Based on Production KG → 1,682.30 / Total Inhouse Production × ₹8
    - DISPATCH: Based on Sales KG → (1,682.30 + 19,751.70) / Total Sales KG × ₹90,560
    - FARM: Based on Production KG → 1,682.30 / Total Inhouse Production × ₹979,018
12. **PURCHASE ACCOUNTS**: Direct Cost (No Allocation) → ₹1,182,131 (direct purchase value for outsourced portion)

---

## 🏗️ **System Architecture & Flow**

### **Overview: How Cost Allocation Works**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INPUT LAYER                                 │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              ├──────────────────────┐
                              │                      │
                              ▼                      ▼
        ┌─────────────────────────────┐  ┌─────────────────────────────┐
        │   SALES DATA EXCEL          │  │   P&L COST SHEET EXCEL      │
        │   (Monthly Sales)           │  │   (Expense Categories)      │
        └─────────────────────────────┘  └─────────────────────────────┘
                  │                              │
                  │                              │
                  ▼                              ▼
        ┌─────────────────────────────┐  ┌─────────────────────────────┐
        │  /api/upload-excel         │  │  /api/upload-pl              │
        │  POST endpoint              │  │  POST endpoint              │
        └─────────────────────────────┘  └─────────────────────────────┘
                  │                              │
                  │                              │
                  ▼                              ▼
        ┌─────────────────────────────┐  ┌─────────────────────────────┐
        │  Parse Sales Data            │  │  Parse P&L Costs             │
        │  - Extract products          │  │  - Extract cost categories   │
        │  - Extract quantities        │  │  - Extract amounts           │
        │  - Determine source          │  │  - Set allocation basis     │
        │    (inhouse/outsourced)      │  │  - Set applies_to            │
        └─────────────────────────────┘  └─────────────────────────────┘
                  │                              │
                  │                              │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
        ┌─────────────────────────────────────────────────────────────┐
        │                    DATABASE LAYER                            │
        ├─────────────────────────────────────────────────────────────┤
        │                                                             │
        │  PRODUCTS Table                                            │
        │  MONTHLY_SALES Table                                       │
        │  COSTS Table                                               │
        │  ALLOCATIONS Table                                         │
        │                                                             │
        └─────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
        ┌─────────────────────────────────────────────────────────────┐
        │              COST ALLOCATION ENGINE                          │
        │              (CostAllocationEngine)                          │
        └─────────────────────────────────────────────────────────────┘
                                 │
                                 │ For each Cost:
                                 │ 1. Filter applicable products
                                 │ 2. Calculate basis for each product
                                 │ 3. Allocate proportionally
                                 │
                                 ▼
        ┌─────────────────────────────────────────────────────────────┐
        │              PROFITABILITY CALCULATION                        │
        │                                                              │
        │  For each Product:                                          │
        │  Revenue = outward_qty × sale_price                         │
        │  Direct Cost = inward_value (if outsourced)                 │
        │  Allocated Overhead = Sum of allocations                    │
        │  Net Profit = Revenue - Direct Cost - Allocated Overhead   │
        └─────────────────────────────────────────────────────────────┘
```

---

### **Step-by-Step Flow with Example**

#### **Step 1: Upload Sales Data**

**Input Excel:**
```
Month    | Particulars      | Type      | Inward Qty | Outward Qty | Outward Rate
---------|------------------|-----------|------------|-------------|-------------
April    | Strawberry       | Inhouse   | 15,766.5   | 15,766.5    | ₹350
April    | Tree Tomato      | Outsourced| 24.5       | 31.0        | ₹151.13
April    | Iceberg Lettuce  | Both      | 21,473.0   | 21,434.0    | ₹126.01
```

**What Happens:**
```
┌─────────────────────────────────────────────────────────┐
│  System Processing:                                     │
│                                                         │
│  1. Parse Excel → Extract product data                  │
│  2. For "Both" type → Check harvest data               │
│     - Iceberg Lettuce: Harvest shows 1,682.30 kg       │
│     - Split: 1,682.30 kg (inhouse) + 19,791.0 kg (outsourced)│
│  3. Create Products:                                    │
│     - Strawberry (source="inhouse")                     │
│     - Tree Tomato (source="outsourced")                 │
│     - Iceberg Lettuce Inhouse (source="inhouse")       │
│     - Iceberg Lettuce Outsourced (source="outsourced") │
│  4. Create MonthlySales records with quantities         │
└─────────────────────────────────────────────────────────┘
```

---

#### **Step 2: Upload P&L Cost Sheet**

**Input Excel:**
```
SL.NO | PARTICULARS              | TOTAL
------|--------------------------|----------
1     | FIXED COST CAT - I       | 393350
2     | FIXED COST CAT -II       | 2815164
      | VARIABLE COST            |
A)    | OPEN FIELD               | 344751
B)    | LETTUCE                  | 704051
C)    | STRAWBERRY               | 1715301
      | PACKING                  | 813443
      | AGGREGATION              | 1556118
3     | DISTRIBUTION COST        | 1518289
4     | MARKETING EXPENSES       | 221594
5     | VEHICLE RUNNING COST     | 3144393
6     | OTHERS                   | 410219
7     | WASTAGE & SHORTAGE       | 1069586
8     | PURCHASE ACCOUNTS        | 13523576
```

**What Happens:**
```
┌─────────────────────────────────────────────────────────┐
│  System Processing:                                     │
│                                                         │
│  1. Parse Excel → Extract cost categories              │
│  2. Create Cost records with:                           │
│     - name: "FIXED COST CAT - I"                       │
│     - amount: ₹393,350                                  │
│     - basis: "Sales Value"                             │
│     - applies_to: "both"                                │
│                                                         │
│     - name: "VARIABLE COST - PACKING"                  │
│     - amount: ₹813,443                                  │
│     - basis: "Handled KG"                               │
│     - applies_to: "both"                                │
│                                                         │
│     - name: "PURCHASE ACCOUNTS"                         │
│     - amount: ₹13,523,576                               │
│     - basis: "Direct Cost" (no allocation)             │
│     - applies_to: "outsourced"                          │
└─────────────────────────────────────────────────────────┘
```

---

#### **Step 3: Cost Allocation Process**

**For Each Cost, the System:**

```
┌─────────────────────────────────────────────────────────────┐
│  ALLOCATION PROCESS FOR: FIXED COST CAT - I (₹393,350)      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Step 1: Filter Products                                   │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ applies_to = "both" → ALL products                  │  │
│  │ ✅ Strawberry (inhouse)                              │  │
│  │ ✅ Tree Tomato (outsourced)                          │  │
│  │ ✅ Iceberg Lettuce Inhouse                           │  │
│  │ ✅ Iceberg Lettuce Outsourced                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  Step 2: Calculate Basis (Sales Value)                     │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Strawberry: 15,766.5 × ₹350 = ₹5,518,275           │  │
│  │ Tree Tomato: 31.0 × ₹151.13 = ₹4,685                │  │
│  │ Iceberg Inhouse: 1,682.3 × ₹126.01 = ₹211,955      │  │
│  │ Iceberg Outsourced: 19,751.7 × ₹126.01 = ₹2,488,920│  │
│  │ Total Sales Value = ₹8,223,835                      │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  Step 3: Allocate Proportionally                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Strawberry: (₹5,518,275 / ₹8,223,835) × ₹393,350    │  │
│  │           = ₹263,456                                 │  │
│  │                                                      │  │
│  │ Tree Tomato: (₹4,685 / ₹8,223,835) × ₹393,350      │  │
│  │            = ₹224                                    │  │
│  │                                                      │  │
│  │ Iceberg Inhouse: (₹211,955 / ₹8,223,835) × ₹393,350│  │
│  │                = ₹10,123                             │  │
│  │                                                      │  │
│  │ Iceberg Outsourced: (₹2,488,920 / ₹8,223,835) × ₹393,350│
│  │                    = ₹118,547                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  Step 4: Save to ALLOCATIONS Table                         │
└─────────────────────────────────────────────────────────────┘
```

---

### **Special Cases**

**A. VARIABLE COST - PACKING (Handled KG)**

All products (both inhouse and outsourced) share packing costs based on handled quantity (inward quantity).

**B. PURCHASE ACCOUNTS (Direct Cost - No Allocation)**

Purchase Accounts are NOT allocated. Each outsourced product uses its direct purchase value (inward_value) as a direct cost.

**C. WASTAGE-DISPATCH (Sales KG)**

All products share dispatch wastage costs based on sales volume (outward quantity).

---

## 📝 **Notes**

- All allocations are **proportional** within each category's applicable products
- Products with "Both" type are split into Inhouse and Outsourced portions using harvest data
- Section-based filtering (for VARIABLE COST categories) uses ProductSectionMapping table
- EA units are converted to kg using conversion factors
- Total cost per product = Direct Purchase Cost (if outsourced) + Sum of all allocations from all applicable cost categories
- Purchase Accounts are NOT allocated - they are direct costs assigned directly to each outsourced product

---

This comprehensive guide covers all aspects of the cost allocation system, from quick reference tables to detailed calculation examples and system architecture.
