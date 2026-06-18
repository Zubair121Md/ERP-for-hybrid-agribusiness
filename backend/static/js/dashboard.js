// Dashboard JavaScript for Fruit & Vegetable Cost Allocation System
const API_BASE = '/api';
let currentTab = 'dashboard';
let charts = {};
let currentData = {};
let cachedSalesMonths = [];
let cachedCostMonths = [];
let cachedCosts = [];

// Fixed cost template — names/keys used for display and manual entry
const COST_TEMPLATE = [
    { key: 'fixed_cost_cat_i', label: '1. FIXED COST CAT - I', level: 0, defaultAppliesTo: 'both' },
    { key: 'fixed_cost_cat_ii', label: '2. FIXED COST CAT - II', level: 0, defaultAppliesTo: 'inhouse' },
    { key: 'variable_cost', label: '3. VARIABLE COST', level: 0, isParent: true },
    { key: 'open_field', label: 'A) OPEN FIELD', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'lettuce', label: 'B) LETTUCE', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'strawberry', label: 'C) STRAWBERRY', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'raspberry_blueberry', label: 'D) RASPBERRY & BLUEBERRY', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'citrus', label: 'E) CITRUS', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'packing', label: 'F) PACKING', level: 1, parent: 'variable_cost', defaultAppliesTo: 'both' },
    { key: 'aggregation', label: 'G) AGGREGATION', level: 1, parent: 'variable_cost', defaultAppliesTo: 'outsourced' },
    { key: 'common_expenses_farm', label: 'H) COMMON EXPENSES - FARM', level: 1, parent: 'variable_cost', defaultAppliesTo: 'inhouse' },
    { key: 'packing_materials_others', label: 'I) PACKING MATERIALS (OTHERS)', level: 1, parent: 'variable_cost', defaultAppliesTo: 'both' },
    { key: 'distribution_cost', label: '4. DISTRIBUTION COST', level: 0, defaultAppliesTo: 'both' },
    { key: 'marketing_expenses', label: '5. MARKETING EXPENSES', level: 0, defaultAppliesTo: 'both' },
    { key: 'vehicle_running_cost', label: '6. VEHICLE RUNNING COST', level: 0, defaultAppliesTo: 'both' },
    { key: 'others', label: '7. OTHERS', level: 0, defaultAppliesTo: 'both' },
    { key: 'wastage_shortage', label: '8. WASTAGE & SHORTAGE', level: 0, defaultAppliesTo: 'outsourced' },
    { key: 'purchase_accounts', label: '9. PURCHASE ACCOUNTS', level: 0, defaultAppliesTo: 'outsourced' },
];

const FC2_DEFAULT_SPLITS = { strawberry: 50, greens: 25, openField: 10, aggregation: 15 };

const FC2_BUCKET_CONFIG = [
    { key: 'strawberry', label: 'Strawberry', pctId: 'fc2-strawberry-pct', name: 'FIXED COST CAT - II - Strawberry', applies_to: 'inhouse', cost_type: 'inhouse-only' },
    { key: 'greens', label: 'Greens', pctId: 'fc2-greens-pct', name: 'FIXED COST CAT - II - Greens', applies_to: 'inhouse', cost_type: 'inhouse-only' },
    { key: 'openField', label: 'Open Field', pctId: 'fc2-openfield-pct', name: 'FIXED COST CAT - II - Open Field', applies_to: 'inhouse', cost_type: 'inhouse-only' },
    { key: 'aggregation', label: 'Aggregation', pctId: 'fc2-aggregation-pct', name: 'FIXED COST CAT - II - Aggregation', applies_to: 'outsourced', cost_type: 'common' },
];

function getFc2CostsForMonth(costs, month) {
    const m = normalizeMonthKey(month);
    return (costs || []).filter(c => {
        const n = (c.name || '').toUpperCase();
        return normalizeMonthKey(c.month) === m && n.startsWith('FIXED COST CAT - II');
    });
}

function getFc2BucketCosts(costs, month) {
    const fc2 = getFc2CostsForMonth(costs, month);
    return FC2_BUCKET_CONFIG.map(cfg => fc2.find(c => (c.name || '').toUpperCase() === cfg.name.toUpperCase()) || null);
}

function getFc2TotalAmount(costs, month) {
    const buckets = getFc2BucketCosts(costs, month);
    const bucketSum = buckets.reduce((s, c) => s + (c?.amount || 0), 0);
    if (bucketSum > 0) return bucketSum;
    const pooled = getFc2CostsForMonth(costs, month).find(c => (c.name || '').trim().toUpperCase() === 'FIXED COST CAT - II');
    return pooled?.amount || 0;
}

function getFc2SplitPercents(costs, month) {
    const buckets = getFc2BucketCosts(costs, month);
    const total = buckets.reduce((s, c) => s + (c?.amount || 0), 0);
    if (total <= 0) return { ...FC2_DEFAULT_SPLITS };
    const [s, g, o, a] = buckets;
    return {
        strawberry: s ? ((s.amount / total) * 100) : 0,
        greens: g ? ((g.amount / total) * 100) : 0,
        openField: o ? ((o.amount / total) * 100) : 0,
        aggregation: a ? ((a.amount / total) * 100) : 0,
    };
}

function renderFc2SplitSection(costs, activeMonth, fc2Total) {
    const pcts = getFc2SplitPercents(costs, activeMonth);
    const buckets = getFc2BucketCosts(costs, activeMonth);
    const pctValues = [pcts.strawberry, pcts.greens, pcts.openField, pcts.aggregation];
    const amounts = FC2_BUCKET_CONFIG.map((_, i) => {
        const pct = pctValues[i];
        return fc2Total > 0 && pct > 0 ? (fc2Total * pct / 100) : (buckets[i]?.amount || 0);
    });
    let bucketRows = FC2_BUCKET_CONFIG.map((cfg, i) => `
        <tr style="background: #fafafa;">
            <td style="padding-left: 48px; font-size: 13px; color: #374151;">↳ ${cfg.label}</td>
            <td style="text-align: right; font-size: 13px; color: #6b7280;" id="fc2-amt-${cfg.key}">₹${formatNumber(amounts[i])}</td>
            <td colspan="2"></td>
        </tr>
    `).join('');
    return `
        <tr>
            <td colspan="4" style="padding: 12px 20px; background: #f9fafb; border-bottom: 1px solid #e5e7eb;">
                <div style="font-weight: 600; margin-bottom: 10px; font-size: 13px;">Fixed Cost II Split (% of total)</div>
                <div style="display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end;">
                    ${FC2_BUCKET_CONFIG.map((cfg, i) => `
                        <label style="font-size: 12px;">
                            ${cfg.label}
                            <input id="${cfg.pctId}" type="number" min="0" max="100" step="0.1"
                                value="${pctValues[i].toFixed(1)}"
                                oninput="updateFc2SplitPreview()"
                                style="width: 70px; margin-left: 4px;"> %
                        </label>
                    `).join('')}
                    <span id="fc2-pct-total-label" style="font-size: 12px; color: #6b7280; margin-left: 8px;">
                        Total: ${pctValues.reduce((a, b) => a + b, 0).toFixed(1)}%
                    </span>
                </div>
            </td>
        </tr>
        ${bucketRows}
    `;
}

function updateFc2SplitPreview() {
    const totalInput = document.querySelector('tr[data-template-key="fixed_cost_cat_ii"] .cost-amount-input');
    const totalFc2 = parseFloat(totalInput?.value || 0);
    const pcts = FC2_BUCKET_CONFIG.map(cfg => parseFloat(document.getElementById(cfg.pctId)?.value || 0));
    const sumPct = pcts.reduce((a, b) => a + b, 0);
    const label = document.getElementById('fc2-pct-total-label');
    if (label) {
        label.textContent = `Total: ${sumPct.toFixed(1)}%` + (Math.abs(sumPct - 100) > 0.01 ? ' (will normalize to 100%)' : '');
        label.style.color = Math.abs(sumPct - 100) > 0.01 ? '#b45309' : '#6b7280';
    }
    if (totalFc2 <= 0 || sumPct <= 0) return;
    const normalized = pcts.map(v => (v / sumPct) * 100);
    let allocated = 0;
    FC2_BUCKET_CONFIG.forEach((cfg, i) => {
        const el = document.getElementById(`fc2-amt-${cfg.key}`);
        if (!el) return;
        let amt;
        if (i === FC2_BUCKET_CONFIG.length - 1) {
            amt = Math.round((totalFc2 - allocated) * 100) / 100;
        } else {
            amt = Math.round((totalFc2 * (normalized[i] / 100)) * 100) / 100;
            allocated += amt;
        }
        el.textContent = '₹' + formatNumber(amt);
    });
}

function getActiveCostMonth(costs) {
    const sel = normalizeMonthKey(document.getElementById('allocation-month')?.value || '');
    if (sel) return sel;
    const months = [...new Set((costs || []).map(c => normalizeMonthKey(c.month)).filter(Boolean))].sort();
    if (months.length) return months[months.length - 1];
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
}

function resolveTemplateKey(cost) {
    const nameUpper = (cost.name || '').toUpperCase();
    const cat = (cost.category || '').toLowerCase();
    if (nameUpper.startsWith('FIXED COST CAT - II -')) return null;
    if (cat === 'fixed_cost_cat_i' || (nameUpper.includes('FIXED COST CAT') && nameUpper.includes('I') && !nameUpper.includes('II'))) {
        return 'fixed_cost_cat_i';
    }
    if (nameUpper === 'FIXED COST CAT - II' || (cat === 'fixed_cost_cat_ii' && nameUpper === 'FIXED COST CAT - II')) {
        return 'fixed_cost_cat_ii';
    }
    if (nameUpper.includes('OPEN FIELD') && !nameUpper.includes('FIXED COST')) return 'open_field';
    if (nameUpper.includes('LETTUCE')) return 'lettuce';
    if (nameUpper.includes('STRAWBERRY')) return 'strawberry';
    if (nameUpper.includes('RASPBERRY') || nameUpper.includes('BLUEBERRY')) return 'raspberry_blueberry';
    if (nameUpper.includes('CITRUS')) return 'citrus';
    if (nameUpper.includes('PACKING MATERIALS') && nameUpper.includes('OTHER')) return 'packing_materials_others';
    if (nameUpper.includes('PACKING')) return 'packing';
    if (nameUpper.includes('AGGREGATION') && !nameUpper.includes('FIXED COST')) return 'aggregation';
    if (nameUpper.includes('COMMON EXPENSES') && nameUpper.includes('FARM')) return 'common_expenses_farm';
    if (cat === 'distribution_cost' || nameUpper.includes('DISTRIBUTION')) return 'distribution_cost';
    if (cat === 'marketing_expenses' || nameUpper.includes('MARKETING')) return 'marketing_expenses';
    if (cat === 'vehicle_running_cost' || nameUpper.includes('VEHICLE')) return 'vehicle_running_cost';
    if (cat === 'others' || nameUpper === 'OTHERS') return 'others';
    if (cat === 'wastage_shortage' || nameUpper.includes('WASTAGE')) return 'wastage_shortage';
    if (cat === 'purchase_accounts' || nameUpper === 'PURCHASE ACCOUNTS') return 'purchase_accounts';
    return null;
}
let pendingDashboardStats = null;

function normalizeMonthKey(value) {
    if (value === undefined || value === null) return '';
    const s = String(value).trim();
    if (!s) return '';
    const m = s.match(/(\d{4})-(\d{2})/);
    if (m) return `${m[1]}-${m[2]}`;
    const dt = new Date(s);
    if (!Number.isNaN(dt.getTime())) {
        return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
    }
    return s.length >= 7 ? s.slice(0, 7) : s;
}

function refreshAllocationMonthOptionsFromCache() {
    const sel = document.getElementById('allocation-month');
    if (!sel) return;
    const current = normalizeMonthKey(sel.value);
    const salesSet = new Set(cachedSalesMonths.filter(Boolean));
    const costSet = new Set(cachedCostMonths.filter(Boolean));
    const common = [...salesSet].filter(m => costSet.has(m));
    const monthList = (common.length > 0 ? common : [...new Set([...salesSet, ...costSet])]).sort().reverse();
    if (monthList.length === 0) return;

    sel.innerHTML = monthList.map(m => `<option value="${m}">${m}</option>`).join('');
    sel.value = monthList.includes(current) ? current : monthList[0];
}

function getCheckedRadioValue(name) {
    const el = document.querySelector(`input[name="${name}"]:checked`);
    return el ? el.value : '';
}

function setCheckedRadioValue(name, value) {
    const radios = document.querySelectorAll(`input[name="${name}"]`);
    radios.forEach(r => {
        r.checked = (r.value === value);
    });
}

// Quantity display formatter for EA and KG
function formatQtyDisplay(productName, unit, quantity) {
    const name = (productName || '').toLowerCase();
    const u = (unit || 'kg').toUpperCase();
    const isEA = ['EA','EACH','PC','PCS','UNIT','UNITS'].includes(u);
    const roundedQty = parseFloat(Number(quantity).toFixed(3));
    if (!isEA) return `${roundedQty} ${unit || 'kg'}`;
    if (name.includes('hamper')) return `${roundedQty} EA`;
    if (name.includes('button mushroom') || name.includes('baby corn')) {
        const kg = (roundedQty * 200) / 1000; // 200 g per EA
        return `${roundedQty} EA (200 g ea, ${kg.toFixed(2)} kg)`;
    }
    return `${roundedQty} EA`;
}

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeSidebar();
    initializeDashboard();
    loadDashboardData();
    setupEventListeners();
    refreshAllocationMonthOptionsFromCache();
});

// Initialize dashboard components
function initializeDashboard() {
    // Initialize charts
    initializeCharts();
    
    // Load initial data
    loadProducts();
    loadSales();
    loadCosts();
}

// Setup event listeners
function setupEventListeners() {
    // Tab navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const tab = this.getAttribute('data-tab');
            showTab(tab);
        });
    });
    
    // Form submissions (use unified submit handlers that support add + edit modes)
    document.getElementById('product-form').addEventListener('submit', submitProductForm);
    document.getElementById('sales-form').addEventListener('submit', submitSalesForm);
    document.getElementById('cost-form').addEventListener('submit', submitCostForm);
    
    // Month filters removed - now using all data
}

// Tab switching
function showTab(tabName) {
    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active class from all nav items
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.remove('active');
    });
    
    // Show selected tab content
    const tabElement = document.getElementById(tabName);
    if (!tabElement) {
        console.error(`❌ Tab element not found: ${tabName}`);
        return;
    }
    tabElement.classList.add('active');
    
    // Add active class to clicked nav item
    const navItem = document.querySelector(`[data-tab="${tabName}"]`);
    if (navItem) {
        navItem.classList.add('active');
    } else {
        console.error(`❌ Nav item not found for tab: ${tabName}`);
    }
    
    // Update page title and subtitle
    const titles = {
        'dashboard': { title: 'Dashboard', subtitle: 'Overview & Analytics' },
        'products': { title: 'Products', subtitle: 'Manage Your Inventory' },
        'sales': { title: 'Sales before removal of Wastage', subtitle: 'Track Monthly Sales Data (harvest & purchase inward)' },
        'costs': { title: 'Costs', subtitle: 'Manage Operational Costs' },
        'allocation': { title: 'Allocation', subtitle: 'Cost Distribution Analysis' },
        'data-upload': { title: 'Data Upload', subtitle: 'Upload Sales & P&L Data' },
        'harvest-mapping': { title: 'Mapping', subtitle: 'Upload Product-Section Mappings' },
        'settings': { title: 'Settings', subtitle: 'System Configuration' }
    };
    
    const tabInfo = titles[tabName] || { title: 'Dashboard', subtitle: 'Overview & Analytics' };
    document.getElementById('page-title').textContent = tabInfo.title;
    document.getElementById('page-subtitle').textContent = tabInfo.subtitle;
    
    currentTab = tabName;
    
    // Load data for specific tabs
    if (tabName === 'dashboard') {
        loadDashboardData();
        // Charts need a visible canvas — refresh after tab is shown
        requestAnimationFrame(() => refreshDashboardCharts(pendingDashboardStats));
    } else if (tabName === 'products') {
        loadProducts();
    } else if (tabName === 'sales') {
        loadSales();
    } else if (tabName === 'costs') {
        loadCosts();
    } else if (tabName === 'harvest-mapping') {
        loadProductAllowlists();
        loadSectionMappings();
    }
}

// Load dashboard data
async function loadDashboardData() {
    try {
        showLoading('stats-grid');
        
        // Load dashboard stats
        const statsResponse = await fetch(`${API_BASE}/dashboard/stats`);
        const stats = await statsResponse.json();
        
        displayDashboardStats(stats);
        pendingDashboardStats = stats;
        
        // Load top products
        await loadTopProducts();
        
        // Only paint charts when dashboard tab is visible (Chart.js breaks on hidden canvas)
        if (currentTab === 'dashboard') {
            refreshDashboardCharts(stats);
        }
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        showAlert('Error loading dashboard data', 'error');
    }
}

// Display dashboard statistics
function displayDashboardStats(stats) {
    const statsGrid = document.getElementById('stats-grid');
    const gross = stats.gross_sales_revenue != null ? stats.gross_sales_revenue : (stats.total_revenue || 0);
    const netRev = stats.net_revenue != null ? stats.net_revenue : (stats.total_revenue || 0);
    const sr = stats.sales_returns || 0;
    const ii = stats.indirect_income || 0;
    const stk = stats.stock_adjustment || 0;
    const hasAdj = sr !== 0 || ii !== 0 || stk !== 0;
    let revDetail = `Gross sales ₹${formatNumber(gross)}`;
    if (hasAdj) {
        const parts = [revDetail];
        if (sr !== 0) parts.push(`− returns ₹${formatNumber(sr)}`);
        if (ii !== 0) parts.push(`+ indirect ₹${formatNumber(ii)}`);
        if (stk !== 0) parts.push(`− stock ₹${formatNumber(stk)}`);
        revDetail = parts.join(' · ');
    }
    const pnlTot = stats.pnl_expenses_total != null ? stats.pnl_expenses_total : 0;
    const econCost = stats.total_costs || 0;
    const allocatedCost = stats.allocated_costs_total != null ? stats.allocated_costs_total : econCost;
    const costFootnote = Math.abs(pnlTot - allocatedCost) > 1 && pnlTot > 0
        ? `Allocated ₹${formatNumber(allocatedCost)} · P&amp;L sheet ₹${formatNumber(pnlTot)}`
        : 'Direct + allocated costs';
    
    const statsHTML = `
        <div class="stat-card products">
            <div class="stat-header">
                <span class="stat-title">Total Products</span>
                <i class="fas fa-apple-alt stat-icon"></i>
            </div>
            <div class="stat-value">${stats.total_products}</div>
            <div class="stat-change positive">
                <i class="fas fa-arrow-up"></i>
                ${stats.active_products} active
            </div>
        </div>
        
        <div class="stat-card revenue">
            <div class="stat-header">
                <span class="stat-title">Net revenue</span>
                <i class="fas fa-chart-line stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(netRev)}</div>
            <div class="stat-change positive">
                <i class="fas fa-percent"></i>
                ${(stats.revenue_margin || 0).toFixed(1)}% on sales
            </div>
            <div class="stat-change" style="font-size: 0.8rem; margin-top: 6px; line-height: 1.35;">
                ${revDetail}
            </div>
        </div>
        
        <div class="stat-card costs">
            <div class="stat-header">
                <span class="stat-title">Total costs</span>
                <i class="fas fa-dollar-sign stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(econCost)}</div>
            <div class="stat-change">
                <i class="fas fa-layer-group"></i>
                ${costFootnote}
            </div>
        </div>
        
        <div class="stat-card profit">
            <div class="stat-header">
                <span class="stat-title">Net profit</span>
                <i class="fas fa-trophy stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(stats.total_profit || 0)}</div>
            <div class="stat-change ${(stats.total_profit || 0) >= 0 ? 'positive' : 'negative'}">
                <i class="fas fa-${(stats.total_profit || 0) >= 0 ? 'arrow-up' : 'arrow-down'}"></i>
                ${(stats.profit_margin || 0).toFixed(1)}% on cost (CP)
            </div>
        </div>
    `;
    
    statsGrid.innerHTML = statsHTML;
}

// Load top products (by profit, with costs and margin from allocation)
async function loadTopProducts() {
    try {
        const response = await fetch(`${API_BASE}/dashboard/top-products`);
        if (!response.ok) {
            throw new Error(`Failed to load top products: ${response.status}`);
        }
        const topProducts = await response.json();
        displayTopProducts(topProducts);
    } catch (error) {
        console.error('Error loading top products:', error);
        document.getElementById('top-products-table').innerHTML = '<p>No data available</p>';
    }
}

// Display top products
function displayTopProducts(products) {
    const container = document.getElementById('top-products-table');
    
    if (products.length === 0) {
        container.innerHTML = '<p>No products data available</p>';
        return;
    }
    
    let tableHTML = `
        <table class="table">
            <thead>
                <tr>
                    <th>Product</th>
                    <th>Source</th>
                    <th>Revenue</th>
                    <th>Costs</th>
                    <th>Profit</th>
                    <th>Margin</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    products.forEach(product => {
        const revenue = product.revenue || 0;
        const totalCost = product.total_cost || 0;
        const profit = product.profit || 0;
        const profitMargin = product.profit_margin || 0;
        
        tableHTML += `
            <tr>
                <td><strong>${product.product_name || 'Unknown'}</strong></td>
                <td><span class="badge ${product.source === 'inhouse' ? 'badge-success' : 'badge-info'}">${product.source || 'unknown'}</span></td>
                <td>₹${formatNumber(revenue)}</td>
                <td>₹${formatNumber(totalCost)}</td>
                <td class="${profit >= 0 ? 'text-success' : 'text-danger'}">₹${formatNumber(profit)}</td>
                <td>${profitMargin.toFixed(1)}%</td>
            </tr>
        `;
    });
    
    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
}

// Initialize charts
function destroyAllCharts() {
    ['revenue', 'source', 'bucket', 'bucketPie'].forEach((key) => {
        if (charts[key]) {
            try { charts[key].destroy(); } catch (_) { /* already destroyed */ }
            charts[key] = null;
        }
    });
}

function isDashboardTabVisible() {
    const el = document.getElementById('dashboard');
    return el && el.classList.contains('active');
}

function getCanvas(id) {
    const el = document.getElementById(id);
    return el && el.isConnected ? el : null;
}

function ensureChartsInitialized() {
    const revenueCanvas = getCanvas('revenueChart');
    const sourceCanvas = getCanvas('sourceChart');
    if (!revenueCanvas || !sourceCanvas) return false;
    if (charts.revenue?.canvas !== revenueCanvas || charts.source?.canvas !== sourceCanvas) {
        destroyAllCharts();
        initializeCharts();
    }
    return Boolean(charts.revenue && charts.source);
}

function safeChartUpdate(chart, fn) {
    if (!chart || !chart.canvas || !chart.canvas.isConnected) return;
    try {
        fn();
        chart.update('none');
    } catch (e) {
        console.warn('Chart update skipped:', e);
    }
}

function refreshDashboardCharts(stats) {
    if (!isDashboardTabVisible()) return;
    if (!ensureChartsInitialized()) return;
    if (stats) updateCharts(stats);
    loadDashboardBucketCharts();
    requestAnimationFrame(() => {
        ['revenue', 'source', 'bucket', 'bucketPie'].forEach((key) => {
            if (charts[key]?.resize) {
                try { charts[key].resize(); } catch (_) { /* ignore */ }
            }
        });
    });
}

function initializeCharts() {
    const palette = {
        purple: 'rgba(139, 92, 246, 0.85)',
        green: 'rgba(16, 185, 129, 0.85)',
        red: 'rgba(239, 68, 68, 0.85)',
        amber: 'rgba(245, 158, 11, 0.85)',
        blue: 'rgba(59, 130, 246, 0.85)',
    };

    const revenueCanvas = getCanvas('revenueChart');
    const sourceCanvas = getCanvas('sourceChart');
    if (!revenueCanvas || !sourceCanvas) return;

    charts.revenue = new Chart(revenueCanvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: ['Revenue', 'Costs', 'Profit'],
            datasets: [{
                label: 'Amount (₹)',
                data: [0, 0, 0],
                backgroundColor: [palette.blue, palette.red, palette.green],
                borderRadius: 8,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: '#f1f5f9' },
                    ticks: { callback: (v) => '₹' + formatNumber(v) }
                },
                x: { grid: { display: false } }
            }
        }
    });
    
    const sourceCanvas2 = getCanvas('sourceChart');
    if (!sourceCanvas2) return;
    charts.source = new Chart(sourceCanvas2.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: ['Inhouse', 'Outsourced'],
            datasets: [{
                data: [0, 0],
                backgroundColor: [palette.green, palette.amber],
                borderWidth: 0,
                hoverOffset: 6,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '62%',
            plugins: { legend: { position: 'bottom' } }
        }
    });

    const bucketCanvas = getCanvas('bucketChart');
    if (bucketCanvas) {
        charts.bucket = new Chart(bucketCanvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: 'kg',
                    data: [],
                    backgroundColor: [
                        '#f472b6', '#a78bfa', '#34d399', '#fbbf24', '#94a3b8'
                    ],
                    borderRadius: 6,
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { beginAtZero: true, grid: { color: '#f1f5f9' } },
                    y: { grid: { display: false } }
                }
            }
        });
    }

    const bucketPieCanvas = getCanvas('bucketPieChart');
    if (bucketPieCanvas) {
        charts.bucketPie = new Chart(bucketPieCanvas.getContext('2d'), {
            type: 'doughnut',
            data: {
                labels: [],
                datasets: [{
                    data: [],
                    backgroundColor: ['#f472b6', '#a78bfa', '#34d399', '#fbbf24', '#94a3b8'],
                    borderWidth: 0,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '55%',
                plugins: { legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } } }
            }
        });
    }
}

async function loadDashboardBucketCharts() {
    if (!isDashboardTabVisible()) return;
    if (!ensureChartsInitialized()) return;
    if (!charts.bucket && !charts.bucketPie) return;
    try {
        const month = normalizeMonthKey(document.getElementById('allocation-month')?.value || '');
        const url = month
            ? `${API_BASE}/sales-weight-summary?month=${encodeURIComponent(month)}`
            : `${API_BASE}/sales-weight-summary`;
        const res = await fetch(url);
        if (!res.ok) return;
        const summary = await res.json();
        const dist = summary.distribution || [];
        const labels = dist.map(d => d.label);
        const kgs = dist.map(d => d.kg);
        const pcts = dist.map(d => d.percent);

        const monthEl = document.getElementById('bucket-chart-month');
        if (monthEl) {
            monthEl.textContent = summary.month ? summary.month : (summary.scope === 'all_time' ? 'all months' : 'latest month');
        }

        if (charts.bucket) {
            charts.bucket.data.labels = labels;
            charts.bucket.data.datasets[0].data = kgs;
            safeChartUpdate(charts.bucket, () => {});
        }
        if (charts.bucketPie) {
            charts.bucketPie.data.labels = labels;
            charts.bucketPie.data.datasets[0].data = pcts;
            safeChartUpdate(charts.bucketPie, () => {});
        }
    } catch (e) {
        console.warn('Bucket charts:', e);
    }
}

// Update charts with data
function updateCharts(stats) {
    if (!charts.revenue || !charts.source) return;
    safeChartUpdate(charts.revenue, () => {
        charts.revenue.data.datasets[0].data = [
            stats.total_revenue,
            stats.total_costs,
            stats.total_profit
        ];
    });
    safeChartUpdate(charts.source, () => {
        charts.source.data.datasets[0].data = [
            stats.inhouse_revenue,
            stats.outsourced_revenue
        ];
    });
}

// Load products
async function loadProducts() {
    try {
        showLoading('products-table');
        
        const response = await fetch(`${API_BASE}/products/`);
        if (!response.ok) {
            throw new Error(`Products API returned ${response.status}`);
        }
        const products = await response.json();
        
        if (!Array.isArray(products)) {
            throw new Error('Invalid products data received');
        }
        
        displayProducts(products);
        
        // Update product dropdowns
        updateProductDropdowns(products);
        
    } catch (error) {
        console.error('Error loading products:', error);
        showAlert('Error loading products', 'error');
    }
}

// Display products
function displayProducts(products) {
    const container = document.getElementById('products-table');
    
    if (products.length === 0) {
        container.innerHTML = '<p>No products found. Add some products to get started!</p>';
        return;
    }
    
    let tableHTML = `
        <table class="table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Source</th>
                    <th>Unit</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    products.forEach(product => {
        tableHTML += `
            <tr>
                <td>${product.id}</td>
                <td><strong>${product.name}</strong></td>
                <td><span class="badge ${product.source === 'inhouse' ? 'badge-success' : 'badge-info'}">${product.source}</span></td>
                <td>${product.unit}</td>
                <td><span class="badge ${product.is_active ? 'badge-success' : 'badge-danger'}">${product.is_active ? 'Active' : 'Inactive'}</span></td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="editProduct(${product.id})">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteProduct(${product.id})">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    
    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
}

// Load sales data
async function loadSales() {
    try {
        showLoading('sales-table');
        
        const salesRes = await fetch(`${API_BASE}/sales`);
        const sales = await salesRes.json();
        cachedSalesMonths = [...new Set((sales || []).map(s => normalizeMonthKey(s.month)).filter(Boolean))];
        refreshAllocationMonthOptionsFromCache();

        const allocSel = normalizeMonthKey(document.getElementById('allocation-month')?.value || '');
        const hasAllocMonth = allocSel && (sales || []).some(s => normalizeMonthKey(s.month) === allocSel);
        const sortedMonths = [...cachedSalesMonths].sort();
        const monthForSummary = hasAllocMonth ? allocSel : (sortedMonths.length ? sortedMonths[sortedMonths.length - 1] : '');
        const summaryUrl = monthForSummary
            ? `${API_BASE}/sales-weight-summary?month=${encodeURIComponent(monthForSummary)}`
            : `${API_BASE}/sales-weight-summary`;

        let summary = null;
        try {
            const summaryRes = await fetch(summaryUrl);
            if (summaryRes.ok) summary = await summaryRes.json();
        } catch (e) {
            console.warn('sales-weight-summary failed', e);
        }
        displaySalesWeightSummary(summary);
        displaySales(sales);
        
    } catch (error) {
        console.error('Error loading sales:', error);
        showAlert('Error loading sales data', 'error');
    }
}

function displaySalesWeightSummary(summary) {
    const container = document.getElementById('sales-weight-summary');
    if (!container) return;
    if (!summary || !summary.total_kg) {
        container.style.display = 'none';
        container.innerHTML = '';
        return;
    }
    const monthNote = summary.month
        ? ` — ${summary.month}`
        : (summary.scope === 'all_time' ? ' — all months' : '');
    const scopeP = summary.scope_note
        ? `<p style="margin:0 0 10px;font-size:0.85rem;color:#64748b;">${summary.scope_note}</p>`
        : '';
    const pctNote = summary.distribution_percent_note
        ? `<p style="margin:0 0 6px;font-size:0.8rem;color:#94a3b8;">${summary.distribution_percent_note}</p>`
        : '';
    const ofNote = summary.open_field_note
        ? `<p style="margin:0 0 8px;font-size:0.8rem;color:#64748b;">${summary.open_field_note}</p>`
        : '';
    container.style.display = 'block';
    const inhouseGross = summary.line_inhouse_gross_kg != null ? summary.line_inhouse_gross_kg : 0;
    const excelInW = summary.excel_scan?.inhouse_wastage_kg != null
        ? summary.excel_scan.inhouse_wastage_kg
        : (summary.line_inhouse_farm_wf_kg != null ? summary.line_inhouse_farm_wf_kg : (summary.line_inhouse_farm_wastage_kg || 0));
    const excelOutW = summary.excel_scan?.outsourced_wastage_kg != null
        ? summary.excel_scan.outsourced_wastage_kg
        : (summary.line_outsourced_wastage_kg || 0);
    const effInW = summary.effective_wastage?.inhouse_wastage_kg != null
        ? summary.effective_wastage.inhouse_wastage_kg
        : excelInW;
    const effOutW = summary.effective_wastage?.outsourced_wastage_kg != null
        ? summary.effective_wastage.outsourced_wastage_kg
        : excelOutW;
    const ovIn = summary.wastage_override?.inhouse_wastage_kg;
    const ovOut = summary.wastage_override?.outsourced_wastage_kg;
    const wastageMonth = summary.month || summary.wastage_override?.month || '';
    const inhouseSold = summary.line_inhouse_sold_kg != null ? summary.line_inhouse_sold_kg : 0;
    const outPurchase = summary.line_outsourced_purchase_kg != null
        ? summary.line_outsourced_purchase_kg
        : (summary.line_outsourced_kg || 0);
    const outOpening = summary.line_outsourced_opening_kg || 0;
    const outWd = summary.line_outsourced_wd_kg || 0;
    const outSold = summary.line_outsourced_sold_kg != null ? summary.line_outsourced_sold_kg : 0;
    const hasOverride = ovIn != null || ovOut != null;

    container.innerHTML = `
        <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:16px 20px;">
            <h4 style="margin:0 0 4px;color:#1e293b;">Inward weight (before wastage removal)${monthNote}</h4>
            <p style="margin:0 0 14px;color:#64748b;font-size:0.85rem;">
                From upload Harvest / Purchase columns — not sold qty. Override wastage below if Excel WD+WF totals are wrong.
            </p>
            ${wastageMonth ? `
            <div style="background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:14px 16px;margin-bottom:16px;">
                <div style="font-weight:600;color:#374151;margin-bottom:10px;">Manual wastage (kg) — ${wastageMonth}</div>
                <p style="margin:0 0 12px;font-size:0.82rem;color:#64748b;">
                    Excel scan: inhouse (WF on harvest rows) <strong>${formatNumber(excelInW)}</strong> kg ·
                    outsourced (WD+WF) <strong>${formatNumber(excelOutW)}</strong> kg.
                    Saved values are used for display and for WASTAGE &amp; SHORTAGE allocation (outsourced only).
                </p>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;align-items:end;">
                    <label style="display:block;font-size:0.85rem;color:#374151;">
                        Inhouse wastage (kg)
                        <input type="number" id="wastage-override-inhouse" step="0.001" min="0" class="form-control"
                            style="margin-top:4px;width:100%;"
                            value="${ovIn != null ? ovIn : ''}"
                            placeholder="Excel: ${formatNumber(excelInW)}">
                    </label>
                    <label style="display:block;font-size:0.85rem;color:#374151;">
                        Outsourced wastage (kg)
                        <input type="number" id="wastage-override-outsourced" step="0.001" min="0" class="form-control"
                            style="margin-top:4px;width:100%;"
                            value="${ovOut != null ? ovOut : ''}"
                            placeholder="e.g. 1180.55">
                    </label>
                    <div style="display:flex;gap:8px;flex-wrap:wrap;">
                        <button type="button" class="btn btn-primary" onclick="saveMonthlyWastageOverride('${wastageMonth}')">
                            Save wastage
                        </button>
                        <button type="button" class="btn btn-secondary" onclick="clearMonthlyWastageOverride('${wastageMonth}')">
                            Use Excel scan
                        </button>
                    </div>
                </div>
                ${hasOverride ? `<p style="margin:10px 0 0;font-size:0.82rem;color:#047857;">Using saved: inhouse <strong>${formatNumber(effInW)}</strong> kg · outsourced <strong>${formatNumber(effOutW)}</strong> kg (re-run allocation to apply).</p>` : ''}
            </div>
            ` : ''}
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:16px;">
                <div style="background:#ecfdf5;border:1px solid #a7f3d0;border-radius:8px;padding:12px 14px;">
                    <div style="font-size:0.8rem;color:#047857;font-weight:600;">Inhouse — Harvest column <span style="font-weight:500;">(before wastage removal)</span></div>
                    <div style="font-size:1.35rem;color:#065f46;font-weight:700;">${formatNumber(inhouseGross)} kg</div>
                    <div style="font-size:0.85rem;color:#6b7280;margin-top:4px;">
                        Inhouse wastage: <strong>${formatNumber(effInW)}</strong> kg
                        ${hasOverride && ovIn != null ? ' (manual)' : (excelInW > 0 ? ` (Excel scan ${formatNumber(excelInW)} kg)` : '')}
                    </div>
                    <div style="font-size:0.85rem;color:#64748b;margin-top:4px;">Not deducted from harvest · not used for WASTAGE pool.</div>
                    ${inhouseSold > 0 ? `<div style="font-size:0.85rem;color:#94a3b8;margin-top:4px;">Sold kg (separate): ${formatNumber(inhouseSold)} kg</div>` : ''}
                    <div style="font-size:0.85rem;color:#6b7280;margin-top:6px;">${summary.inhouse_line_count || 0} products with harvest · ${summary.inhouse_share_percent != null ? summary.inhouse_share_percent : '—'}% of inward</div>
                </div>
                <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 14px;">
                    <div style="font-size:0.8rem;color:#1d4ed8;font-weight:600;">Outsourced — Purchase column</div>
                    <div style="font-size:1.35rem;color:#1e40af;font-weight:700;">${formatNumber(outPurchase)} kg</div>
                    ${outOpening > 0 ? `<div style="font-size:0.85rem;color:#6b7280;margin-top:4px;">Opening stock (excluded from purchase): ${formatNumber(outOpening)} kg</div>` : '<div style="font-size:0.85rem;color:#6b7280;margin-top:4px;">Opening stock excluded from purchase total</div>'}
                    <div style="font-size:0.85rem;color:#6b7280;margin-top:4px;">
                        Outsourced wastage (WD+WF): <strong>${formatNumber(effOutW)}</strong> kg
                        ${hasOverride && ovOut != null ? ' (manual — used for WASTAGE &amp; SHORTAGE)' : (excelOutW > 0 ? ` (Excel scan ${formatNumber(excelOutW)} kg)` : '')}
                        ${outWd > 0 && !hasOverride ? ` · dispatch scan ${formatNumber(outWd)} kg` : ''}
                    </div>
                    ${outSold > 0 ? `<div style="font-size:0.85rem;color:#94a3b8;margin-top:4px;">Sold kg (separate): ${formatNumber(outSold)} kg</div>` : ''}
                    <div style="font-size:0.85rem;color:#6b7280;margin-top:6px;">${summary.outsourced_line_count || 0} products with purchase · ${summary.outsourced_share_percent != null ? summary.outsourced_share_percent : '—'}% of inward</div>
                </div>
            </div>
            <p style="margin:0 0 14px;color:#475569;font-size:0.95rem;">
                <strong>Total sold weight (after your process, for allocation):</strong> ${formatNumber(summary.total_kg)} kg
                <span style="color:#64748b;"> (${summary.line_count || 0} sales lines)</span>
            </p>
            ${summary.harvest_data_note ? `<p style="margin:0 0 10px;font-size:0.85rem;color:#b45309;"><strong>Note:</strong> ${summary.harvest_data_note}</p>` : ''}
            ${summary.unattributed_kg > 0 ? `<p style="margin:0 0 10px;font-size:0.85rem;color:#b45309;"><strong>Note:</strong> ${formatNumber(summary.unattributed_kg)} kg on rows with no linked product (shown under Other).</p>` : ''}
            ${scopeP}
            ${pctNote}
            ${ofNote}
            <p style="margin:0 0 8px;font-size:0.85rem;color:#64748b;">
                ${summary.weight_basis_note || 'Sales quantity (kg); wastage is not subtracted.'}
            </p>
            <table class="table" style="margin:0;background:#fff;">
                <thead>
                    <tr>
                        <th>Bucket</th>
                        <th style="text-align:right;">Inhouse kg</th>
                        <th style="text-align:right;">Outsourced kg</th>
                        <th style="text-align:right;">Total kg</th>
                        <th style="text-align:right;">Share</th>
                    </tr>
                </thead>
                <tbody>${(summary.distribution || []).map(d => `
        <tr>
            <td><strong>${d.label}</strong></td>
            <td style="text-align:right;">${formatNumber(d.inhouse_kg != null ? d.inhouse_kg : 0)}</td>
            <td style="text-align:right;">${formatNumber(d.outsourced_kg != null ? d.outsourced_kg : 0)}</td>
            <td style="text-align:right;">${formatNumber(d.kg)}</td>
            <td style="text-align:right;">${d.percent}%</td>
        </tr>
    `).join('')}</tbody>
            </table>
            ${(summary.product_lines && summary.product_lines.length) ? (() => {
                const hasInward = summary.product_lines.some(p => p.inhouse_inward_kg != null);
                return `
                <details style="margin-top:10px;" open>
                    <summary style="cursor:pointer;font-weight:600;color:#374151;">Product breakdown (${summary.product_lines.length} products)</summary>
                    <div style="overflow-x:auto;">
                    <table class="table" style="margin-top:10px;background:#fff;font-size:0.85rem;">
                        <thead>
                            <tr>
                                <th>Product</th>
                                <th>Bucket</th>
                                <th style="text-align:right;">Inhouse sold kg</th>
                                <th style="text-align:right;">Outsourced sold kg</th>
                                ${hasInward ? '<th style="text-align:right;">Harvest inward</th><th style="text-align:right;">Purchase inward</th><th style="text-align:right;">Farm wastage</th>' : ''}
                                <th style="text-align:right;">Row total kg</th>
                                <th style="text-align:right;">In bucket</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${summary.product_lines.map(p => `
                                <tr>
                                    <td>${p.product}</td>
                                    <td><span style="font-size:0.78rem;background:#e2e8f0;padding:2px 6px;border-radius:4px;">${formatCategoryLabel(p.bucket)}</span></td>
                                    <td style="text-align:right;">${formatNumber(p.inhouse_kg)}</td>
                                    <td style="text-align:right;">${formatNumber(p.outsourced_kg)}</td>
                                    ${hasInward ? `<td style="text-align:right;">${p.inhouse_inward_kg != null ? formatNumber(p.inhouse_inward_kg) : '—'}</td><td style="text-align:right;">${p.outsourced_inward_kg != null ? formatNumber(p.outsourced_inward_kg) : '—'}</td><td style="text-align:right;">${p.inhouse_wastage_kg != null ? formatNumber(p.inhouse_wastage_kg) : '—'}</td>` : ''}
                                    <td style="text-align:right;">${formatNumber(p.row_total_kg)}</td>
                                    <td style="text-align:right;"><strong>${formatNumber(p.counted_in_bucket_kg)}</strong></td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                    </div>
                </details>`;
            })() : ''}
        </div>
    `;
}

// Display sales data
async function saveMonthlyWastageOverride(month) {
    const m = normalizeMonthKey(month);
    if (!m) {
        showAlert('Select a valid month (YYYY-MM)', 'error');
        return;
    }
    const inEl = document.getElementById('wastage-override-inhouse');
    const outEl = document.getElementById('wastage-override-outsourced');
    const payload = { month: m };
    if (inEl && inEl.value !== '') {
        const v = parseFloat(inEl.value);
        if (Number.isNaN(v) || v < 0) {
            showAlert('Inhouse wastage must be a non-negative number', 'error');
            return;
        }
        payload.inhouse_wastage_kg = v;
    } else {
        payload.inhouse_wastage_kg = null;
    }
    if (outEl && outEl.value !== '') {
        const v = parseFloat(outEl.value);
        if (Number.isNaN(v) || v < 0) {
            showAlert('Outsourced wastage must be a non-negative number', 'error');
            return;
        }
        payload.outsourced_wastage_kg = v;
    } else {
        payload.outsourced_wastage_kg = null;
    }
    try {
        const res = await fetch(`${API_BASE}/monthly-wastage-override`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
            throw new Error(data.detail || data.message || 'Save failed');
        }
        showAlert(data.message || 'Wastage saved. Re-run allocation to apply.', 'success');
        await loadSales();
    } catch (e) {
        console.error(e);
        showAlert(e.message || 'Failed to save wastage override', 'error');
    }
}

async function clearMonthlyWastageOverride(month) {
    const m = normalizeMonthKey(month);
    if (!m) return;
    const inEl = document.getElementById('wastage-override-inhouse');
    const outEl = document.getElementById('wastage-override-outsourced');
    if (inEl) inEl.value = '';
    if (outEl) outEl.value = '';
    await saveMonthlyWastageOverride(m);
}

window.saveMonthlyWastageOverride = saveMonthlyWastageOverride;
window.clearMonthlyWastageOverride = clearMonthlyWastageOverride;

function displaySales(sales) {
    const container = document.getElementById('sales-table');
    
    if (sales.length === 0) {
        container.innerHTML = '<p>No sales data found for this month. Add some sales data!</p>';
        return;
    }
    
    let tableHTML = `
        <table class="table">
            <thead>
                <tr>
                    <th>Product</th>
                    <th>Source</th>
                    <th>Quantity</th>
                    <th>Sale Price</th>
                    <th>Direct Cost</th>
                    <th>Revenue</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    sales.forEach(sale => {
        const revenue = sale.quantity * sale.sale_price;
        const qtyText = formatQtyDisplay(sale.product_name, sale.product?.unit || sale.unit, sale.quantity);
        const src = sale.product_source || sale.product?.source || '';
        const badgeClass = src === 'outsourced' ? 'badge-info' : 'badge-success';
        const srcLabel = src ? src.charAt(0).toUpperCase() + src.slice(1) : '—';
        tableHTML += `
            <tr data-sale-id="${sale.id}">
                <td><strong>${sale.product_name}</strong></td>
                <td>${src ? `<span class="badge ${badgeClass}">${srcLabel}</span>` : '—'}</td>
                <td>${qtyText}</td>
                <td>₹${formatNumber(sale.sale_price)}</td>
                <td>₹${formatNumber(sale.direct_cost)}</td>
                <td>₹${formatNumber(revenue)}</td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="editSales(${sale.id})">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteSales(${sale.id})">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    
    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
}

// Load costs
async function loadCosts() {
    try {
        showLoading('costs-table');
        
        const response = await fetch(`${API_BASE}/costs`);
        const costs = await response.json();
        cachedCosts = costs || [];
        cachedCostMonths = [...new Set(cachedCosts.map(c => normalizeMonthKey(c.month)).filter(Boolean))];
        refreshAllocationMonthOptionsFromCache();
        
        displayCosts(cachedCosts);
        
    } catch (error) {
        console.error('Error loading costs:', error);
        showAlert('Error loading costs', 'error');
    }
}

// Initialize Cost Items function removed - use cost sheet upload instead

// Display costs with FIXED TEMPLATE FORMAT and inline editing
function displayCosts(costs) {
    const container = document.getElementById('costs-table');
    costs = costs || [];
    const activeMonth = getActiveCostMonth(costs);

    const costMap = {};
    costs.forEach(cost => {
        const templateKey = resolveTemplateKey(cost);
        if (!templateKey) return;
        const month = normalizeMonthKey(cost.month);
        if (month && month !== activeMonth) return;
        if (!costMap[templateKey]) costMap[templateKey] = cost;
        else if ((cost.amount || 0) > (costMap[templateKey].amount || 0)) {
            costMap[templateKey] = cost;
        }
    });

    let html = `
        <div style="margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
            <div>
                <h4 style="margin: 0;">Cost Allocation Template</h4>
                <span style="font-size: 12px; color: #6b7280;">Month: <strong>${activeMonth}</strong> — enter amounts manually or upload P&L</span>
            </div>
            <button class="btn btn-success btn-sm" onclick="saveAllCostChanges()">
                <i class="fas fa-save"></i> Save All Changes
            </button>
        </div>
        <table class="table" style="width: 100%;">
            <thead>
                <tr style="background: #1f2937; color: white;">
                    <th style="width: 35%;">Cost Category</th>
                    <th style="width: 20%;">Amount (₹)</th>
                    <th style="width: 25%;">Applies To</th>
                    <th style="width: 20%;">Actions</th>
                </tr>
            </thead>
            <tbody>
    `;

    COST_TEMPLATE.forEach(template => {
        let cost = costMap[template.key];
        let amount = cost ? cost.amount : 0;
        let costId = cost ? cost.id : null;

        if (template.key === 'fixed_cost_cat_ii') {
            amount = getFc2TotalAmount(costs, activeMonth);
            const pooled = getFc2CostsForMonth(costs, activeMonth).find(
                c => (c.name || '').trim().toUpperCase() === 'FIXED COST CAT - II'
            );
            costId = pooled?.id || null;
        }
        const appliesTo = cost ? cost.applies_to : template.defaultAppliesTo;
        const indent = template.level === 1 ? 'padding-left: 40px;' : '';
        const bgColor = template.level === 0 ? 'background: #f3f4f6;' : '';
        const fontWeight = template.level === 0 ? 'font-weight: 600;' : '';
        
        // Skip parent category row (Variable Cost header)
        if (template.isParent) {
            html += `
                <tr style="${bgColor} ${fontWeight}">
                    <td colspan="4" style="padding: 12px; font-size: 14px; border-bottom: 2px solid #d1d5db;">
                        <strong>${template.label}</strong>
                    </td>
                </tr>
            `;
            return;
        }
        
        html += `
            <tr data-cost-id="${costId || ''}" data-template-key="${template.key}" data-month="${activeMonth}" style="${bgColor}">
                <td style="${indent} ${fontWeight}">${template.label}</td>
                <td>
                    <input type="number" class="form-control cost-amount-input" 
                           data-cost-id="${costId || ''}" 
                           data-template-key="${template.key}"
                           value="${amount.toFixed(2)}" 
                           step="0.01"
                           ${template.key === 'fixed_cost_cat_ii' ? 'oninput="updateFc2SplitPreview()"' : ''}
                           style="width: 150px; text-align: right; font-weight: 500;">
                </td>
                <td>
                    <div style="display: flex; gap: 8px; align-items: center;">
                        <label style="display: flex; align-items: center; gap: 4px; cursor: pointer; font-size: 12px;">
                            <input type="checkbox" class="applies-to-checkbox" 
                                   data-cost-id="${costId || ''}"
                                   data-template-key="${template.key}"
                                   data-type="inhouse"
                                   ${appliesTo === 'inhouse' || appliesTo === 'both' ? 'checked' : ''}>
                            <span style="padding: 2px 6px; background: #dbeafe; color: #1e40af; border-radius: 4px;">Inhouse</span>
                        </label>
                        <label style="display: flex; align-items: center; gap: 4px; cursor: pointer; font-size: 12px;">
                            <input type="checkbox" class="applies-to-checkbox"
                                   data-cost-id="${costId || ''}"
                                   data-template-key="${template.key}"
                                   data-type="outsourced"
                                   ${appliesTo === 'outsourced' || appliesTo === 'both' ? 'checked' : ''}>
                            <span style="padding: 2px 6px; background: #fef3c7; color: #92400e; border-radius: 4px;">Outsourced</span>
                        </label>
                    </div>
                </td>
                <td>
                    ${costId ? `
                        <button class="btn btn-sm btn-danger" onclick="deleteCost(${costId})" title="Delete">
                            <i class="fas fa-trash"></i>
                        </button>
                    ` : `
                        <span style="color: #9ca3af; font-size: 11px;">Manual entry</span>
                    `}
                </td>
            </tr>
        `;

        if (template.key === 'fixed_cost_cat_ii') {
            html += renderFc2SplitSection(costs, activeMonth, amount);
        }
    });

    html += '</tbody></table>';
    
    // Add totals summary
    let totalCosts = 0;
    let inhouseCosts = 0;
    let outsourcedCosts = 0;
    costs.forEach(c => {
        totalCosts += c.amount || 0;
        if (c.applies_to === 'inhouse') inhouseCosts += c.amount || 0;
        else if (c.applies_to === 'outsourced') outsourcedCosts += c.amount || 0;
        else { inhouseCosts += (c.amount || 0) / 2; outsourcedCosts += (c.amount || 0) / 2; }
    });
    
    html += `
        <div style="margin-top: 20px; padding: 15px; background: #f9fafb; border-radius: 8px; display: flex; gap: 30px;">
            <div>
                <span style="color: #6b7280; font-size: 12px;">Total Costs</span>
                <div style="font-size: 18px; font-weight: 700;">₹${formatNumber(totalCosts)}</div>
            </div>
            <div>
                <span style="color: #6b7280; font-size: 12px;">Inhouse Allocated</span>
                <div style="font-size: 18px; font-weight: 700; color: #1e40af;">₹${formatNumber(inhouseCosts)}</div>
            </div>
            <div>
                <span style="color: #6b7280; font-size: 12px;">Outsourced Allocated</span>
                <div style="font-size: 18px; font-weight: 700; color: #92400e;">₹${formatNumber(outsourcedCosts)}</div>
            </div>
        </div>
    `;

    container.innerHTML = html;
}

// Save all cost changes (amounts and applies_to) — updates existing or creates new rows
async function saveAllCostChanges() {
    const rows = document.querySelectorAll('tr[data-template-key]');
    const updates = [];
    
    rows.forEach(row => {
        const costId = row.dataset.costId;
        const templateKey = row.dataset.templateKey;
        const month = row.dataset.month || getActiveCostMonth(cachedCosts);
        
        const amountInput = row.querySelector('.cost-amount-input');
        const inhouseCheckbox = row.querySelector('.applies-to-checkbox[data-type="inhouse"]');
        const outsourcedCheckbox = row.querySelector('.applies-to-checkbox[data-type="outsourced"]');
        
        const amount = parseFloat(amountInput?.value || 0);
        const inhouse = inhouseCheckbox?.checked;
        const outsourced = outsourcedCheckbox?.checked;
        
        let appliesTo = 'both';
        if (inhouse && !outsourced) appliesTo = 'inhouse';
        else if (!inhouse && outsourced) appliesTo = 'outsourced';
        else if (inhouse && outsourced) appliesTo = 'both';
        else if (!inhouse && !outsourced) appliesTo = 'both';
        
        const hasId = costId && costId !== '';
        if (templateKey === 'fixed_cost_cat_ii') return;
        if (!hasId && amount <= 0) return;
        
        updates.push({
            id: hasId ? parseInt(costId, 10) : null,
            template_key: templateKey,
            month,
            amount,
            applies_to: appliesTo
        });
    });
    
    const fc2Input = document.querySelector('tr[data-template-key="fixed_cost_cat_ii"] .cost-amount-input');
    const fc2Total = parseFloat(fc2Input?.value || 0);

    if (updates.length === 0 && fc2Total <= 0) {
        showAlert('No changes to save', 'info');
        return;
    }
    
    try {
        if (updates.length > 0) {
            const response = await fetch(`${API_BASE}/costs/bulk-update`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ updates })
            });
            
            const result = await response.json().catch(() => ({}));
            if (!response.ok || result.success === false) {
                let msg = result.message || result.detail || 'Unknown error';
                if (Array.isArray(msg)) {
                    msg = msg.map(e => e.msg || JSON.stringify(e)).join('; ');
                } else if (typeof msg === 'object') {
                    msg = JSON.stringify(msg);
                }
                showAlert('Failed to save: ' + msg, 'error');
                return;
            }
        }

        if (fc2Total > 0) {
            const splitOk = await applyFixedCostIISplits();
            if (splitOk === false) return;
        }

        showAlert('Costs saved successfully', 'success');
        loadCosts();
    } catch (error) {
        console.error('Save error:', error);
        showAlert('Failed to save changes', 'error');
    }
}

// Apply user-defined split for FIXED COST CAT - II (50/25/10/15 default)
async function applyFixedCostIISplits() {
    try {
        const strawberryPct = parseFloat(document.getElementById('fc2-strawberry-pct')?.value || '0');
        const greensPct = parseFloat(document.getElementById('fc2-greens-pct')?.value || '0');
        const openFieldPct = parseFloat(document.getElementById('fc2-openfield-pct')?.value || '0');
        const aggregationPct = parseFloat(document.getElementById('fc2-aggregation-pct')?.value || '0');

        const inputPcts = [strawberryPct, greensPct, openFieldPct, aggregationPct];
        if (inputPcts.some(v => v < 0 || Number.isNaN(v))) {
            showAlert('Fixed Cost II percentages must be non-negative numbers.', 'error');
            return false;
        }
        const totalPct = inputPcts.reduce((a, b) => a + b, 0);
        if (totalPct <= 0) {
            showAlert('Enter at least one percentage greater than 0.', 'error');
            return false;
        }

        const normalized = inputPcts.map(v => (v / totalPct) * 100);
        if (Math.abs(totalPct - 100) > 0.01) {
            showAlert(`Percentages normalized from ${totalPct.toFixed(1)}% to 100%.`, 'info');
        }

        const fc2Input = document.querySelector('tr[data-template-key="fixed_cost_cat_ii"] .cost-amount-input');
        const totalFc2 = parseFloat(fc2Input?.value || 0);
        if (totalFc2 <= 0) {
            showAlert('Enter a Fixed Cost II total amount before applying split.', 'error');
            return false;
        }

        const fc2Month = document.querySelector('tr[data-template-key="fixed_cost_cat_ii"]')?.dataset.month
            || getActiveCostMonth(cachedCosts);
        const fixedCosts = getFc2CostsForMonth(cachedCosts, fc2Month);

        const pooledFc2Cost = fixedCosts.find(c =>
            (c.name || '').trim().toUpperCase() === 'FIXED COST CAT - II'
        );

        const findBucket = (cfg) => fixedCosts.find(c => (c.name || '').toUpperCase() === cfg.name.toUpperCase());

        const targets = FC2_BUCKET_CONFIG.map((cfg, idx) => ({
            pct: normalized[idx],
            cost: findBucket(cfg),
            label: cfg.label,
            cfg,
        }));

        const positiveIdx = targets.map((t, i) => (t.pct > 0 ? i : -1)).filter(i => i >= 0);
        const amounts = [0, 0, 0, 0];
        let allocatedSum = 0;
        for (let j = 0; j < positiveIdx.length; j++) {
            const i = positiveIdx[j];
            if (j === positiveIdx.length - 1) {
                amounts[i] = Math.round((totalFc2 - allocatedSum) * 100) / 100;
            } else {
                const part = Math.round((totalFc2 * (targets[i].pct / 100)) * 100) / 100;
                amounts[i] = part;
                allocatedSum += part;
            }
        }
        const updates = targets.map((t, idx) => ({ ...t, amount: amounts[idx] }));

        for (const u of updates) {
            if (!u.cost) {
                const cfg = u.cfg;
                const createPayload = {
                    name: cfg.name,
                    amount: u.amount > 0 ? u.amount : 0.01,
                    applies_to: cfg.applies_to,
                    cost_type: cfg.cost_type,
                    basis: 'sales_kg',
                    month: fc2Month,
                    is_fixed: 'fixed',
                    category: 'fixed_cost_cat_ii',
                    source_file: 'manual'
                };
                const createRes = await fetch(`${API_BASE}/costs/`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(createPayload)
                });
                if (!createRes.ok) {
                    const err = await createRes.json().catch(() => ({}));
                    console.error(`Error creating Fixed Cost II ${u.label}:`, err);
                    showAlert(`Error creating Fixed Cost II - ${u.label} row.`, 'error');
                    return false;
                }
                const created = await createRes.json().catch(() => null);
                if (created && created.id) {
                    u.cost = created;
                } else {
                    showAlert(`Error creating Fixed Cost II - ${u.label} row.`, 'error');
                    return false;
                }
            }

            const payload = { amount: u.amount };
            const response = await fetch(`${API_BASE}/costs/${u.cost.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                showAlert(`Error updating Fixed Cost II (${u.label})`, 'error');
                return false;
            }
        }

        if (pooledFc2Cost) {
            await fetch(`${API_BASE}/costs/${pooledFc2Cost.id}`, { method: 'DELETE' });
        }

        return true;
    } catch (error) {
        console.error('Error applying Fixed Cost II split:', error);
        showAlert('Unexpected error while updating Fixed Cost II split.', 'error');
        return false;
    }
}

// Form handlers
function handleProductSubmit(e) {
    e.preventDefault();
    
    const productData = {
        name: document.getElementById('product-name').value,
        source: document.getElementById('product-source').value,
        unit: document.getElementById('product-unit').value,
        extra_info: document.getElementById('product-info').value || null
    };
    
    createProduct(productData);
}

function handleSalesSubmit(e) {
    e.preventDefault();
    
    const salesData = {
        product_id: parseInt(document.getElementById('sales-product').value),
        month: document.getElementById('sales-month').value,
        quantity: parseFloat(document.getElementById('sales-quantity').value),
        sale_price: parseFloat(document.getElementById('sales-price').value),
        direct_cost: parseFloat(document.getElementById('sales-direct-cost').value) || 0
    };
    
    createSales(salesData);
}

function handleCostSubmit(e) {
    e.preventDefault();
    
    const costData = {
        name: document.getElementById('cost-name').value,
        amount: parseFloat(document.getElementById('cost-amount').value),
        applies_to: getCheckedRadioValue('cost-applies-to'),
        cost_type: getCheckedRadioValue('cost-type'),
        basis: getCheckedRadioValue('cost-basis'),
        month: document.getElementById('cost-month').value,
        is_fixed: getCheckedRadioValue('cost-fixed'),
        category: document.getElementById('cost-category').value,
        allocation_pool: getCheckedRadioValue('cost-allocation-pool') || 'auto',
        allocation_denominator_kg: document.getElementById('cost-denominator-kg').value ? parseFloat(document.getElementById('cost-denominator-kg').value) : null
    };
    
    createCost(costData);
}

// API calls
async function createProduct(productData) {
    try {
        const response = await fetch(`${API_BASE}/products/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(productData)
        });
        
        if (response.ok) {
            showAlert('Product created successfully!', 'success');
            closeModal('product-modal');
            document.getElementById('product-form').reset();
            loadProducts();
        } else {
            const error = await response.json();
            showAlert(error.detail || 'Error creating product', 'error');
        }
    } catch (error) {
        showAlert('Error connecting to server', 'error');
    }
}

async function createSales(salesData) {
    try {
        const response = await fetch(`${API_BASE}/monthly-sales/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(salesData)
        });
        
        if (response.ok) {
            showAlert('Sales data created successfully!', 'success');
            closeModal('sales-modal');
            document.getElementById('sales-form').reset();
            loadSales();
        } else {
            const error = await response.json();
            showAlert(error.detail || 'Error creating sales data', 'error');
        }
    } catch (error) {
        showAlert('Error connecting to server', 'error');
    }
}

async function createCost(costData) {
    try {
        const response = await fetch(`${API_BASE}/costs/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(costData)
        });
        
        if (response.ok) {
            showAlert('Cost created successfully!', 'success');
            closeModal('cost-modal');
            document.getElementById('cost-form').reset();
            loadCosts();
        } else {
            const error = await response.json();
            showAlert(error.detail || 'Error creating cost', 'error');
        }
    } catch (error) {
        showAlert('Error connecting to server', 'error');
    }
}

// Modal functions
function showProductForm() {
    // Reset to "Add" mode
    document.getElementById('product-modal-title').textContent = 'Add Product';
    document.getElementById('product-form').removeAttribute('data-edit-id');
    document.getElementById('product-form').reset();
    document.getElementById('product-modal').classList.add('active');
}

function showSalesForm() {
    // Reset to "Add" mode
    document.getElementById('sales-modal-title').textContent = 'Add Sales Data';
    document.getElementById('sales-form').removeAttribute('data-edit-id');
    document.getElementById('sales-form').reset();
    document.getElementById('sales-modal').classList.add('active');
}

function showCostForm() {
    // Reset to "Add" mode
    document.getElementById('cost-modal-title').textContent = 'Add Cost';
    document.getElementById('cost-form').removeAttribute('data-edit-id');
    document.getElementById('cost-form').reset();
    setCheckedRadioValue('cost-applies-to', 'both');
    setCheckedRadioValue('cost-type', 'common');
    setCheckedRadioValue('cost-basis', 'sales_kg');
    setCheckedRadioValue('cost-fixed', 'variable');
    setCheckedRadioValue('cost-allocation-pool', 'auto');
    document.getElementById('cost-denominator-kg').value = '';
    document.getElementById('cost-modal').classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
    // Reset forms when closing
    if (modalId === 'product-modal') {
        document.getElementById('product-form').reset();
        document.getElementById('product-form').removeAttribute('data-edit-id');
        // Reset title to "Add Product"
        document.getElementById('product-modal-title').textContent = 'Add Product';
    } else if (modalId === 'sales-modal') {
        document.getElementById('sales-form').reset();
        document.getElementById('sales-form').removeAttribute('data-edit-id');
        // Reset title to "Add Sales Data"
        document.getElementById('sales-modal-title').textContent = 'Add Sales Data';
    } else if (modalId === 'cost-modal') {
        document.getElementById('cost-form').reset();
        document.getElementById('cost-form').removeAttribute('data-edit-id');
        // Reset title to "Add Cost"
        document.getElementById('cost-modal-title').textContent = 'Add Cost';
    }
}

// Form submission functions
async function submitProductForm(event) {
    event.preventDefault();
    
    const formData = {
        name: document.getElementById('product-name').value,
        source: document.getElementById('product-source').value,
        unit: document.getElementById('product-unit').value,
        extra_info: document.getElementById('product-info').value
    };
    
    const editId = document.getElementById('product-form').getAttribute('data-edit-id');
    const isEdit = editId !== null;
    
    try {
        const url = isEdit ? `${API_BASE}/products/${editId}` : `${API_BASE}/products/`;
        const method = isEdit ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        if (response.ok) {
            showAlert(`Product ${isEdit ? 'updated' : 'added'} successfully!`, 'success');
            closeModal('product-modal');
            loadProducts();
        } else {
            const error = await response.json();
            showAlert(`Error: ${error.detail}`, 'error');
        }
    } catch (error) {
        showAlert(`Error ${isEdit ? 'updating' : 'adding'} product`, 'error');
    }
}

async function submitSalesForm(event) {
    event.preventDefault();
    
    const formData = {
        product_id: parseInt(document.getElementById('sales-product').value),
        month: document.getElementById('sales-month').value,
        quantity: parseFloat(document.getElementById('sales-quantity').value),
        sale_price: parseFloat(document.getElementById('sales-price').value),
        direct_cost: parseFloat(document.getElementById('sales-direct-cost').value)
    };
    
    const editId = document.getElementById('sales-form').getAttribute('data-edit-id');
    const isEdit = editId !== null;
    
    try {
        const url = isEdit ? `${API_BASE}/monthly-sales/${editId}` : `${API_BASE}/monthly-sales/`;
        const method = isEdit ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        if (response.ok) {
            showAlert(`Sales ${isEdit ? 'updated' : 'added'} successfully!`, 'success');
            closeModal('sales-modal');
            loadSales();
        } else {
            const error = await response.json();
            showAlert(`Error: ${error.detail}`, 'error');
        }
    } catch (error) {
        showAlert(`Error ${isEdit ? 'updating' : 'adding'} sales`, 'error');
    }
}

async function submitCostForm(event) {
    event.preventDefault();
    
    const formData = {
        name: document.getElementById('cost-name').value,
        amount: parseFloat(document.getElementById('cost-amount').value),
        applies_to: getCheckedRadioValue('cost-applies-to'),
        cost_type: getCheckedRadioValue('cost-type'),
        basis: getCheckedRadioValue('cost-basis'),
        month: document.getElementById('cost-month').value,
        category: document.getElementById('cost-category').value,
        is_fixed: getCheckedRadioValue('cost-fixed'),
        allocation_pool: getCheckedRadioValue('cost-allocation-pool') || 'auto',
        allocation_denominator_kg: document.getElementById('cost-denominator-kg').value ? parseFloat(document.getElementById('cost-denominator-kg').value) : null
    };
    
    const editId = document.getElementById('cost-form').getAttribute('data-edit-id');
    const isEdit = editId !== null;
    
    try {
        const url = isEdit ? `${API_BASE}/costs/${editId}` : `${API_BASE}/costs/`;
        const method = isEdit ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        if (response.ok) {
            showAlert(`Cost ${isEdit ? 'updated' : 'added'} successfully!`, 'success');
            closeModal('cost-modal');
            loadCosts();
        } else {
            const error = await response.json();
            showAlert(`Error: ${error.detail}`, 'error');
        }
    } catch (error) {
        showAlert(`Error ${isEdit ? 'updating' : 'adding'} cost`, 'error');
    }
}

// Update product dropdowns
function updateProductDropdowns(products) {
    const salesProductSelect = document.getElementById('sales-product');
    salesProductSelect.innerHTML = '<option value="">Select Product</option>';
    
    products.forEach(product => {
        if (product.is_active) {
            const option = document.createElement('option');
            option.value = product.id;
            option.textContent = `${product.name} (${product.source})`;
            salesProductSelect.appendChild(option);
        }
    });
}

function formatCategoryLabel(category) {
    const labels = {
        purchase_accounts: 'PURCHASE ACCOUNTS',
        fixed_cost_cat_i: 'Fixed Cost Cat I',
        fixed_cost_cat_ii: 'Fixed Cost Cat II',
        variable_cost: 'Variable Cost',
        distribution_cost: 'Distribution Cost',
        marketing_expenses: 'Marketing Expenses',
        vehicle_running_cost: 'Vehicle Running Cost',
        others: 'Others',
        wastage: 'Wastage',
        general: 'General',
        open_field: 'Open Field',
        lettuce_greens: 'Lettuce / Greens',
        aggregation: 'Aggregation (Outsourced)',
        strawberry: 'Strawberry',
        other: 'Other (Inhouse)',
    };
    return labels[category] || category;
}

async function loadProductAllowlists() {
    const lettuceEl = document.getElementById('lettuce-greens-allowlist');
    const openFieldEl = document.getElementById('open-field-allowlist');
    if (!lettuceEl && !openFieldEl) return;
    try {
        const res = await fetch(`${API_BASE}/product-allowlists`);
        if (!res.ok) return;
        const data = await res.json();
        if (lettuceEl) lettuceEl.value = (data.lettuce_greens_products || []).join('\n');
        if (openFieldEl) {
            const of = data.open_field_products || data.open_field_extra_products || [];
            openFieldEl.value = of.join('\n');
        }
    } catch (e) {
        console.error('Failed to load allowlists', e);
    }
}

async function saveProductAllowlists() {
    const lettuceEl = document.getElementById('lettuce-greens-allowlist');
    const openFieldEl = document.getElementById('open-field-allowlist');
    const status = document.getElementById('allowlist-save-status');
    const lettuceNames = lettuceEl ? lettuceEl.value.split('\n').map(s => s.trim()).filter(Boolean) : [];
    const openFieldNames = openFieldEl ? openFieldEl.value.split('\n').map(s => s.trim()).filter(Boolean) : [];
    try {
        const res = await fetch(`${API_BASE}/product-allowlists`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                lettuce_greens_products: lettuceNames,
                open_field_products: openFieldNames.length ? openFieldNames : undefined,
            }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Save failed');
        if (status) status.textContent = `Saved ${data.lettuce_greens_count || lettuceNames.length} lettuce/greens · ${(data.open_field_products || []).length} open field`;
        showAlert('Product allowlists saved', 'success');
        loadSales();
        loadDashboardBucketCharts();
    } catch (e) {
        if (status) status.textContent = 'Save failed';
        showAlert('Could not save allowlists: ' + e.message, 'error');
    }
}

async function saveLettuceGreensAllowlist() {
    return saveProductAllowlists();
}

async function loadSectionMappings() {
    const container = document.getElementById('section-mappings-table');
    const badge = document.getElementById('mapping-count-badge');
    if (!container) return;
    try {
        const res = await fetch(`${API_BASE}/product-section-mappings`);
        if (!res.ok) throw new Error('Failed to load mappings');
        const data = await res.json();
        if (badge) badge.textContent = data.count ? `(${data.count} products)` : '(none uploaded)';
        if (!data.count) {
            container.innerHTML = '<p style="padding:32px;text-align:center;color:#94a3b8;">No mappings yet. Upload an Excel file with Section and Product columns.</p>';
            return;
        }
        let html = '<table class="schema-table"><thead><tr><th>Section</th><th>Products</th><th style="text-align:right;">Count</th></tr></thead><tbody>';
        for (const [section, products] of Object.entries(data.by_section || {})) {
            html += `<tr>
                <td class="col-name">${section}</td>
                <td class="col-sub" style="font-size:0.82rem;line-height:1.5;">${products.join(', ')}</td>
                <td style="text-align:right;font-weight:600;">${products.length}</td>
            </tr>`;
        }
        html += '</tbody></table>';
        container.innerHTML = html;
    } catch (e) {
        container.innerHTML = `<p style="padding:24px;text-align:center;color:#ef4444;">${e.message}</p>`;
    }
}

window.loadSectionMappings = loadSectionMappings;
window.saveProductAllowlists = saveProductAllowlists;

// Allocation functions
async function runAllocation() {
    const month = normalizeMonthKey(document.getElementById('allocation-month').value);
    const purchaseCostMode = getCheckedRadioValue('purchase-cost-mode') || 'direct';
    
    if (!month) {
        showAlert('Please select a month', 'error');
        return;
    }
    
    try {
        showLoading('allocation-results');
        
        const params = new URLSearchParams({ purchase_cost_mode: purchaseCostMode });
        const response = await fetch(`${API_BASE}/allocate/${encodeURIComponent(month)}?${params}`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            displayAllocationResults(result);
            showAlert('Allocation completed successfully!', 'success');
        } else {
            const error = await response.json();
            showAlert(error.detail || 'Error running allocation', 'error');
        }
    } catch (error) {
        showAlert('Error connecting to server', 'error');
    }
}

function displayAllocationResults(result) {
    const container = document.getElementById('allocation-results');
    const modeBanner = result.purchase_cost_mode_label
        ? `<div style="margin-bottom:16px;padding:12px 16px;background:#eff6ff;border-left:4px solid #3b82f6;border-radius:6px;color:#1e3a8a;font-size:0.9rem;"><strong>Purchase mode:</strong> ${result.purchase_cost_mode_label}</div>`
        : '';
    
    let html = `${modeBanner}
        <div class="stats-grid" style="margin-bottom: 20px;">
            <div class="stat-card revenue">
                <div class="stat-header">
                    <span class="stat-title">Total Revenue</span>
                    <i class="fas fa-chart-line stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_revenue)}</div>
            </div>
            <div class="stat-card costs">
                <div class="stat-header">
                    <span class="stat-title">Total Costs</span>
                    <i class="fas fa-dollar-sign stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_costs)}</div>
            </div>
            <div class="stat-card profit">
                <div class="stat-header">
                    <span class="stat-title">Net Profit</span>
                    <i class="fas fa-trophy stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_profit)}</div>
            </div>
        </div>
        
        <h3>Product-wise Allocation Results</h3>
        <table class="table">
            <thead>
                <tr>
                    <th>Product</th>
                    <th>Source</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Direct Cost</th>
                    <th>Allocated</th>
                    <th>Total Cost</th>
                    <th>Revenue</th>
                    <th>Profit</th>
                    <th>Margin</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    result.products.forEach(product => {
        const qtyText = formatQtyDisplay(product.product_name, product.unit, product.quantity);
        const productId = product.product_id || product.id;
        console.log('📦 Product data:', product, 'Product ID:', productId);
        html += `
            <tr class="product-row" style="cursor: pointer;" data-product-id="${productId}" onclick="showCostBreakdown(${productId})" title="Click to view cost breakdown">
                <td><strong>${product.product_name}</strong></td>
                <td><span class="badge ${product.source === 'inhouse' ? 'badge-success' : 'badge-info'}">${product.source}</span></td>
                <td>${qtyText}</td>
                <td>₹${product.sale_price}</td>
                <td>₹${formatNumber(product.direct_cost)}</td>
                <td>₹${formatNumber(product.allocated_costs)}</td>
                <td>₹${formatNumber(product.total_cost)}</td>
                <td>₹${formatNumber(product.revenue)}</td>
                <td class="${product.profit >= 0 ? 'text-success' : 'text-danger'}">₹${formatNumber(product.profit)}</td>
                <td>${product.profit_margin.toFixed(1)}%</td>
            </tr>
        `;
    });
    
    html += '</tbody></table>';

    if (result.cost_breakdown && Object.keys(result.cost_breakdown).length > 0) {
        const poolNote = result.purchase_accounts_pool_total
            ? `<p style="font-size:0.9rem;color:#64748b;margin:8px 0 12px;">PURCHASE ACCOUNTS pool (P&amp;L): ₹${formatNumber(result.purchase_accounts_pool_total)} — split across outsourced lines in direct mode.</p>`
            : '';
        html += `${poolNote}<h3 style="margin-top:24px;">Cost Breakdown by Category</h3>
        <table class="table"><thead><tr><th>Category</th><th style="text-align:right;">Amount</th></tr></thead><tbody>`;
        Object.entries(result.cost_breakdown)
            .sort((a, b) => b[1] - a[1])
            .forEach(([cat, amt]) => {
                html += `<tr><td><strong>${formatCategoryLabel(cat)}</strong></td><td style="text-align:right;">₹${formatNumber(amt)}</td></tr>`;
            });
        html += '</tbody></table>';
    }

    container.innerHTML = html;
    
    // Add event delegation for product rows (in case onclick doesn't work)
    const productRows = container.querySelectorAll('.product-row');
    productRows.forEach(row => {
        const productId = row.getAttribute('data-product-id');
        if (productId) {
            row.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                console.log('🖱️ Row clicked, productId:', productId);
                showCostBreakdown(parseInt(productId));
            });
        }
    });
}

// Cost Breakdown Modal Functions
async function showCostBreakdown(productId) {
    console.log('🔍 showCostBreakdown called with productId:', productId);
    
    if (!productId) {
        console.error('❌ No productId provided');
        showAlert('Error: Product ID is missing', 'error');
        return;
    }
    
    try {
        // Don't show loading for cost breakdown - we're opening a modal, not replacing content
        console.log('📡 Fetching from:', `${API_BASE}/product-cost-breakdown/${productId}`);
        
        const purchaseCostMode = getCheckedRadioValue('purchase-cost-mode') || 'direct';
        const month = normalizeMonthKey(document.getElementById('allocation-month')?.value || '');
        const params = new URLSearchParams({ purchase_cost_mode: purchaseCostMode });
        if (month) params.set('month', month);
        const response = await fetch(`${API_BASE}/product-cost-breakdown/${productId}?${params}`);
        
        console.log('📥 Response status:', response.status, response.statusText);
        
        if (!response.ok) {
            const errorText = await response.text();
            console.error('❌ Response error:', errorText);
            throw new Error(`Failed to fetch cost breakdown: ${response.status} ${response.statusText}`);
        }
        
        const breakdown = await response.json();
        console.log('✅ Breakdown received:', breakdown);
        displayCostBreakdownModal(breakdown);
    } catch (error) {
        console.error('❌ Error fetching cost breakdown:', error);
        showAlert('Error loading cost breakdown: ' + error.message, 'error');
    }
}

function displayCostBreakdownModal(breakdown) {
    console.log('📊 Displaying cost breakdown modal for:', breakdown.product_name);
    
    if (!breakdown || !breakdown.product_name) {
        console.error('❌ Invalid breakdown data:', breakdown);
        showAlert('Error: Invalid cost breakdown data', 'error');
        return;
    }
    
    const salesKg = breakdown.sales_kg || 0;
    const allocatedPerKg = salesKg > 0 ? (breakdown.total_allocated || 0) / salesKg : 0;

    // Create modal HTML
    const modalHtml = `
        <div id="costBreakdownModal" class="modal" style="display: block;">
            <div class="modal-content" style="max-width: 900px; max-height: 90vh; overflow-y: auto;">
                <div class="modal-header">
                    <h2>Cost Breakdown: ${breakdown.product_name}</h2>
                    <span class="close" onclick="closeCostBreakdownModal()">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="stats-grid" style="margin-bottom: 20px;">
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Quantity</span>
                            </div>
                            <div class="stat-value">${formatQtyDisplay(breakdown.product_name, breakdown.unit || 'kg', breakdown.quantity)}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Sale Price</span>
                            </div>
                            <div class="stat-value">₹${breakdown.sale_price}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Revenue</span>
                            </div>
                            <div class="stat-value">₹${formatNumber(breakdown.revenue)}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Total Cost</span>
                            </div>
                            <div class="stat-value">₹${formatNumber(breakdown.total_cost)}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Cost per KG</span>
                            </div>
                            <div class="stat-value">₹${breakdown.cost_per_kg.toFixed(2)}</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-header">
                                <span class="stat-title">Profit</span>
                            </div>
                            <div class="stat-value ${breakdown.profit >= 0 ? 'text-success' : 'text-danger'}">₹${formatNumber(breakdown.profit)}</div>
                        </div>
                    </div>
                    
                    <h3>Cost Breakdown by Category</h3>
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Category</th>
                                <th>Total Allocated</th>
                                <th>Allocated ₹/kg</th>
                                <th>Number of Costs</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${Object.entries(breakdown.costs_by_category).map(([category, data]) => `
                                <tr>
                                    <td><strong>${formatCategoryLabel(category)}</strong></td>
                                    <td>₹${formatNumber(data.total)}</td>
                                    <td>₹${formatNumber(data.per_kg || 0)}</td>
                                    <td>${data.costs.length}</td>
                                </tr>
                            `).join('')}
                            ${(breakdown.purchase_cost > 0 && !breakdown.costs_by_category.purchase_accounts) ? `
                                <tr>
                                    <td><strong>PURCHASE ACCOUNTS</strong></td>
                                    <td>₹${formatNumber(breakdown.purchase_cost)}</td>
                                    <td>₹${salesKg > 0 ? formatNumber(breakdown.purchase_cost / salesKg) : '0'}</td>
                                    <td>1</td>
                                </tr>
                            ` : ''}
                        </tbody>
                    </table>
                    
                    <h3>Cost Breakdown by Type</h3>
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin-bottom: 20px;">
                        <div>
                            <h4>Inhouse Only Costs</h4>
                            <ul style="list-style: none; padding: 0;">
                                ${breakdown.costs_by_type.inhouse_only.map(cost => `
                                    <li style="padding: 5px 0; border-bottom: 1px solid #eee;">
                                        <strong>${cost.cost_name}</strong><br>
                                        <small>₹${formatNumber(cost.amount)} | ₹${formatNumber(cost.amount_per_kg || 0)}/kg (${cost.basis})</small>
                                    </li>
                                `).join('')}
                                ${breakdown.costs_by_type.inhouse_only.length === 0 ? '<li>None</li>' : ''}
                            </ul>
                        </div>
                        <div>
                            <h4>Outsourced Only Costs</h4>
                            <ul style="list-style: none; padding: 0;">
                                ${breakdown.costs_by_type.outsourced_only.map(cost => `
                                    <li style="padding: 5px 0; border-bottom: 1px solid #eee;">
                                        <strong>${cost.cost_name}</strong><br>
                                        <small>₹${formatNumber(cost.amount)} | ₹${formatNumber(cost.amount_per_kg || 0)}/kg (${cost.basis})</small>
                                    </li>
                                `).join('')}
                                ${breakdown.costs_by_type.outsourced_only.length === 0 ? '<li>None</li>' : ''}
                            </ul>
                        </div>
                        <div>
                            <h4>Common Costs (Both)</h4>
                            <ul style="list-style: none; padding: 0;">
                                ${breakdown.costs_by_type.common.map(cost => `
                                    <li style="padding: 5px 0; border-bottom: 1px solid #eee;">
                                        <strong>${cost.cost_name}</strong><br>
                                        <small>₹${formatNumber(cost.amount)} | ₹${formatNumber(cost.amount_per_kg || 0)}/kg (${cost.basis})</small>
                                    </li>
                                `).join('')}
                                ${breakdown.costs_by_type.common.length === 0 ? '<li>None</li>' : ''}
                            </ul>
                        </div>
                    </div>
                    
                    <h3>Detailed Cost Allocation</h3>
                    <p style="margin: 0 0 12px 0; color: #6b7280; font-size: 13px;">
                        Sales Qty: <strong>${formatNumber(salesKg)}</strong> kg
                        | Allocated Cost per kg: <strong>₹${formatNumber(allocatedPerKg)}</strong>
                    </p>
                    <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:10px; margin-bottom:14px;">
                        ${Object.entries(breakdown.costs_by_category).map(([category, data]) => `
                            <div style="border:1px solid #e5e7eb; border-radius:8px; padding:10px; background:#fafafa;">
                                <div style="font-size:12px; color:#6b7280;">${category}</div>
                                <div style="font-size:16px; font-weight:700;">₹${formatNumber(data.total || 0)}</div>
                                <div style="font-size:12px; color:#374151;">₹${formatNumber(data.per_kg || 0)}/kg</div>
                            </div>
                        `).join('')}
                    </div>
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Cost Name</th>
                                <th>Category</th>
                                <th>Applies To</th>
                                <th>Basis</th>
                                <th>Allocated Amount</th>
                                <th>Allocated ₹/kg</th>
                                <th>Total Cost Amount</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${breakdown.detailed_costs.map(cost => `
                                <tr>
                                    <td><strong>${cost.cost_name}</strong></td>
                                    <td>${cost.category}</td>
                                    <td><span class="badge ${cost.applies_to === 'inhouse' ? 'badge-success' : cost.applies_to === 'outsourced' ? 'badge-info' : 'badge-secondary'}">${cost.applies_to}</span></td>
                                    <td>${cost.basis}</td>
                                    <td>₹${formatNumber(cost.amount)}</td>
                                    <td>₹${formatNumber(cost.amount_per_kg || 0)}</td>
                                    <td>₹${formatNumber(cost.total_cost_amount)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                        <tfoot>
                            <tr style="font-weight: bold; background-color: #f5f5f5;">
                                <td colspan="4">Total Allocated Costs</td>
                                <td>₹${formatNumber(breakdown.total_allocated)}</td>
                                <td>₹${formatNumber(allocatedPerKg)}</td>
                                <td></td>
                            </tr>
                            <tr style="font-weight: bold; background-color: #e8f5e9;">
                                <td colspan="4">Direct Cost</td>
                                <td>₹${formatNumber(breakdown.direct_cost)}</td>
                                <td></td>
                            </tr>
                            <tr style="font-weight: bold; background-color: #fff3e0;">
                                <td colspan="4">Total Cost</td>
                                <td>₹${formatNumber(breakdown.total_cost)}</td>
                                <td></td>
                            </tr>
                        </tfoot>
                    </table>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" onclick="closeCostBreakdownModal()">Close</button>
                </div>
            </div>
        </div>
    `;
    
    // Remove any existing modal first
    const existingModal = document.getElementById('costBreakdownModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Add modal to page
    const modalContainer = document.createElement('div');
    modalContainer.innerHTML = modalHtml;
    const modalElement = modalContainer.firstElementChild;
    document.body.appendChild(modalElement);
    
    // Ensure modal is visible
    console.log('✅ Modal added to DOM');
    
    // Add close button handler
    const closeBtn = modalElement.querySelector('.close');
    if (closeBtn) {
        closeBtn.addEventListener('click', closeCostBreakdownModal);
    }
}

function closeCostBreakdownModal() {
    const modal = document.getElementById('costBreakdownModal');
    if (modal) {
        modal.remove();
        console.log('✅ Modal closed');
    }
}

// Close modal when clicking outside (but preserve existing onclick handlers)
document.addEventListener('click', function(event) {
    const modal = document.getElementById('costBreakdownModal');
    if (modal && event.target === modal) {
        closeCostBreakdownModal();
    }
});

// Report functions
async function generateReport() {
    try {
        showLoading('report-results');
        
        // Get all data instead of month-based report
        const [salesResponse, costsResponse] = await Promise.all([
            fetch(`${API_BASE}/sales`),
            fetch(`${API_BASE}/costs`)
        ]);
        
        if (!salesResponse.ok) {
            const errText = await salesResponse.text();
            throw new Error(errText && errText.length < 150 ? errText : 'Sales request failed');
        }
        if (!costsResponse.ok) {
            const errText = await costsResponse.text();
            throw new Error(errText && errText.length < 150 ? errText : 'Costs request failed');
        }
        
        const sales = await salesResponse.json();
        const costs = await costsResponse.json();
        
        // Generate report from all data
        const result = generateReportFromData(sales, costs);
        displayReportResults(result);
        
    } catch (error) {
        console.error('Error generating report:', error);
        showAlert('Error generating report: ' + (error.message || 'Unknown error'), 'error');
    }
}

function generateReportFromData(sales, costs) {
    // Calculate totals
    const totalRevenue = sales.reduce((sum, sale) => sum + (sale.quantity * sale.sale_price), 0);
    const totalDirectCosts = sales.reduce((sum, sale) => sum + sale.direct_cost, 0);
    const totalSharedCosts = costs.reduce((sum, cost) => sum + cost.amount, 0);
    const totalCosts = totalDirectCosts + totalSharedCosts;
    const totalProfit = totalRevenue - totalCosts;
    
    // Group by product
    const productStats = {};
    sales.forEach(sale => {
        const productName = sale.product_name;
        if (!productStats[productName]) {
            productStats[productName] = {
                name: productName,
                quantity: 0,
                revenue: 0,
                direct_cost: 0,
                source: sale.product?.source || 'unknown'
            };
        }
        productStats[productName].quantity += sale.quantity;
        productStats[productName].revenue += sale.quantity * sale.sale_price;
        productStats[productName].direct_cost += sale.direct_cost;
    });
    
    // Calculate top products
    const topProducts = Object.values(productStats)
        .sort((a, b) => b.revenue - a.revenue)
        .slice(0, 10);
    
    return {
        total_revenue: totalRevenue,
        total_costs: totalCosts,
        total_profit: totalProfit,
        total_direct_costs: totalDirectCosts,
        total_shared_costs: totalSharedCosts,
        top_products: topProducts,
        product_count: Object.keys(productStats).length,
        sales_count: sales.length
    };
}

function displayReportResults(result) {
    const container = document.getElementById('report-results');
    
    let html = `
        <div class="stats-grid" style="margin-bottom: 20px;">
            <div class="stat-card revenue">
                <div class="stat-header">
                    <span class="stat-title">Total Revenue</span>
                    <i class="fas fa-chart-line stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_revenue)}</div>
            </div>
            <div class="stat-card costs">
                <div class="stat-header">
                    <span class="stat-title">Total Costs</span>
                    <i class="fas fa-dollar-sign stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_costs)}</div>
            </div>
            <div class="stat-card profit">
                <div class="stat-header">
                    <span class="stat-title">Net Profit</span>
                    <i class="fas fa-trophy stat-icon"></i>
                </div>
                <div class="stat-value">₹${formatNumber(result.total_profit)}</div>
            </div>
            <div class="stat-card products">
                <div class="stat-header">
                    <span class="stat-title">Profit Margin</span>
                    <i class="fas fa-percentage stat-icon"></i>
                </div>
                <div class="stat-value">${result.profit_margin.toFixed(1)}%</div>
            </div>
        </div>
        
        <h3>Detailed Product Analysis</h3>
        <table class="table">
            <thead>
                <tr>
                    <th>Product</th>
                    <th>Source</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>Direct Cost</th>
                    <th>Allocated</th>
                    <th>Total Cost</th>
                    <th>Revenue</th>
                    <th>Profit</th>
                    <th>Margin</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    result.products.forEach(product => {
        const qtyText = formatQtyDisplay(product.product_name, product.unit, product.quantity);
        html += `
            <tr>
                <td><strong>${product.product_name}</strong></td>
                <td><span class="badge ${product.source === 'inhouse' ? 'badge-success' : 'badge-info'}">${product.source}</span></td>
                <td>${qtyText}</td>
                <td>₹${product.sale_price}</td>
                <td>₹${formatNumber(product.direct_cost)}</td>
                <td>₹${formatNumber(product.allocated_costs)}</td>
                <td>₹${formatNumber(product.total_cost)}</td>
                <td>₹${formatNumber(product.revenue)}</td>
                <td class="${product.profit >= 0 ? 'text-success' : 'text-danger'}">₹${formatNumber(product.profit)}</td>
                <td>${product.profit_margin.toFixed(1)}%</td>
            </tr>
        `;
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
}

// Export functions
async function exportReport() {
    try {
        // Get all data and generate CSV
        const [salesResponse, costsResponse] = await Promise.all([
            fetch(`${API_BASE}/sales`),
            fetch(`${API_BASE}/costs`)
        ]);
        
        const sales = await salesResponse.json();
        const costs = await costsResponse.json();
        
        // Generate CSV content
        const csvContent = generateCSVContent(sales, costs);
        
        // Create download link
        const blob = new Blob([csvContent], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `report_all_data.csv`;
        link.click();
        
        // Clean up
        window.URL.revokeObjectURL(url);
        
        showAlert('Report exported successfully!', 'success');
        
    } catch (error) {
        console.error('Error exporting report:', error);
        showAlert('Error exporting report', 'error');
    }
}

function generateCSVContent(sales, costs) {
    let csv = 'Product,Source,Quantity,Price,Revenue,Direct Cost,Month\n';
    
    sales.forEach(sale => {
        const productName = sale.product_name || 'Unknown';
        const source = sale.product?.source || 'unknown';
        const quantity = sale.quantity || 0;
        const price = sale.sale_price || 0;
        const revenue = quantity * price;
        const directCost = sale.direct_cost || 0;
        const month = sale.month || 'Unknown';
        
        csv += `"${productName}","${source}",${quantity},${price},${revenue},${directCost},"${month}"\n`;
    });
    
    csv += '\nCosts\n';
    csv += 'Name,Amount,Type,Month\n';
    
    costs.forEach(cost => {
        csv += `"${cost.name}",${cost.amount},"${cost.type}","${cost.month}"\n`;
    });
    
    return csv;
}

// Utility functions
function formatNumber(num) {
    if (num === undefined || num === null || isNaN(num)) {
        return '0.00';
    }
    return new Intl.NumberFormat('en-IN').format(Number(num).toFixed(2));
}

function showLoading(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
    container.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <p>Loading...</p>
        </div>
    `;
    }
}

function hideLoading(containerId) {
    const container = document.getElementById(containerId);
    // For cost breakdown, we don't want to clear the allocation results
    // Just remove any loading indicator if present
    if (container) {
        const loadingDiv = container.querySelector('.loading');
        if (loadingDiv) {
            loadingDiv.remove();
        }
    }
}

function showAlert(message, type) {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
        ${message}
    `;
    
    const content = document.querySelector('.content-area');
    content.insertBefore(alertDiv, content.firstChild);
    
    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}

function refreshData() {
    if (currentTab === 'dashboard') {
        loadDashboardData();
    } else if (currentTab === 'products') {
        loadProducts();
    } else if (currentTab === 'sales') {
        loadSales();
    } else if (currentTab === 'costs') {
        loadCosts();
    }
}

// Edit/Delete functions
async function editProduct(id) {
    try {
        const response = await fetch(`${API_BASE}/products/${id}`);
        if (response.ok) {
            const product = await response.json();
            
            // Change title to "Edit Product"
            document.getElementById('product-modal-title').textContent = 'Edit Product';
            
            // Populate form with existing data
            document.getElementById('product-name').value = product.name;
            document.getElementById('product-source').value = product.source;
            document.getElementById('product-unit').value = product.unit;
            document.getElementById('product-info').value = product.extra_info || '';
            
            // Store the ID for update
            document.getElementById('product-form').setAttribute('data-edit-id', id);
            
            // Show the modal
            document.getElementById('product-modal').classList.add('active');
        } else {
            showAlert('Error loading product data', 'error');
        }
    } catch (error) {
        showAlert('Error loading product data', 'error');
    }
}

async function deleteProduct(id) {
    if (confirm('Are you sure you want to delete this product?')) {
        try {
            const response = await fetch(`${API_BASE}/products/${id}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                showAlert('Product deleted successfully!', 'success');
                loadProducts();
            } else {
                showAlert('Error deleting product', 'error');
            }
        } catch (error) {
            showAlert('Error deleting product', 'error');
        }
    }
}

async function editSales(id) {
    try {
        const response = await fetch(`${API_BASE}/sales/${id}`);
        if (!response.ok) {
            showAlert('Error loading sales data', 'error');
            return;
        }
        const sale = await response.json();
        
        // Change title to "Edit Sales Data"
        document.getElementById('sales-modal-title').textContent = 'Edit Sales Data';
        
        // Populate form with existing data
        document.getElementById('sales-product').value = sale.product_id;
        document.getElementById('sales-month').value = sale.month || '';
        document.getElementById('sales-quantity').value = sale.quantity || 0;
        document.getElementById('sales-price').value = sale.sale_price || 0;
        document.getElementById('sales-direct-cost').value = sale.direct_cost || 0;
        
        // Store the ID for update
        document.getElementById('sales-form').setAttribute('data-edit-id', id);
        
        // Show the modal
        document.getElementById('sales-modal').classList.add('active');
    } catch (error) {
        showAlert('Error loading sales data', 'error');
    }
}

async function editCost(id) {
    try {
        const response = await fetch(`${API_BASE}/costs/id/${id}`);
        if (response.ok) {
            const cost = await response.json();
            
            // Change title to "Edit Cost"
            document.getElementById('cost-modal-title').textContent = 'Edit Cost';
            
            // Populate form with existing data
            document.getElementById('cost-name').value = cost.name;
            document.getElementById('cost-amount').value = cost.amount;
            setCheckedRadioValue('cost-applies-to', cost.applies_to);
            setCheckedRadioValue('cost-type', cost.cost_type);
            setCheckedRadioValue('cost-basis', cost.basis);
            document.getElementById('cost-month').value = cost.month;
            document.getElementById('cost-category').value = cost.category;
            setCheckedRadioValue('cost-fixed', cost.is_fixed);
            setCheckedRadioValue('cost-allocation-pool', cost.allocation_pool || 'auto');
            document.getElementById('cost-denominator-kg').value = cost.allocation_denominator_kg ?? '';
            
            // Store the ID for update
            document.getElementById('cost-form').setAttribute('data-edit-id', id);
            
            // Show the modal
            document.getElementById('cost-modal').classList.add('active');
        } else {
            showAlert('Error loading cost data', 'error');
        }
    } catch (error) {
        showAlert('Error loading cost data', 'error');
    }
}

async function deleteSales(id) {
    if (confirm('Are you sure you want to delete this sales record?')) {
        try {
            const response = await fetch(`${API_BASE}/monthly-sales/${id}`, {
                method: 'DELETE'
            });
            if (response.ok) {
                showAlert('Sales record deleted successfully!', 'success');
                loadSales();
                loadDashboardData();
            } else {
                showAlert('Error deleting sales record', 'error');
            }
        } catch (error) {
            showAlert('Error deleting sales record', 'error');
        }
    }
}

async function deleteCost(id) {
    if (confirm('Are you sure you want to delete this cost?')) {
        try {
            const response = await fetch(`${API_BASE}/costs/${id}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                showAlert('Cost deleted successfully!', 'success');
                loadCosts();
            } else {
                showAlert('Error deleting cost', 'error');
            }
        } catch (error) {
            showAlert('Error deleting cost', 'error');
        }
    }
}

function saveSettings() {
    showAlert('Settings saved successfully!', 'success');
}

// Reset database functionality
async function resetDatabase() {
    if (confirm('⚠️ WARNING: This will permanently delete ALL data from the database!\n\nThis action cannot be undone. Are you sure you want to continue?')) {
        if (confirm('🚨 FINAL CONFIRMATION: This will delete ALL products, sales, costs, and allocations!\n\nType "RESET" to confirm (case sensitive):')) {
            const confirmation = prompt('Type "RESET" to confirm database reset:');
            if (confirmation === 'RESET') {
                try {
                    showAlert('Resetting database...', 'info');
                    
                    const response = await fetch(`${API_BASE}/reset-database`, {
                        method: 'POST'
                    });
                    
                    if (response.ok) {
                        showAlert('Database reset successfully! All data has been cleared.', 'success');
                        
                        // Refresh all data
                        loadDashboardData();
                        loadProducts();
                        loadSales();
                        loadCosts();
                        
                        // Clear any charts
                        destroyAllCharts();
                        initializeCharts();
                        if (pendingDashboardStats) refreshDashboardCharts(pendingDashboardStats);
                        
                    } else {
                        showAlert('Error resetting database', 'error');
                    }
                } catch (error) {
                    showAlert('Error resetting database: ' + error.message, 'error');
                }
            } else {
                showAlert('Reset cancelled - confirmation text did not match', 'warning');
            }
        } else {
            showAlert('Reset cancelled', 'info');
        }
    } else {
        showAlert('Reset cancelled', 'info');
    }
}

// Sidebar toggle functionality
function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    const mainContent = document.querySelector('.main-content');
    
    if (sidebar && mainContent) {
        sidebar.classList.toggle('collapsed');
        mainContent.classList.toggle('sidebar-collapsed');
        
        // Store the state in localStorage
        const isCollapsed = sidebar.classList.contains('collapsed');
        localStorage.setItem('sidebarCollapsed', isCollapsed);
    }
}

// Initialize sidebar state from localStorage
function initializeSidebar() {
    const sidebar = document.getElementById('sidebar');
    const mainContent = document.querySelector('.main-content');
    const isCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';
    
    if (sidebar && mainContent) {
        if (isCollapsed) {
            sidebar.classList.add('collapsed');
            mainContent.classList.add('sidebar-collapsed');
        }
    }
}

// Excel Upload Functions
let uploadedData = null;

async function handleExcelUpload(event) {
    console.log('🚀 Starting Excel upload...');
    
    const file = event.target.files[0];
    if (!file) {
        console.log('❌ No file selected');
        return;
    }
    
    console.log(`📁 Selected file: ${file.name} (${file.size} bytes)`);
    
    // Validate file type
    const allowedTypes = ['.xlsx', '.xls'];
    const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
    
    if (!allowedTypes.includes(fileExtension)) {
        console.log('❌ Invalid file type:', fileExtension);
        showUploadError('Please select an Excel file (.xlsx or .xls). CSV files are not supported.');
        event.target.value = ''; // Clear the file input
        return;
    }
    
    console.log('✅ File type valid, starting upload...');
    
    // Show progress
    showUploadProgress();
    
    const month = (typeof getSalesUploadMonth === 'function')
        ? getSalesUploadMonth()
        : (document.getElementById('sales-upload-month')?.value || document.getElementById('allocation-month')?.value || '');
    if (!month || month === 'any') {
        showUploadError('Please select a reporting month on the Data Upload tab before uploading.');
        event.target.value = '';
        return;
    }
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('month', month);
        
        console.log('📤 Sending request to backend...');
        
        const response = await fetch(`${API_BASE}/upload-excel`, {
            method: 'POST',
            body: formData
        });
        
        console.log(`📥 Response status: ${response.status} ${response.statusText}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        console.log('📊 Upload result:', result);
        
        if (result.success) {
            console.log('✅ Upload successful!');
            console.log('📊 Upload details:', {
                message: result.message,
                products_created: result.products_created,
                sales_created: result.sales_created,
                parsed_data_length: result.parsed_data ? result.parsed_data.length : 0
            });
            
            uploadedData = result;
            showUploadResults(result);
            showExcelPreview(result);
            
            // Force refresh all data with delay to ensure database is updated
            console.log('🔄 Refreshing all dashboard data...');
            setTimeout(async () => {
                await Promise.all([
                    loadDashboardData(),
                    loadProducts(),
                    loadSales(),
                    loadCosts()
                ]);
                console.log('✅ Dashboard data refreshed');
                
                // Force update the data preview
                updateDataPreview();
            }, 500);
        } else {
            console.log('❌ Upload failed:', result.message);
            showUploadError(result.message || 'Upload failed');
        }
    } catch (error) {
        console.error('💥 Upload error:', error);
        showUploadError('Failed to upload file: ' + error.message);
    } finally {
        hideUploadProgress();
    }
}

function showUploadProgress() {
    document.getElementById('upload-progress').style.display = 'block';
    document.getElementById('upload-results').style.display = 'none';
    document.getElementById('excel-preview').style.display = 'none';
    
    // Simulate progress
    let progress = 0;
    const interval = setInterval(() => {
        progress += 10;
        document.getElementById('progress-fill').style.width = `${progress}%`;
        document.getElementById('upload-status').textContent = `Processing... ${progress}%`;
        
        if (progress >= 100) {
            clearInterval(interval);
        }
    }, 100);
}

function hideUploadProgress() {
    document.getElementById('upload-progress').style.display = 'none';
}

function showUploadResults(result) {
    console.log('showUploadResults called with:', result);
    const resultsDiv = document.getElementById('upload-results');
    const messageDiv = document.getElementById('upload-message');
    const productsCreatedDiv = document.getElementById('products-created');
    const salesCreatedDiv = document.getElementById('sales-created');
    
    console.log('Elements found:', {
        resultsDiv: !!resultsDiv,
        messageDiv: !!messageDiv,
        productsCreatedDiv: !!productsCreatedDiv,
        salesCreatedDiv: !!salesCreatedDiv
    });
    
    messageDiv.textContent = result.message || 'Upload completed';
    productsCreatedDiv.textContent = `Products Created: ${result.products_created || 0}`;
    salesCreatedDiv.textContent = `Sales Records: ${result.sales_created || 0}`;
    
    resultsDiv.style.display = 'block';
    console.log('Results displayed');
    
    // Always refresh dashboard data after successful upload
    if (result.success) {
        console.log('Refreshing dashboard data after upload...');
        loadDashboardData();
        loadProducts();
        loadSales();
        loadCosts();
        
        // Update the preview section with current database state
        updateDataPreview();
    }
}

async function updateDataPreview() {
    try {
        // Get current dashboard stats and sales data
        const [statsResponse, salesResponse] = await Promise.all([
            fetch(`${API_BASE}/dashboard/stats`),
            fetch(`${API_BASE}/sales`)
        ]);
        
        const stats = await statsResponse.json();
        const sales = await salesResponse.json();
        
        // Calculate inhouse production from sales data
        const inhouseProduction = sales.reduce((sum, sale) => sum + (sale.inhouse_production || 0), 0);
        
        // Update preview cards with current database state
        document.getElementById('preview-products-count').textContent = stats.total_products || 0;
        document.getElementById('preview-sales-count').textContent = sales.length || 0;
        document.getElementById('preview-revenue').textContent = `₹${(stats.total_revenue || 0).toLocaleString()}`;
        document.getElementById('preview-production').textContent = `${inhouseProduction.toFixed(1)} kg`;
        
        console.log('Data preview updated:', {
            products: stats.total_products,
            sales: sales.length,
            revenue: stats.total_revenue,
            production: inhouseProduction
        });
    } catch (error) {
        console.error('Error updating data preview:', error);
        // Set default values on error
        document.getElementById('preview-products-count').textContent = '0';
        document.getElementById('preview-sales-count').textContent = '0';
        document.getElementById('preview-revenue').textContent = '₹0';
        document.getElementById('preview-production').textContent = '0 kg';
    }
}

function showUploadError(message) {
    const resultsDiv = document.getElementById('upload-results');
    resultsDiv.innerHTML = `
        <div class="alert alert-error" style="background: #fee2e2; color: #991b1b; padding: 15px; border-radius: 8px; border: 1px solid #fecaca;">
            <h4 style="margin-bottom: 10px;">
                <i class="fas fa-exclamation-circle"></i>
                Upload Failed
            </h4>
            <p>${message}</p>
        </div>
    `;
    resultsDiv.style.display = 'block';
}

function showExcelPreview(result) {
    console.log('showExcelPreview called with:', result);
    const previewDiv = document.getElementById('excel-preview');
    
    // Update summary cards
    document.getElementById('preview-products-count').textContent = result.products_created;
    document.getElementById('preview-sales-count').textContent = result.sales_created;
    
    // Calculate totals
    if (result.parsed_data && result.parsed_data.length > 0) {
        const totalRevenue = result.parsed_data.reduce((sum, item) => sum + (item.outward_quantity * item.outward_rate), 0);
        const totalProduction = result.parsed_data.reduce((sum, item) => sum + item.inhouse_production, 0);
        
        document.getElementById('preview-revenue').textContent = `₹${totalRevenue.toLocaleString()}`;
        document.getElementById('preview-production').textContent = `${totalProduction.toFixed(1)} kg`;
        
        // Populate products table
        const productsTable = document.getElementById('preview-products-table');
        const uniqueProducts = [...new Set(result.parsed_data.map(item => item.particulars))];
        
        productsTable.innerHTML = uniqueProducts.map(product => {
            const productData = result.parsed_data.find(item => item.particulars === product);
            return `
                <tr>
                    <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">${product}</td>
                    <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">
                        <span style="padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; 
                            background: ${productData.type.toLowerCase() === 'in-house' ? '#dbeafe' : '#fef3c7'}; 
                            color: ${productData.type.toLowerCase() === 'in-house' ? '#1e40af' : '#92400e'};">
                            ${productData.type}
                        </span>
                    </td>
                </tr>
            `;
        }).join('');
        
        // Populate sales table
        const salesTable = document.getElementById('preview-sales-table');
        salesTable.innerHTML = result.parsed_data.map(item => `
            <tr>
                <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">${item.particulars}</td>
                <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">${item.outward_quantity.toFixed(1)} kg</td>
                <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">₹${item.outward_rate.toFixed(2)}</td>
                <td style="padding: 12px; border-bottom: 1px solid var(--border-color);">₹${(item.outward_quantity * item.outward_rate).toLocaleString()}</td>
            </tr>
        `).join('');
    }
    
    previewDiv.style.display = 'block';
}

function confirmExcelUpload() {
    if (!uploadedData) return;
    
    // Data is already saved to database during upload
    // Just refresh the current data and show success
    showNotification('Data uploaded and saved successfully!', 'success');
    
    // Refresh current tab data
    if (currentTab === 'products') {
        loadProducts();
    } else if (currentTab === 'sales') {
        loadSales();
    } else if (currentTab === 'dashboard') {
        loadDashboardData();
    }
    
    // Hide preview
    cancelExcelUpload();
}

function cancelExcelUpload() {
    document.getElementById('excel-preview').style.display = 'none';
    document.getElementById('upload-results').style.display = 'none';
    document.getElementById('excel-file-input').value = '';
    uploadedData = null;
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 20px;
        border-radius: 8px;
        color: white;
        font-weight: 500;
        z-index: 1000;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transform: translateX(100%);
        transition: transform 0.3s ease;
    `;
    
    // Set background color based on type
    const colors = {
        success: '#10B981',
        error: '#EF4444',
        warning: '#F59E0B',
        info: '#3B82F6'
    };
    notification.style.backgroundColor = colors[type] || colors.info;
    
    notification.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
        ${message}
    `;
    
    document.body.appendChild(notification);
    
    // Animate in
    setTimeout(() => {
        notification.style.transform = 'translateX(0)';
    }, 100);
    
    // Remove after 3 seconds
    setTimeout(() => {
        notification.style.transform = 'translateX(100%)';
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}
