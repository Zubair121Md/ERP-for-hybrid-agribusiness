// Dashboard JavaScript for Fruit & Vegetable Cost Allocation System
const API_BASE = '/api';
let currentTab = 'dashboard';
let charts = {};
let currentData = {};

// Quantity display formatter for EA and KG
function formatQtyDisplay(productName, unit, quantity) {
    const name = (productName || '').toLowerCase();
    const u = (unit || 'kg').toUpperCase();
    const isEA = ['EA','EACH','PC','PCS','UNIT','UNITS'].includes(u);
    if (!isEA) return `${quantity} ${unit || 'kg'}`;
    if (name.includes('hamper')) return `${quantity} EA`;
    if (name.includes('button mushroom') || name.includes('baby corn')) {
        const kg = (quantity * 200) / 1000; // 200 g per EA
        return `${quantity} EA (200 g ea, ${kg.toFixed(2)} kg)`;
    }
    return `${quantity} EA`;
}

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeSidebar();
    initializeDashboard();
    loadDashboardData();
    setupEventListeners();
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
    
    // Form submissions
    document.getElementById('product-form').addEventListener('submit', handleProductSubmit);
    document.getElementById('sales-form').addEventListener('submit', handleSalesSubmit);
    document.getElementById('cost-form').addEventListener('submit', handleCostSubmit);
    
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
        'sales': { title: 'Sales', subtitle: 'Track Monthly Sales Data' },
        'costs': { title: 'Costs', subtitle: 'Manage Operational Costs' },
        'allocation': { title: 'Allocation', subtitle: 'Cost Distribution Analysis' },
        'reports': { title: 'Reports', subtitle: 'Generate & Export Reports' },
        'data-upload': { title: 'Data Upload', subtitle: 'Upload Sales & P&L Data' },
        'harvest-mapping': { title: 'Harvest & Mapping', subtitle: 'Upload Harvest Data & Product-Section Mappings' },
        'settings': { title: 'Settings', subtitle: 'System Configuration' }
    };
    
    const tabInfo = titles[tabName] || { title: 'Dashboard', subtitle: 'Overview & Analytics' };
    document.getElementById('page-title').textContent = tabInfo.title;
    document.getElementById('page-subtitle').textContent = tabInfo.subtitle;
    
    currentTab = tabName;
    
    // Load data for specific tabs
    if (tabName === 'dashboard') {
        loadDashboardData();
    } else if (tabName === 'products') {
        loadProducts();
    } else if (tabName === 'sales') {
        loadSales();
    } else if (tabName === 'costs') {
        loadCosts();
    } else if (tabName === 'harvest-mapping') {
        // No data loading needed for harvest-mapping tab
        console.log('✅ Harvest & Mapping tab activated');
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
        
        // Load top products
        await loadTopProducts();
        
        // Update charts
        updateCharts(stats);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        showAlert('Error loading dashboard data', 'error');
    }
}

// Display dashboard statistics
function displayDashboardStats(stats) {
    const statsGrid = document.getElementById('stats-grid');
    
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
                <span class="stat-title">Total Revenue</span>
                <i class="fas fa-chart-line stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(stats.total_revenue || 0)}</div>
            <div class="stat-change positive">
                <i class="fas fa-arrow-up"></i>
                +${(stats.profit_margin || 0).toFixed(1)}% margin
            </div>
        </div>
        
        <div class="stat-card costs">
            <div class="stat-header">
                <span class="stat-title">Total Costs</span>
                <i class="fas fa-dollar-sign stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(stats.total_costs || 0)}</div>
            <div class="stat-change">
                <i class="fas fa-info-circle"></i>
                All categories
            </div>
        </div>
        
        <div class="stat-card profit">
            <div class="stat-header">
                <span class="stat-title">Net Profit</span>
                <i class="fas fa-trophy stat-icon"></i>
            </div>
            <div class="stat-value">₹${formatNumber(stats.total_profit || 0)}</div>
            <div class="stat-change ${(stats.total_profit || 0) >= 0 ? 'positive' : 'negative'}">
                <i class="fas fa-${(stats.total_profit || 0) >= 0 ? 'arrow-up' : 'arrow-down'}"></i>
                ${(stats.profit_margin || 0).toFixed(1)}% margin
            </div>
        </div>
    `;
    
    statsGrid.innerHTML = statsHTML;
}

// Load top products
async function loadTopProducts() {
    try {
        // Get all sales data instead of month-based report
        const response = await fetch(`${API_BASE}/sales`);
        const sales = await response.json();
        
        // Calculate top products from all sales data
        const productStats = {};
        sales.forEach(sale => {
            const productName = sale.product_name;
            if (!productStats[productName]) {
                productStats[productName] = {
                    name: productName,
                    revenue: 0,
                    quantity: 0
                };
            }
            productStats[productName].revenue += sale.quantity * sale.sale_price;
            productStats[productName].quantity += sale.quantity;
        });
        
        const topProducts = Object.values(productStats)
            .sort((a, b) => b.revenue - a.revenue)
            .slice(0, 5);
        
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
function initializeCharts() {
    // Revenue vs Costs Chart
    const revenueCtx = document.getElementById('revenueChart').getContext('2d');
    charts.revenue = new Chart(revenueCtx, {
        type: 'bar',
        data: {
            labels: ['Revenue', 'Costs', 'Profit'],
            datasets: [{
                label: 'Amount (₹)',
                data: [0, 0, 0],
                backgroundColor: [
                    'rgba(33, 150, 243, 0.8)',
                    'rgba(244, 67, 54, 0.8)',
                    'rgba(76, 175, 80, 0.8)'
                ],
                borderColor: [
                    'rgba(33, 150, 243, 1)',
                    'rgba(244, 67, 54, 1)',
                    'rgba(76, 175, 80, 1)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '₹' + formatNumber(value);
                        }
                    }
                }
            }
        }
    });
    
    // Source Distribution Chart
    const sourceCtx = document.getElementById('sourceChart').getContext('2d');
    charts.source = new Chart(sourceCtx, {
        type: 'doughnut',
        data: {
            labels: ['Inhouse', 'Outsourced'],
            datasets: [{
                data: [0, 0],
                backgroundColor: [
                    'rgba(76, 175, 80, 0.8)',
                    'rgba(255, 152, 0, 0.8)'
                ],
                borderColor: [
                    'rgba(76, 175, 80, 1)',
                    'rgba(255, 152, 0, 1)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

// Update charts with data
function updateCharts(stats) {
    // Update revenue chart
    charts.revenue.data.datasets[0].data = [
        stats.total_revenue,
        stats.total_costs,
        stats.total_profit
    ];
    charts.revenue.update();
    
    // Update source chart
    charts.source.data.datasets[0].data = [
        stats.inhouse_revenue,
        stats.outsourced_revenue
    ];
    charts.source.update();
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
        
        const response = await fetch(`${API_BASE}/sales`);
        const sales = await response.json();
        
        displaySales(sales);
        
    } catch (error) {
        console.error('Error loading sales:', error);
        showAlert('Error loading sales data', 'error');
    }
}

// Display sales data
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
        tableHTML += `
            <tr data-sale-id="${sale.id}">
                <td><strong>${sale.product_name}</strong></td>
                <td>${qtyText}</td>
                <td>₹${sale.sale_price}</td>
                <td>₹${sale.direct_cost}</td>
                <td>₹${formatNumber(revenue)}</td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="editSales(${sale.id})">
                        <i class="fas fa-edit"></i>
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
        
        displayCosts(costs);
        
    } catch (error) {
        console.error('Error loading costs:', error);
        showAlert('Error loading costs', 'error');
    }
}

// Initialize Cost Items function removed - use cost sheet upload instead

// Display costs
function displayCosts(costs) {
    const container = document.getElementById('costs-table');
    
    if (costs.length === 0) {
        container.innerHTML = '<p>No costs found. Upload a cost sheet Excel file to add costs.</p>';
        return;
    }
    
    // Organize costs by category (like MD file structure)
    const categoryHeadings = {
        'fixed_cost_cat_i': '1. FIXED COST CAT - I',
        'fixed_cost_cat_ii': 'FIXED COST CAT - II',
        'variable_cost_open_field': 'A) OPEN FIELD',
        'variable_cost_lettuce': 'B) LETTUCE',
        'variable_cost_strawberry': 'C) STRAWBERRY',
        'variable_cost_raspberry': 'D) RASPBERRY & BLUEBERRY',
        'variable_cost_packing': 'E) PACKING',
        'variable_cost_aggregation': 'F) AGGREGATION',
        'distribution_cost': '3. DISTRIBUTION COST',
        'marketing_expenses': '4. MARKETING EXPENSES',
        'vehicle_running_cost': '5. VEHICLE RUNNING COST',
        'others': '6. OTHERS',
        'wastage': '7. WASTAGE & SHORTAGE',
        'purchase_accounts': '8. PURCHASE ACCOUNTS',
        'pl_import': 'P&L Imported Costs'
    };
    
    // Group costs by category
    const costsByCategory = {};
    costs.forEach(cost => {
        const category = cost.category || 'other';
        if (!costsByCategory[category]) {
            costsByCategory[category] = [];
        }
        costsByCategory[category].push(cost);
    });
    
    let tableHTML = `
        <table class="table">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Amount</th>
                    <th>Applies To</th>
                    <th>Type</th>
                    <th>Basis</th>
                    <th>Category</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    // Display costs organized by category headings
    Object.keys(categoryHeadings).forEach(categoryKey => {
        if (costsByCategory[categoryKey] && costsByCategory[categoryKey].length > 0) {
            const heading = categoryHeadings[categoryKey];
            tableHTML += `
                <tr style="background-color: #f0f0f0; font-weight: bold;">
                    <td colspan="7" style="padding: 10px; font-size: 14px;">${heading}</td>
                </tr>
            `;
            
            costsByCategory[categoryKey].forEach(cost => {
        tableHTML += `
            <tr>
                        <td style="padding-left: 20px;">${cost.name}</td>
                <td>₹${formatNumber(cost.amount)}</td>
                <td><span class="badge badge-info">${cost.applies_to}</span></td>
                <td>${cost.cost_type}</td>
                <td>${cost.basis}</td>
                <td><span class="badge badge-secondary">${cost.category}</span></td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="editCost(${cost.id})">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm btn-danger" onclick="deleteCost(${cost.id})">
                        <i class="fas fa-trash"></i>
                    </button>
                </td>
            </tr>
        `;
            });
        }
    });
    
    // Display any costs not in predefined categories
    Object.keys(costsByCategory).forEach(category => {
        if (!categoryHeadings[category] && costsByCategory[category].length > 0) {
            tableHTML += `
                <tr style="background-color: #f0f0f0; font-weight: bold;">
                    <td colspan="7" style="padding: 10px; font-size: 14px;">Other Costs</td>
                </tr>
            `;
            
            costsByCategory[category].forEach(cost => {
                tableHTML += `
                    <tr>
                        <td style="padding-left: 20px;">${cost.name}</td>
                        <td>₹${formatNumber(cost.amount)}</td>
                        <td><span class="badge badge-info">${cost.applies_to}</span></td>
                        <td>${cost.cost_type}</td>
                        <td>${cost.basis}</td>
                        <td><span class="badge badge-secondary">${cost.category}</span></td>
                        <td>
                            <button class="btn btn-sm btn-secondary" onclick="editCost(${cost.id})">
                                <i class="fas fa-edit"></i>
                            </button>
                            <button class="btn btn-sm btn-danger" onclick="deleteCost(${cost.id})">
                                <i class="fas fa-trash"></i>
                            </button>
                        </td>
                    </tr>
                `;
            });
        }
    });
    
    tableHTML += '</tbody></table>';
    container.innerHTML = tableHTML;
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
        applies_to: document.getElementById('cost-applies-to').value,
        cost_type: document.getElementById('cost-type').value,
        basis: document.getElementById('cost-basis').value,
        month: document.getElementById('cost-month').value,
        is_fixed: document.getElementById('cost-fixed').value,
        category: document.getElementById('cost-category').value
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
        applies_to: document.getElementById('cost-applies-to').value,
        cost_type: document.getElementById('cost-type').value,
        basis: document.getElementById('cost-basis').value,
        month: document.getElementById('cost-month').value,
        category: document.getElementById('cost-category').value,
        is_fixed: document.getElementById('cost-fixed').value
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

// Allocation functions
async function runAllocation() {
    const month = document.getElementById('allocation-month').value;
    
    if (!month) {
        showAlert('Please select a month', 'error');
        return;
    }
    
    try {
        showLoading('allocation-results');
        
        const response = await fetch(`${API_BASE}/allocate/${month}`, {
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
        html += `
            <tr class="product-row" style="cursor: pointer;" onclick="showCostBreakdown(${product.product_id})" title="Click to view cost breakdown">
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

// Cost Breakdown Modal Functions
async function showCostBreakdown(productId) {
    console.log('🔍 showCostBreakdown called with productId:', productId);
    
    if (!productId) {
        console.error('❌ No productId provided');
        showAlert('Error: Product ID is missing', 'error');
        return;
    }
    
    try {
        showLoading('allocation-results');
        console.log('📡 Fetching from:', `${API_BASE}/product-cost-breakdown/${productId}`);
        
        const response = await fetch(`${API_BASE}/product-cost-breakdown/${productId}`);
        
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
    } finally {
        hideLoading('allocation-results');
    }
}

function displayCostBreakdownModal(breakdown) {
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
                                <th>Number of Costs</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${Object.entries(breakdown.costs_by_category).map(([category, data]) => `
                                <tr>
                                    <td><strong>${category}</strong></td>
                                    <td>₹${formatNumber(data.total)}</td>
                                    <td>${data.costs.length}</td>
                                </tr>
                            `).join('')}
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
                                        <small>₹${formatNumber(cost.amount)} (${cost.basis})</small>
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
                                        <small>₹${formatNumber(cost.amount)} (${cost.basis})</small>
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
                                        <small>₹${formatNumber(cost.amount)} (${cost.basis})</small>
                                    </li>
                                `).join('')}
                                ${breakdown.costs_by_type.common.length === 0 ? '<li>None</li>' : ''}
                            </ul>
                        </div>
                    </div>
                    
                    <h3>Detailed Cost Allocation</h3>
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Cost Name</th>
                                <th>Category</th>
                                <th>Applies To</th>
                                <th>Basis</th>
                                <th>Allocated Amount</th>
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
                                    <td>₹${formatNumber(cost.total_cost_amount)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                        <tfoot>
                            <tr style="font-weight: bold; background-color: #f5f5f5;">
                                <td colspan="4">Total Allocated Costs</td>
                                <td>₹${formatNumber(breakdown.total_allocated)}</td>
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
    
    // Add modal to page
    const modalContainer = document.createElement('div');
    modalContainer.innerHTML = modalHtml;
    document.body.appendChild(modalContainer.firstElementChild);
}

function closeCostBreakdownModal() {
    const modal = document.getElementById('costBreakdownModal');
    if (modal) {
        modal.remove();
    }
}

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('costBreakdownModal');
    if (event.target == modal) {
        closeCostBreakdownModal();
    }
}

// Report functions
async function generateReport() {
    try {
        showLoading('report-results');
        
        // Get all data instead of month-based report
        const [salesResponse, costsResponse] = await Promise.all([
            fetch(`${API_BASE}/sales`),
            fetch(`${API_BASE}/costs`)
        ]);
        
        const sales = await salesResponse.json();
        const costs = await costsResponse.json();
        
        // Generate report from all data
        const result = generateReportFromData(sales, costs);
        displayReportResults(result);
        
    } catch (error) {
        console.error('Error generating report:', error);
        showAlert('Error generating report', 'error');
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
    container.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <p>Loading...</p>
        </div>
    `;
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
        // Get data from the table row instead of API call
        const row = document.querySelector(`tr[data-sale-id="${id}"]`);
        if (!row) {
            showAlert('Sales data not found in table', 'error');
            return;
        }
        
        // Extract data from table row
        const cells = row.querySelectorAll('td');
        const productName = cells[0].textContent.trim();
        const quantity = parseFloat(cells[1].textContent.replace(' kg', ''));
        const salePrice = parseFloat(cells[2].textContent.replace('₹', ''));
        const directCost = parseFloat(cells[3].textContent.replace('₹', ''));
        
        // Get product ID from the product name
        const productSelect = document.getElementById('sales-product');
        let productId = '';
        for (let option of productSelect.options) {
            if (option.textContent.trim() === productName) {
                productId = option.value;
                break;
            }
        }
        
        // Change title to "Edit Sales Data"
        document.getElementById('sales-modal-title').textContent = 'Edit Sales Data';
        
        // Populate form with existing data
        document.getElementById('sales-product').value = productId;
        document.getElementById('sales-month').value = '2025-10'; // Default month (optional)
        document.getElementById('sales-quantity').value = quantity;
        document.getElementById('sales-price').value = salePrice;
        document.getElementById('sales-direct-cost').value = directCost;
        
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
            document.getElementById('cost-applies-to').value = cost.applies_to;
            document.getElementById('cost-type').value = cost.cost_type;
            document.getElementById('cost-basis').value = cost.basis;
            document.getElementById('cost-month').value = cost.month;
            document.getElementById('cost-category').value = cost.category;
            document.getElementById('cost-fixed').value = cost.is_fixed;
            
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
                        if (charts.revenueChart) {
                            charts.revenueChart.destroy();
                        }
                        if (charts.sourceChart) {
                            charts.sourceChart.destroy();
                        }
                        
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
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        
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
