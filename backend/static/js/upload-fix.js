// Sales upload helper — month is required from the Data Upload UI
console.log('🔧 Upload fix script loaded');

function getSalesUploadMonth() {
    const el = document.getElementById('sales-upload-month');
    if (el && el.value) return el.value;
    const alloc = document.getElementById('allocation-month');
    if (alloc && alloc.value && alloc.value !== 'any') return alloc.value;
    const now = new Date();
    return now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0');
}

function handleExcelUpload(event) {
    console.log('🚀 SIMPLIFIED UPLOAD STARTING...');

    const file = event.target.files[0];
    if (!file) {
        console.log('❌ No file selected');
        return;
    }

    const month = getSalesUploadMonth();
    if (!month) {
        alert('Please select a reporting month before uploading.');
        event.target.value = '';
        return;
    }

    console.log(`📁 File: ${file.name} (${file.size} bytes), month: ${month}`);

    if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
        alert('Please select an Excel file (.xlsx or .xls)');
        return;
    }

    const progressDiv = document.getElementById('upload-progress');
    const resultsDiv = document.getElementById('upload-results');
    const alertBox = document.getElementById('upload-alert-box');
    if (progressDiv) progressDiv.style.display = 'block';
    if (resultsDiv) resultsDiv.style.display = 'none';

    const formData = new FormData();
    formData.append('file', file);
    formData.append('month', month);

    fetch('/api/upload-excel', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (progressDiv) progressDiv.style.display = 'none';

        if (resultsDiv) {
            resultsDiv.style.display = 'block';
            if (alertBox) {
                alertBox.className = 'upload-alert ' + (data.success ? 'upload-alert--success' : 'upload-alert--error');
            }

            const messageDiv = document.getElementById('upload-message');
            if (messageDiv) messageDiv.textContent = data.message || 'Upload completed';

            const productsDiv = document.getElementById('products-created');
            if (productsDiv) productsDiv.innerHTML = '<strong>' + (data.products_created || 0) + '</strong> products';

            const salesDiv = document.getElementById('sales-created');
            if (salesDiv) salesDiv.innerHTML = '<strong>' + (data.sales_created || 0) + '</strong> sales records';

            const monthDiv = document.getElementById('upload-month-display');
            if (monthDiv && data.month) {
                monthDiv.style.display = 'inline';
                monthDiv.innerHTML = 'Month: <strong>' + data.month + '</strong>';
            }
        }

        setTimeout(() => {
            if (typeof loadDashboardData === 'function') loadDashboardData();
            if (typeof loadProducts === 'function') loadProducts();
            if (typeof loadSales === 'function') loadSales();
            if (typeof loadCosts === 'function') loadCosts();
            if (typeof updateDataPreview === 'function') updateDataPreview();
        }, 1000);
    })
    .catch(error => {
        console.error('💥 Upload error:', error);
        if (progressDiv) progressDiv.style.display = 'none';
        if (resultsDiv) {
            resultsDiv.style.display = 'block';
            if (alertBox) alertBox.className = 'upload-alert upload-alert--error';
            const messageDiv = document.getElementById('upload-message');
            if (messageDiv) messageDiv.textContent = 'Upload failed: ' + error.message;
        }
    });
}

window.handleExcelUpload = handleExcelUpload;
console.log('✅ Upload fix applied - handleExcelUpload function overridden');
