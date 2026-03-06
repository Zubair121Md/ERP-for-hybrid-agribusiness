"""
Flask route for cost sheet upload and processing
"""
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import os
import tempfile
from cost_sheet_parser import parse_cost_sheet
from cost_allocation import allocate_costs

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/upload-cost-sheet', methods=['POST'])
def upload_cost_sheet():
    """
    POST /upload-cost-sheet
    
    Accepts Excel file upload, extracts cost sheet data, and applies cost allocation rules.
    
    Returns:
        JSON response with:
        - company_name
        - period
        - total_qty_sold
        - total_expenses
        - total_income
        - profit
        - category_totals (all expense categories)
        - percentage_splits (Strawberry, Greens, Aggregation)
        - product_quantity_data
        - product_margin_data
        - cost_allocation (allocated costs per product)
    """
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No file provided'
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'No file selected'
            }), 400
        
        if not allowed_file(file.filename):
            return jsonify({
                'success': False,
                'error': 'Invalid file type. Please upload .xlsx or .xls file'
            }), 400
        
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            # Parse the cost sheet
            parse_result = parse_cost_sheet(filepath)
            
            if not parse_result.get('success', False):
                return jsonify({
                    'success': False,
                    'error': parse_result.get('error', 'Failed to parse cost sheet')
                }), 400
            
            # Extract data
            header_info = parse_result.get('header_info', {})
            expenses = parse_result.get('expenses', {})
            percentage_splits = parse_result.get('percentage_splits', {})
            income = parse_result.get('income', {})
            product_quantity = parse_result.get('product_quantity', [])
            product_margin = parse_result.get('product_margin', [])
            
            # Calculate total expenses
            total_expenses = sum([
                expenses.get('fixed_cost_cat_i', {}).get('total', 0.0),
                expenses.get('fixed_cost_cat_ii', {}).get('total', 0.0),
                expenses.get('variable_cost', {}).get('total', 0.0),
                expenses.get('distribution_cost', {}).get('total', 0.0),
                expenses.get('marketing_expenses', {}).get('total', 0.0),
                expenses.get('vehicle_running_cost', {}).get('total', 0.0),
                expenses.get('others', {}).get('total', 0.0),
                expenses.get('wastage_shortage', {}).get('total', 0.0),
                expenses.get('purchase_accounts', {}).get('total', 0.0)
            ])
            
            # Apply cost allocation rules
            allocation_results = allocate_costs(expenses, product_quantity, percentage_splits)
            
            # Prepare response
            response = {
                'success': True,
                'company_name': header_info.get('company_name', ''),
                'period': header_info.get('period', ''),
                'total_qty_sold': header_info.get('total_qty_sold', 0.0),
                'total_expenses': total_expenses,
                'total_income': income.get('total_income', 0.0),
                'profit': income.get('profit', 0.0),
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
                'percentage_splits': {
                    'strawberry': percentage_splits.get('strawberry', 0.60),
                    'greens': percentage_splits.get('greens', 0.25),
                    'aggregation': percentage_splits.get('aggregation', 0.15)
                },
                'product_quantity_data': product_quantity,
                'product_margin_data': product_margin,
                'cost_allocation': allocation_results
            }
            
            return jsonify(response), 200
        
        finally:
            # Clean up temporary file
            if os.path.exists(filepath):
                os.remove(filepath)
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error processing file: {str(e)}'
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
