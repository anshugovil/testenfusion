# Trade Strategy Processing System - Enhanced Edition

A comprehensive Python application for processing trading positions and trades with automatic strategy assignment, deliverables calculation, and PMS reconciliation.

## 🎯 Overview

This enhanced system now includes:
- **Original Features**: FULO/FUSH strategy assignment, position tracking, ACM mapping
- **NEW: Deliverables Calculation**: Physical delivery obligations for futures and options
- **NEW: Intrinsic Value (IV) Analysis**: Calculate IV for ITM options
- **NEW: PMS Reconciliation**: Compare system positions with external PMS files
- **NEW: Pre/Post Trade Analysis**: All calculations available for both starting and final positions

## ✨ New Features in Enhanced Edition

### 📊 Deliverables & IV Calculator
- Calculate physical delivery obligations for futures and options positions
- Intrinsic value calculations for options based on current prices
- Yahoo Finance integration for real-time price fetching
- Sensitivity analysis for different price scenarios
- Pre-trade vs Post-trade comparison
- Multi-currency support (INR/USD)

### 🔄 PMS Reconciliation
- Compare system positions with external Portfolio Management System files
- Identify mismatches, missing positions, and discrepancies
- Track reconciliation improvements/deteriorations after trades
- Support for multiple file formats (Excel, CSV)
- Comprehensive reconciliation reports with executive summaries

## 📋 Requirements

```bash
# Core requirements (existing)
pandas>=1.3.0
numpy>=1.20.0
streamlit>=1.20.0
openpyxl>=3.0.0
msoffcrypto-tool>=5.0.0

# Additional requirements for enhanced features
yfinance>=0.2.33  # For price fetching
python-dateutil>=2.8.2
pytz>=2023.3
```

## 🚀 Installation

```bash
# Clone repository
git clone <repository-url>
cd trade-strategy-processor

# Install all dependencies
pip install -r requirements.txt
```

## 💻 Usage

### Running the Enhanced Application

```bash
# Run with all features enabled
streamlit run unified-streamlit-app-enhanced.py

# Or use the original version without new features
streamlit run unified-streamlit-app.py
```

### Using New Features

#### 1. Enable Deliverables Calculation
- Check "Enable Deliverables/IV Calculation" in sidebar
- Set USD/INR exchange rate
- Choose whether to fetch Yahoo Finance prices
- Results appear in "Deliverables & IV" tab

#### 2. Enable PMS Reconciliation  
- Check "Enable PMS Reconciliation" in sidebar
- Upload PMS position file (Excel/CSV with Symbol and Position columns)
- Results appear in "PMS Reconciliation" tab

#### 3. Run Enhanced Pipeline
- Click "Run Full Enhanced Pipeline" to process everything at once
- Generates all reports: Strategy, ACM, Deliverables, Reconciliation

## 📁 Module Structure

```
trade-strategy-processor/
├── modules/
│   ├── input_parser.py              # Position file parser
│   ├── trade_parser.py              # Trade file parser
│   ├── position_manager.py          # Position tracking with Yahoo prices
│   ├── trade_processor.py           # Trade processing & strategy
│   ├── output_generator.py          # Output file generation
│   ├── acm_mapper.py               # ACM format mapping
│   ├── deliverables_calculator.py   # NEW: Deliverables & IV calculation
│   └── enhanced_recon_module.py     # NEW: PMS reconciliation
├── unified-streamlit-app.py         # Original application
├── unified-streamlit-app-enhanced.py # Enhanced application
├── futures_mapping.csv              # Symbol mappings
└── README_ENHANCED.md               # This file
```

## 📊 Output Files

### Original Outputs (Stage 1)
- `stage1_1_parsed_trades_*.csv` - Original trades
- `stage1_2_starting_positions_*.csv` - Initial positions
- `stage1_3_processed_trades_*.csv` - Trades with strategies
- `stage1_4_final_positions_*.csv` - Final positions

### Original Outputs (Stage 2)
- `acm_listedtrades_*.csv` - ACM formatted trades
- `acm_listedtrades_*_errors.csv` - Validation errors

### NEW: Deliverables Reports
- `DELIVERABLES_REPORT_*.xlsx` - Comprehensive deliverables workbook
  - Summary sheet with key metrics
  - Pre-Trade Deliverables
  - Post-Trade Deliverables  
  - Deliverables Comparison
  - Pre-Trade IV Analysis
  - Post-Trade IV Analysis

### NEW: Reconciliation Reports
- `PMS_RECONCILIATION_*.xlsx` - Comprehensive reconciliation workbook
  - Executive Summary
  - Pre-Trade reconciliation details
  - Post-Trade reconciliation details
  - Trade Impact Analysis
  - Matched/Mismatched positions

## 🔧 Configuration

### Deliverables Settings
```python
# In the app sidebar
usdinr_rate = 88.0  # Current USD/INR exchange rate
fetch_prices = True  # Enable Yahoo Finance price fetching
```

### PMS File Format
The PMS reconciliation file should have:
- Column A: Symbol/Ticker (Bloomberg format)
- Column B: Position/Quantity (numeric)

Example:
```
Symbol,Position
RIL=H5 IS Equity,100
NZU5 Index,-50
TCS IS 03/27/25 C3500 Equity,25
```

## 📈 Deliverables Calculation Logic

### Futures
- Deliverable = Position (always delivers)

### Call Options
- If Spot > Strike: Deliverable = Position (ITM, will be exercised)
- If Spot ≤ Strike: Deliverable = 0 (OTM, expires worthless)

### Put Options
- If Spot < Strike: Deliverable = -Position (ITM, will be exercised)
- If Spot ≥ Strike: Deliverable = 0 (OTM, expires worthless)

### Intrinsic Value
- Call IV = Max(0, Spot - Strike) × Position × Lot Size
- Put IV = Max(0, Strike - Spot) × Position × Lot Size

## 🔄 Reconciliation Process

1. **Position Matching**: Compares Bloomberg tickers between system and PMS
2. **Discrepancy Types**:
   - **Matched**: Same position in both systems
   - **Mismatch**: Different quantities for same ticker
   - **Missing in PMS**: Position in system but not in PMS
   - **Missing in System**: Position in PMS but not in system
3. **Impact Analysis**: Shows how trades improved or deteriorated reconciliation

## 🐛 Troubleshooting

### Price Fetching Issues
- Ensure internet connection for Yahoo Finance
- Check symbol format (NSE stocks need .NS suffix)
- Indices use special symbols (^NSEI for NIFTY)

### Reconciliation Mismatches
- Verify Bloomberg ticker format matches exactly
- Check for trailing spaces in PMS file
- Ensure position signs are correct (+/- for long/short)

### Performance
- Large files may take time to process
- Disable price fetching for faster processing if not needed
- Use CSV format for very large datasets

## 🔒 Security Notes

- Yahoo Finance data is for reference only
- Always verify deliverables with official exchange data
- PMS reconciliation should be reviewed by operations team
- Keep sensitive position files secure

## 📝 Version History

### v2.0.0 - Enhanced Edition
- Added deliverables and IV calculation
- Added PMS reconciliation
- Enhanced position manager with Yahoo price fetching
- Pre/Post trade analysis for all features
- Comprehensive Excel reports

### v1.0.0 - Original
- FULO/FUSH strategy assignment
- Position tracking and trade splitting
- ACM format mapping
- Basic reporting

## 👥 Contributors

[Your Name/Team]

## 📧 Support

For issues or questions, please contact [your contact information]

---

*Last Updated: [Date]*
