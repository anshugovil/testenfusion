"""
Enhanced Unified Trade Processing Pipeline - COMPLETE VERSION WITH EXPIRY DELIVERIES
Includes ALL features: Strategy Processing, ACM Mapping, Deliverables, PMS Recon, and Expiry Physical Delivery
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import logging
from datetime import datetime
import traceback
import os
import sys

# Import existing modules from root directory
try:
    from input_parser import InputParser
    from Trade_Parser import TradeParser  
    from position_manager import PositionManager
    from trade_processor import TradeProcessor
    from output_generator import OutputGenerator
    from acm_mapper import ACMMapper
    
    # Import enhanced modules
    try:
        from deliverables_calculator import DeliverableCalculator
        from enhanced_recon_module import EnhancedReconciliation
        NEW_FEATURES_AVAILABLE = True
    except ImportError:
        NEW_FEATURES_AVAILABLE = False
    
    # Import Expiry Delivery Generator
    try:
        from expiry_delivery_generator import ExpiryDeliveryGenerator
        EXPIRY_DELIVERY_AVAILABLE = True
    except ImportError:
        EXPIRY_DELIVERY_AVAILABLE = False
        
except ModuleNotFoundError as e:
    st.error(f"Failed to import modules: {e}")
    st.error("Please ensure all module files are in the same directory as this app")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Trade Processing Pipeline - Complete Edition",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #1f77b4; }
    .stDownloadButton button { 
        width: 100%; 
        background-color: #4CAF50; 
        color: white; 
    }
    .stage-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .expiry-card {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)

def ensure_directories():
    """Ensure required directories exist"""
    dirs = ["output", "output/stage1", "output/stage2", "output/expiry_deliveries", "temp"]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

ensure_directories()

def get_temp_dir():
    """Get temporary directory that works on Streamlit Cloud"""
    if Path("/tmp").exists() and os.access("/tmp", os.W_OK):
        return Path("/tmp")
    else:
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        return temp_dir

def main():
    st.title("🎯 Enhanced Trade Processing Pipeline - Complete Edition")
    st.markdown("### Comprehensive pipeline with strategy processing, deliverables, reconciliation, and expiry physical delivery")
    
    # Initialize session state
    if 'stage1_complete' not in st.session_state:
        st.session_state.stage1_complete = False
    if 'stage2_complete' not in st.session_state:
        st.session_state.stage2_complete = False
    if 'stage1_outputs' not in st.session_state:
        st.session_state.stage1_outputs = {}
    if 'stage2_outputs' not in st.session_state:
        st.session_state.stage2_outputs = {}
    if 'dataframes' not in st.session_state:
        st.session_state.dataframes = {}
    if 'acm_mapper' not in st.session_state:
        st.session_state.acm_mapper = None
    if 'deliverables_complete' not in st.session_state:
        st.session_state.deliverables_complete = False
    if 'recon_complete' not in st.session_state:
        st.session_state.recon_complete = False
    if 'deliverables_data' not in st.session_state:
        st.session_state.deliverables_data = {}
    if 'recon_data' not in st.session_state:
        st.session_state.recon_data = {}
    if 'expiry_deliveries_complete' not in st.session_state:
        st.session_state.expiry_deliveries_complete = False
    if 'expiry_delivery_files' not in st.session_state:
        st.session_state.expiry_delivery_files = {}
    
    # Sidebar
    with st.sidebar:
        st.header("📂 Input Files")
        
        # Stage 1 Section
        st.markdown("### Stage 1: Strategy Processing")
        
        position_file = st.file_uploader(
            "1. Position File",
            type=['xlsx', 'xls', 'csv'],
            key='position_file',
            help="BOD, Contract, or MS format"
        )
        
        trade_file = st.file_uploader(
            "2. Trade File", 
            type=['xlsx', 'xls', 'csv'],
            key='trade_file',
            help="MS format trade file"
        )
        
        st.subheader("3. Mapping File")
        default_mapping = None
        
        mapping_locations = [
            "futures_mapping.csv",
            "futures mapping.csv",
            "data/futures_mapping.csv",
            "data/futures mapping.csv",
            "./futures_mapping.csv",
            "./data/futures_mapping.csv"
        ]
        
        for location in mapping_locations:
            if Path(location).exists():
                default_mapping = location
                break
        
        if default_mapping:
            use_default_mapping = st.radio(
                "Mapping source:",
                ["Use default from repository", "Upload custom"],
                index=0,
                key="mapping_radio"
            )
            
            if use_default_mapping == "Upload custom":
                mapping_file = st.file_uploader(
                    "Upload Mapping File",
                    type=['csv'],
                    key='mapping_file'
                )
            else:
                mapping_file = None
                st.success(f"✔ Using {Path(default_mapping).name}")
        else:
            st.warning("Upload mapping file (required)")
            mapping_file = st.file_uploader(
                "Upload Mapping File",
                type=['csv'],
                key='mapping_file',
                help="CSV file with symbol-to-ticker mappings"
            )
            use_default_mapping = None
        
        st.divider()
        
        # Stage 2 Section
        st.markdown("### Stage 2: ACM Mapping")
        
        schema_option = st.radio(
            "Schema Configuration:",
            ["Use built-in schema (default)", "Upload custom schema"],
            index=0,
            key="schema_option"
        )
        
        custom_schema_file = None
        
        if schema_option == "Use built-in schema (default)":
            st.info("✅ Will use hardcoded ACM format")
            
            st.markdown("#### Download Schema Template")
            temp_mapper = ACMMapper()
            schema_bytes = temp_mapper.generate_schema_excel()
            
            st.download_button(
                label="📥 Download ACM Schema Template",
                data=schema_bytes,
                file_name=f"acm_schema_template_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download the default schema as Excel for reference or customization",
                use_container_width=True
            )
        else:
            st.warning("📤 Upload your custom schema file")
            custom_schema_file = st.file_uploader(
                "Upload ACM Schema",
                type=['xlsx', 'xls'],
                key='custom_schema_file'
            )
        
        st.divider()
        
        # Additional Features Section
        st.markdown("### 📊 Additional Features")
        
        # Deliverables/IV option
        enable_deliverables = st.checkbox(
            "Enable Deliverables/IV Calculation",
            value=False,
            key="enable_deliverables",
            help="Calculate physical delivery obligations and intrinsic values"
        )
        
        if enable_deliverables:
            col1, col2 = st.columns(2)
            with col1:
                usdinr_rate = st.number_input(
                    "USD/INR Rate",
                    min_value=50.0,
                    max_value=150.0,
                    value=88.0,
                    step=0.1,
                    key="usdinr_rate"
                )
            with col2:
                fetch_prices = st.checkbox(
                    "Fetch Yahoo Prices",
                    value=True,
                    key="fetch_prices",
                    help="Fetch current prices from Yahoo Finance"
                )
        
        # Expiry Delivery Generation
        if EXPIRY_DELIVERY_AVAILABLE:
            enable_expiry_delivery = st.checkbox(
                "Enable Expiry Physical Delivery",
                value=False,
                key="enable_expiry_delivery",
                help="Generate physical delivery trades per expiry date"
            )
            
            if enable_expiry_delivery:
                col1, col2 = st.columns(2)
                with col1:
                    use_yahoo_for_delivery = st.checkbox(
                        "Use Yahoo Prices",
                        value=True,
                        key="use_yahoo_for_delivery",
                        help="Use Yahoo prices for ITM calculations"
                    )
                with col2:
                    include_taxes = st.checkbox(
                        "Calculate Taxes",
                        value=True,
                        key="include_delivery_taxes",
                        help="Include STT and stamp duty"
                    )
        
        # PMS Reconciliation option
        enable_recon = st.checkbox(
            "Enable PMS Reconciliation",
            value=False,
            key="enable_recon",
            help="Compare positions with PMS file"
        )
        
        pms_file = None
        if enable_recon:
            pms_file = st.file_uploader(
                "Upload PMS Position File",
                type=['xlsx', 'xls', 'csv'],
                key='pms_file'
            )
        
        st.divider()
        
        # Process buttons
        can_process_stage1 = (
            position_file is not None and 
            trade_file is not None and 
            (mapping_file is not None or (use_default_mapping == "Use default from repository" and default_mapping))
        )
        
        can_process_stage2 = st.session_state.stage1_complete
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚀 Run Stage 1", type="primary", use_container_width=True, disabled=not can_process_stage1):
                process_stage1(position_file, trade_file, mapping_file, use_default_mapping, default_mapping)
        
        with col2:
            if st.button("🎯 Run Stage 2", type="secondary", use_container_width=True, disabled=not can_process_stage2):
                process_stage2(schema_option, custom_schema_file)
        
        # Complete pipeline button
        if can_process_stage1:
            st.divider()
            if st.button("⚡ Run Complete Enhanced Pipeline", type="primary", use_container_width=True):
                # Run Stage 1
                if process_stage1(position_file, trade_file, mapping_file, use_default_mapping, default_mapping):
                    # Run Stage 2
                    process_stage2(schema_option, custom_schema_file)
                    # Run enhanced features
                    if enable_deliverables:
                        run_deliverables_calculation(
                            usdinr_rate if enable_deliverables else 88.0,
                            fetch_prices if enable_deliverables else False
                        )
                    if EXPIRY_DELIVERY_AVAILABLE and enable_expiry_delivery:
                        run_expiry_delivery_generation()
                    if enable_recon and pms_file:
                        run_pms_reconciliation(pms_file)
                    st.success("✅ Complete enhanced pipeline finished!")
                    st.balloons()
        
        st.divider()
        if st.button("🔄 Reset All", type="secondary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Main content tabs
    tab_list = ["📊 Pipeline Overview", "📄 Stage 1: Strategy", "📋 Stage 2: ACM"]
    
    if NEW_FEATURES_AVAILABLE:
        if st.session_state.get('enable_deliverables', False):
            tab_list.append("💰 Deliverables & IV")
        if st.session_state.get('enable_recon', False):
            tab_list.append("🔄 PMS Reconciliation")
    
    # Make sure Expiry Deliveries tab is added when enabled
    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.get('enable_expiry_delivery', False):
        tab_list.append("📅 Expiry Deliveries")
        # Debug info
        if st.session_state.get('expiry_deliveries_complete', False):
            files_count = len(st.session_state.get('expiry_delivery_files', {}))
            st.sidebar.success(f"✅ {files_count} expiry reports generated")
    
    tab_list.extend(["📥 Downloads", "📘 Schema Info"])
    
    tabs = st.tabs(tab_list)
    
    tab_index = 0
    with tabs[tab_index]:
        display_pipeline_overview()
    tab_index += 1
    
    with tabs[tab_index]:
        display_stage1_results()
    tab_index += 1
    
    with tabs[tab_index]:
        display_stage2_results()
    tab_index += 1
    
    # Deliverables tab (if enabled)
    if NEW_FEATURES_AVAILABLE and st.session_state.get('enable_deliverables', False):
        with tabs[tab_index]:
            display_deliverables_tab()
        tab_index += 1
    
    # Reconciliation tab (if enabled)
    if NEW_FEATURES_AVAILABLE and st.session_state.get('enable_recon', False):
        with tabs[tab_index]:
            display_reconciliation_tab()
        tab_index += 1
    
    # Expiry Deliveries tab (if enabled)
    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.get('enable_expiry_delivery', False):
        with tabs[tab_index]:
            display_expiry_deliveries_tab()
        tab_index += 1
    
    with tabs[tab_index]:
        display_downloads()
    tab_index += 1
    
    with tabs[tab_index]:
        display_schema_info()

# Processing Functions

def process_stage1(position_file, trade_file, mapping_file, use_default, default_path):
    """Process Stage 1: Strategy Assignment"""
    try:
        with st.spinner("Processing Stage 1: Strategy Assignment..."):
            temp_dir = get_temp_dir()
            
            # Save uploaded files
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(position_file.name).suffix, dir=temp_dir) as tmp:
                tmp.write(position_file.getbuffer())
                pos_path = tmp.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(trade_file.name).suffix, dir=temp_dir) as tmp:
                tmp.write(trade_file.getbuffer())
                trade_path = tmp.name
            
            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir=temp_dir) as tmp:
                    tmp.write(mapping_file.getbuffer())
                    map_path = tmp.name
            else:
                map_path = default_path
            
            # Parse positions
            input_parser = InputParser(map_path)
            positions = input_parser.parse_file(pos_path)
            
            if not positions:
                st.error("❌ No positions found")
                return False
            
            st.success(f"✅ Parsed {len(positions)} positions ({input_parser.format_type} format)")
            
            # Parse trades
            trade_parser = TradeParser(map_path)
            
            if trade_path.endswith('.csv'):
                trade_df = pd.read_csv(trade_path, header=None)
            else:
                trade_df = pd.read_excel(trade_path, header=None)
            
            trades = trade_parser.parse_trade_file(trade_path)
            
            if not trades:
                st.error("❌ No trades found")
                return False
            
            st.success(f"✅ Parsed {len(trades)} trades ({trade_parser.format_type} format)")
            
            # Check for missing mappings
            missing_positions = len(input_parser.unmapped_symbols) if hasattr(input_parser, 'unmapped_symbols') else 0
            missing_trades = len(trade_parser.unmapped_symbols) if hasattr(trade_parser, 'unmapped_symbols') else 0
            
            if missing_positions > 0 or missing_trades > 0:
                st.warning(f"⚠️ Unmapped symbols: {missing_positions} from positions, {missing_trades} from trades")
            
            # Process trades
            position_manager = PositionManager()
            starting_positions_df = position_manager.initialize_from_positions(positions)
            
            trade_processor = TradeProcessor(position_manager)
            output_gen = OutputGenerator("output/stage1")
            
            parsed_trades_df = output_gen.create_trade_dataframe_from_positions(trades)
            processed_trades_df = trade_processor.process_trades(trades, trade_df)
            final_positions_df = position_manager.get_final_positions()
            
            # Generate output files
            output_files = output_gen.save_all_outputs(
                parsed_trades_df,
                starting_positions_df,
                processed_trades_df,
                final_positions_df,
                file_prefix="stage1",
                input_parser=input_parser,
                trade_parser=trade_parser
            )
            
            # Store in session state
            st.session_state.stage1_outputs = output_files
            st.session_state.dataframes['stage1'] = {
                'parsed_trades': parsed_trades_df,
                'starting_positions': starting_positions_df,
                'processed_trades': processed_trades_df,
                'final_positions': final_positions_df
            }
            st.session_state.stage1_complete = True
            st.session_state.processed_trades_for_acm = processed_trades_df
            
            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Starting Positions", len(starting_positions_df))
            with col2:
                st.metric("Trades Processed", len(trades))
            with col3:
                if 'Split?' in processed_trades_df.columns:
                    splits = len(processed_trades_df[processed_trades_df['Split?'] == 'Yes'])
                    st.metric("Split Trades", splits)
            with col4:
                st.metric("Final Positions", len(final_positions_df))
            
            st.success("✅ Stage 1 Complete!")
            return True
            
    except Exception as e:
        st.error(f"❌ Error in Stage 1: {str(e)}")
        st.code(traceback.format_exc())
        return False

def process_stage2(schema_option, custom_schema_file):
    """Process Stage 2: ACM Mapping"""
    try:
        with st.spinner("Processing Stage 2: ACM Mapping..."):
            if 'processed_trades_for_acm' not in st.session_state:
                st.error("❌ Stage 1 must be completed first")
                return False
            
            processed_trades_df = st.session_state.processed_trades_for_acm
            
            # Initialize ACM Mapper
            if schema_option == "Use built-in schema (default)":
                acm_mapper = ACMMapper()
                st.info("Using built-in ACM schema")
            else:
                if not custom_schema_file:
                    st.error("❌ Please upload a custom schema file")
                    return False
                
                temp_dir = get_temp_dir()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', dir=temp_dir) as tmp:
                    tmp.write(custom_schema_file.getbuffer())
                    schema_path = tmp.name
                
                acm_mapper = ACMMapper(schema_path)
                st.info(f"Using custom schema: {custom_schema_file.name}")
            
            st.session_state.acm_mapper = acm_mapper
            
            # Process to ACM format
            mapped_df, errors_df = acm_mapper.process_trades_to_acm(processed_trades_df)
            
            # Save outputs
            output_dir = Path("output/stage2")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            acm_file = output_dir / f"acm_listedtrades_{timestamp}.csv"
            mapped_df.to_csv(acm_file, index=False)
            
            errors_file = output_dir / f"acm_listedtrades_{timestamp}_errors.csv"
            errors_df.to_csv(errors_file, index=False)
            
            schema_file = output_dir / f"acm_schema_used_{timestamp}.xlsx"
            schema_bytes = acm_mapper.generate_schema_excel()
            with open(schema_file, 'wb') as f:
                f.write(schema_bytes)
            
            # Store in session state
            st.session_state.stage2_outputs = {
                'acm_mapped': acm_file,
                'errors': errors_file,
                'schema_used': schema_file
            }
            st.session_state.dataframes['stage2'] = {
                'mapped': mapped_df,
                'errors': errors_df
            }
            st.session_state.stage2_complete = True
            
            # Show results
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Records Mapped", len(mapped_df))
            with col2:
                st.metric("Validation Errors", len(errors_df))
            
            if len(errors_df) == 0:
                st.success("✅ Stage 2 Complete! No validation errors.")
            else:
                st.warning(f"⚠️ Stage 2 Complete with {len(errors_df)} validation errors.")
            
            return True
            
    except Exception as e:
        st.error(f"❌ Error in Stage 2: {str(e)}")
        st.code(traceback.format_exc())
        return False

def run_deliverables_calculation(usdinr_rate: float, fetch_prices: bool):
    """Run deliverables and IV calculations"""
    if not NEW_FEATURES_AVAILABLE:
        st.error("Deliverables module not available")
        return
        
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        with st.spinner("Calculating deliverables and intrinsic values..."):
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            prices = {}
            if fetch_prices:
                try:
                    from position_manager import PriceFetcher
                    fetcher = PriceFetcher()
                    
                    all_symbols = set()
                    if not starting_positions.empty and 'Symbol' in starting_positions.columns:
                        all_symbols.update(starting_positions['Symbol'].unique())
                    if not final_positions.empty and 'Symbol' in final_positions.columns:
                        all_symbols.update(final_positions['Symbol'].unique())
                    
                    if all_symbols:
                        for symbol in all_symbols:
                            price = fetcher.fetch_price_for_symbol(symbol)
                            if price:
                                prices[symbol] = price
                except:
                    st.warning("Could not fetch Yahoo prices")
            
            calc = DeliverableCalculator(usdinr_rate)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/DELIVERABLES_REPORT_{timestamp}.xlsx"
            
            calc.generate_deliverables_report(
                starting_positions,
                final_positions,
                prices,
                output_file,
                report_type="TRADE_PROCESSING"
            )
            
            st.session_state.deliverables_file = output_file
            st.session_state.deliverables_complete = True
            
            pre_deliv = calc.calculate_deliverables_from_dataframe(starting_positions, prices)
            post_deliv = calc.calculate_deliverables_from_dataframe(final_positions, prices)
            
            st.session_state.deliverables_data = {
                'pre_trade': pre_deliv,
                'post_trade': post_deliv,
                'prices': prices
            }
            
            st.success(f"✅ Deliverables calculated and saved!")
            
    except Exception as e:
        st.error(f"❌ Error calculating deliverables: {str(e)}")
        logger.error(traceback.format_exc())

def run_expiry_delivery_generation():
    """Generate physical delivery outputs per expiry date"""
    if not EXPIRY_DELIVERY_AVAILABLE:
        st.error("Expiry Delivery Generator module not available. Please ensure expiry_delivery_generator.py is in the directory.")
        return
    
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        with st.spinner("Generating expiry delivery reports..."):
            # Get positions from Stage 1
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            # Check if we have positions
            if starting_positions.empty and final_positions.empty:
                st.warning("No positions found to process for expiry deliveries")
                return
            
            # Show position counts
            st.info(f"Processing {len(starting_positions)} starting positions and {len(final_positions)} final positions")
            
            # Get prices if available
            prices = {}
            if st.session_state.get('use_yahoo_for_delivery', True):
                # Extract prices from dataframes if they have Yahoo_Price column
                for df in [starting_positions, final_positions]:
                    if not df.empty and 'Symbol' in df.columns:
                        for idx, row in df.iterrows():
                            if 'Yahoo_Price' in df.columns and pd.notna(row['Yahoo_Price']) and row['Yahoo_Price'] != 'N/A':
                                try:
                                    prices[row['Symbol']] = float(row['Yahoo_Price'])
                                except:
                                    pass
                
                st.info(f"Found {len(prices)} prices from Yahoo data")
            
            # Initialize generator
            generator = ExpiryDeliveryGenerator(usdinr_rate=st.session_state.get('usdinr_rate', 88.0))
            
            # Process positions by expiry
            pre_trade_results = generator.process_positions_by_expiry(
                starting_positions, prices, "Pre-Trade"
            )
            
            post_trade_results = generator.process_positions_by_expiry(
                final_positions, prices, "Post-Trade"
            )
            
            # Check if we got any results
            if not pre_trade_results and not post_trade_results:
                st.warning("No expiry positions found to process")
                return
            
            st.info(f"Found {len(set(list(pre_trade_results.keys()) + list(post_trade_results.keys())))} unique expiry dates")
            
            # Generate reports
            output_dir = "output/expiry_deliveries"
            output_files = generator.generate_expiry_reports(
                pre_trade_results, post_trade_results, output_dir
            )
            
            # Store in session state
            st.session_state.expiry_delivery_files = output_files
            st.session_state.expiry_delivery_results = {
                'pre_trade': pre_trade_results,
                'post_trade': post_trade_results
            }
            st.session_state.expiry_deliveries_complete = True
            
            # Show success with details
            if output_files:
                st.success(f"✅ Successfully generated {len(output_files)} expiry delivery reports!")
                
                # Show the expiry dates processed
                expiry_list = ", ".join([d.strftime('%Y-%m-%d') for d in sorted(output_files.keys())])
                st.info(f"Expiry dates processed: {expiry_list}")
            else:
                st.warning("No expiry delivery files were generated. Check if positions have valid expiry dates.")
            
    except Exception as e:
        st.error(f"❌ Error generating expiry deliveries: {str(e)}")
        st.code(traceback.format_exc())
        logger.error(traceback.format_exc())

def run_pms_reconciliation(pms_file):
    """Run PMS reconciliation"""
    if not NEW_FEATURES_AVAILABLE:
        st.error("Reconciliation module not available")
        return
        
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        with st.spinner("Running PMS reconciliation..."):
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(pms_file.name).suffix) as tmp:
                tmp.write(pms_file.getbuffer())
                pms_path = tmp.name
            
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            recon = EnhancedReconciliation()
            pms_df = recon.read_pms_file(pms_path)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/PMS_RECONCILIATION_{timestamp}.xlsx"
            
            recon.create_comprehensive_recon_report(
                starting_positions,
                final_positions,
                pms_df,
                output_file
            )
            
            st.session_state.recon_file = output_file
            st.session_state.recon_complete = True
            
            pre_recon = recon.reconcile_positions(starting_positions, pms_df, "Pre-Trade")
            post_recon = recon.reconcile_positions(final_positions, pms_df, "Post-Trade")
            
            st.session_state.recon_data = {
                'pre_trade': pre_recon,
                'post_trade': post_recon,
                'pms_df': pms_df
            }
            
            st.success(f"✅ Reconciliation complete!")
            
            try:
                os.unlink(pms_path)
            except:
                pass
                
    except Exception as e:
        st.error(f"❌ Error in reconciliation: {str(e)}")
        logger.error(traceback.format_exc())

# Display Functions

def display_pipeline_overview():
    """Display pipeline overview"""
    st.header("Pipeline Overview")
    
    col1, col2, col3 = st.columns([1, 0.1, 1])
    
    with col1:
        st.markdown('<div class="stage-header">Stage 1: Strategy Processing</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Position File
        - Trade File
        - Symbol Mapping
        
        **Processing:**
        - Bloomberg ticker generation
        - FULO/FUSH strategy assignment
        - Trade splitting
        - Position tracking
        
        **Output:**
        - Processed trades with strategies
        - Position summaries
        """)
        
        if st.session_state.stage1_complete:
            st.success("✅ Stage 1 Complete")
    
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("## →", unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="stage-header">Stage 2: ACM Mapping</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Processed trades from Stage 1
        - ACM schema
        
        **Processing:**
        - Field mapping
        - Transaction type logic
        - Validation
        
        **Output:**
        - ACM ListedTrades CSV
        - Error report
        """)
        
        if st.session_state.stage2_complete:
            st.success("✅ Stage 2 Complete")
    
    # Additional features overview
    if any([st.session_state.get('enable_deliverables'), 
            st.session_state.get('enable_expiry_delivery'),
            st.session_state.get('enable_recon')]):
        st.markdown("### Enhanced Features Enabled")
        
        cols = st.columns(3)
        feature_idx = 0
        
        if st.session_state.get('enable_deliverables'):
            with cols[feature_idx % 3]:
                st.markdown("**💰 Deliverables/IV**")
                if st.session_state.get('deliverables_complete'):
                    st.success("✅ Complete")
                else:
                    st.info("⏳ Pending")
            feature_idx += 1
        
        if st.session_state.get('enable_expiry_delivery'):
            with cols[feature_idx % 3]:
                st.markdown("**📅 Expiry Deliveries**")
                if st.session_state.get('expiry_deliveries_complete'):
                    st.success("✅ Complete")
                else:
                    st.info("⏳ Pending")
            feature_idx += 1
        
        if st.session_state.get('enable_recon'):
            with cols[feature_idx % 3]:
                st.markdown("**🔄 PMS Reconciliation**")
                if st.session_state.get('recon_complete'):
                    st.success("✅ Complete")
                else:
                    st.info("⏳ Pending")

def display_stage1_results():
    """Display Stage 1 results"""
    st.header("Stage 1: Strategy Processing Results")
    
    if not st.session_state.stage1_complete:
        st.info("Stage 1 has not been run yet.")
        return
    
    if 'stage1' not in st.session_state.dataframes:
        return
    
    data = st.session_state.dataframes['stage1']
    
    sub_tabs = st.tabs(["Processed Trades", "Starting Positions", "Final Positions", "Parsed Trades"])
    
    with sub_tabs[0]:
        df = data['processed_trades']
        st.subheader("Processed Trades")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[1]:
        df = data['starting_positions']
        st.subheader("Starting Positions")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[2]:
        df = data['final_positions']
        st.subheader("Final Positions")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[3]:
        df = data['parsed_trades']
        st.subheader("Parsed Trades")
        st.dataframe(df, use_container_width=True, height=400)

def display_stage2_results():
    """Display Stage 2 results"""
    st.header("Stage 2: ACM Mapping Results")
    
    if not st.session_state.stage2_complete:
        st.info("Stage 2 has not been run yet.")
        return
    
    if 'stage2' not in st.session_state.dataframes:
        return
    
    data = st.session_state.dataframes['stage2']
    
    sub_tabs = st.tabs(["ACM Mapped Data", "Validation Errors"])
    
    with sub_tabs[0]:
        df = data['mapped']
        st.subheader("ACM ListedTrades Format")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[1]:
        errors_df = data['errors']
        if len(errors_df) == 0:
            st.success("✅ No validation errors!")
        else:
            st.error(f"⚠️ {len(errors_df)} validation errors")
            st.dataframe(errors_df, use_container_width=True)

def display_deliverables_tab():
    """Display deliverables and IV analysis"""
    st.header("💰 Deliverables & Intrinsic Value Analysis")
    
    if not st.session_state.get('deliverables_complete'):
        st.info("Run the pipeline with deliverables enabled to see this analysis")
        return
    
    data = st.session_state.deliverables_data
    
    col1, col2, col3, col4 = st.columns(4)
    
    pre_deliv = data['pre_trade']
    post_deliv = data['post_trade']
    
    with col1:
        pre_total = pre_deliv['Deliverable_Lots'].sum() if not pre_deliv.empty else 0
        st.metric("Pre-Trade Deliverable (Lots)", f"{pre_total:,.0f}")
    
    with col2:
        post_total = post_deliv['Deliverable_Lots'].sum() if not post_deliv.empty else 0
        st.metric("Post-Trade Deliverable (Lots)", f"{post_total:,.0f}")
    
    with col3:
        change = post_total - pre_total
        st.metric("Deliverable Change", f"{change:,.0f}", delta=f"{change:+,.0f}")
    
    with col4:
        pre_iv = pre_deliv['Intrinsic_Value_INR'].sum() if not pre_deliv.empty else 0
        post_iv = post_deliv['Intrinsic_Value_INR'].sum() if not post_deliv.empty else 0
        iv_change = post_iv - pre_iv
        st.metric("IV Change (INR)", f"{iv_change:,.0f}", delta=f"{iv_change:+,.0f}")
    
    tab1, tab2, tab3 = st.tabs(["Pre-Trade Deliverables", "Post-Trade Deliverables", "Comparison"])
    
    with tab1:
        if not pre_deliv.empty:
            st.dataframe(pre_deliv, use_container_width=True, hide_index=True)
    
    with tab2:
        if not post_deliv.empty:
            st.dataframe(post_deliv, use_container_width=True, hide_index=True)
    
    with tab3:
        if not pre_deliv.empty and not post_deliv.empty:
            comparison = pd.merge(
                pre_deliv[['Ticker', 'Deliverable_Lots', 'Intrinsic_Value_INR']],
                post_deliv[['Ticker', 'Deliverable_Lots', 'Intrinsic_Value_INR']],
                on='Ticker',
                how='outer',
                suffixes=('_Pre', '_Post')
            ).fillna(0)
            
            comparison['Deliv_Change'] = comparison['Deliverable_Lots_Post'] - comparison['Deliverable_Lots_Pre']
            comparison['IV_Change'] = comparison['Intrinsic_Value_INR_Post'] - comparison['Intrinsic_Value_INR_Pre']
            
            st.dataframe(comparison, use_container_width=True, hide_index=True)

def display_expiry_deliveries_tab():
    """Display expiry delivery results"""
    st.header("📅 Expiry Physical Deliveries")
    
    if not st.session_state.get('expiry_deliveries_complete'):
        st.info("Run the pipeline with expiry deliveries enabled to see this analysis")
        return
    
    results = st.session_state.get('expiry_delivery_results', {})
    files = st.session_state.get('expiry_delivery_files', {})
    
    if not results and not files:
        st.warning("No expiry delivery data available")
        return
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    pre_results = results.get('pre_trade', {})
    post_results = results.get('post_trade', {})
    
    with col1:
        st.metric("Expiry Dates", len(files))
    
    with col2:
        pre_count = sum(len(data.get('derivatives', pd.DataFrame())) for data in pre_results.values())
        st.metric("Pre-Trade Deliveries", pre_count)
    
    with col3:
        post_count = sum(len(data.get('derivatives', pd.DataFrame())) for data in post_results.values())
        st.metric("Post-Trade Deliveries", post_count)
    
    # Download section
    st.markdown("### 📥 Download Expiry Reports")
    
    if files:
        # Create a grid of download buttons
        cols = st.columns(3)
        for idx, (expiry_date, file_path) in enumerate(sorted(files.items())):
            col_idx = idx % 3
            with cols[col_idx]:
                try:
                    with open(file_path, 'rb') as f:
                        st.download_button(
                            f"📅 {expiry_date.strftime('%Y-%m-%d')}",
                            data=f.read(),
                            file_name=f"EXPIRY_{expiry_date.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key=f"dl_expiry_{expiry_date}"
                        )
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    
    # Detailed view by expiry
    st.markdown("### 📊 Expiry Details")
    
    if pre_results or post_results:
        all_expiries = sorted(set(list(pre_results.keys()) + list(post_results.keys())))
        
        selected_expiry = st.selectbox(
            "Select Expiry Date",
            options=all_expiries,
            format_func=lambda x: x.strftime('%Y-%m-%d')
        )
        
        if selected_expiry:
            tabs = st.tabs(["Pre-Trade", "Post-Trade", "Comparison"])
            
            with tabs[0]:
                pre_data = pre_results.get(selected_expiry, {})
                if pre_data:
                    st.markdown(f"**Positions:** {pre_data.get('position_count', 0)}")
                    
                    # Show derivatives
                    deriv_df = pre_data.get('derivatives', pd.DataFrame())
                    if not deriv_df.empty:
                        st.markdown("**Derivative Trades:**")
                        st.dataframe(deriv_df, use_container_width=True, hide_index=True)
                    
                    # Show cash trades
                    cash_df = pre_data.get('cash_trades', pd.DataFrame())
                    if not cash_df.empty:
                        st.markdown("**Cash Trades:**")
                        st.info("Trade Notes: E=Exercise (long options), A=Assignment (short options)")
                        st.dataframe(cash_df, use_container_width=True, hide_index=True)
                    
                    # Show summary
                    summary_df = pre_data.get('cash_summary', pd.DataFrame())
                    if not summary_df.empty:
                        st.markdown("**Cash Summary:**")
                        
                        def highlight_rows(row):
                            if 'NET DELIVERABLE' in str(row.get('Type', '')) or 'GRAND TOTAL' in str(row.get('Underlying', '')):
                                return ['background-color: lightblue; font-weight: bold'] * len(row)
                            return [''] * len(row)
                        
                        styled_df = summary_df.style.apply(highlight_rows, axis=1)
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No pre-trade positions for this expiry")
            
            with tabs[1]:
                post_data = post_results.get(selected_expiry, {})
                if post_data:
                    st.markdown(f"**Positions:** {post_data.get('position_count', 0)}")
                    
                    # Show derivatives
                    deriv_df = post_data.get('derivatives', pd.DataFrame())
                    if not deriv_df.empty:
                        st.markdown("**Derivative Trades:**")
                        st.dataframe(deriv_df, use_container_width=True, hide_index=True)
                    
                    # Show cash trades
                    cash_df = post_data.get('cash_trades', pd.DataFrame())
                    if not cash_df.empty:
                        st.markdown("**Cash Trades:**")
                        st.info("Trade Notes: E=Exercise (long options), A=Assignment (short options)")
                        st.dataframe(cash_df, use_container_width=True, hide_index=True)
                    
                    # Show summary
                    summary_df = post_data.get('cash_summary', pd.DataFrame())
                    if not summary_df.empty:
                        st.markdown("**Cash Summary:**")
                        
                        def highlight_rows(row):
                            if 'NET DELIVERABLE' in str(row.get('Type', '')) or 'GRAND TOTAL' in str(row.get('Underlying', '')):
                                return ['background-color: lightblue; font-weight: bold'] * len(row)
                            return [''] * len(row)
                        
                        styled_df = summary_df.style.apply(highlight_rows, axis=1)
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No post-trade positions for this expiry")
            
            with tabs[2]:
                pre_data = pre_results.get(selected_expiry, {})
                post_data = post_results.get(selected_expiry, {})
                
                if pre_data and post_data:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Pre-Trade Metrics:**")
                        pre_deriv = len(pre_data.get('derivatives', pd.DataFrame()))
                        pre_cash = len(pre_data.get('cash_trades', pd.DataFrame()))
                        st.write(f"- Derivatives: {pre_deriv}")
                        st.write(f"- Cash Trades: {pre_cash}")
                    
                    with col2:
                        st.markdown("**Post-Trade Metrics:**")
                        post_deriv = len(post_data.get('derivatives', pd.DataFrame()))
                        post_cash = len(post_data.get('cash_trades', pd.DataFrame()))
                        st.write(f"- Derivatives: {post_deriv}")
                        st.write(f"- Cash Trades: {post_cash}")
                    
                    # Show changes
                    st.markdown("**Changes:**")
                    st.write(f"- Derivative Trades: {post_deriv - pre_deriv:+d}")
                    st.write(f"- Cash Trades: {post_cash - pre_cash:+d}")
                else:
                    st.info("Comparison requires both pre and post trade data")

def display_reconciliation_tab():
    """Display PMS reconciliation results"""
    st.header("🔄 PMS Position Reconciliation")
    
    if not st.session_state.get('recon_complete'):
        st.info("Run the pipeline with PMS reconciliation enabled to see this analysis")
        return
    
    data = st.session_state.recon_data
    pre_recon = data['pre_trade']
    post_recon = data['post_trade']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pre-Trade Reconciliation")
        st.metric("Total Discrepancies", pre_recon['summary']['total_discrepancies'])
        st.metric("Matched Positions", pre_recon['summary']['matched_count'])
        st.metric("Mismatches", pre_recon['summary']['mismatch_count'])
    
    with col2:
        st.subheader("Post-Trade Reconciliation")
        st.metric("Total Discrepancies", post_recon['summary']['total_discrepancies'])
        st.metric("Matched Positions", post_recon['summary']['matched_count'])
        st.metric("Mismatches", post_recon['summary']['mismatch_count'])
    
    if pre_recon['position_mismatches'] or post_recon['position_mismatches']:
        st.subheader("Position Mismatches")
        
        tab1, tab2 = st.tabs(["Pre-Trade", "Post-Trade"])
        
        with tab1:
            if pre_recon['position_mismatches']:
                df = pd.DataFrame(pre_recon['position_mismatches'])
                st.dataframe(df, use_container_width=True, hide_index=True)
        
        with tab2:
            if post_recon['position_mismatches']:
                df = pd.DataFrame(post_recon['position_mismatches'])
                st.dataframe(df, use_container_width=True, hide_index=True)

def display_downloads():
    """Display download section"""
    st.header("📥 Download Outputs")
    
    # Check if we need 4 columns instead of 3
    if st.session_state.get('expiry_deliveries_complete', False):
        cols = st.columns(4)
    else:
        cols = st.columns(3)
    
    with cols[0]:
        st.markdown("### Stage 1 Outputs")
        
        if st.session_state.stage1_complete and st.session_state.stage1_outputs:
            for key, path in st.session_state.stage1_outputs.items():
                if path and Path(path).exists():
                    try:
                        with open(path, 'rb') as f:
                            data = f.read()
                        
                        mime = 'text/csv'
                        if 'excel' in key:
                            mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        elif 'summary' in key:
                            mime = 'text/plain'
                        
                        label = key.replace('_', ' ').title()
                        st.download_button(
                            f"📄 {label}",
                            data,
                            file_name=Path(path).name,
                            mime=mime,
                            key=f"dl_s1_{key}",
                            use_container_width=True
                        )
                    except:
                        pass
        else:
            st.info("No outputs yet")
    
    with cols[1]:
        st.markdown("### Stage 2 Outputs")
        
        if st.session_state.stage2_complete and st.session_state.stage2_outputs:
            for key, path in st.session_state.stage2_outputs.items():
                if path and Path(path).exists():
                    try:
                        with open(path, 'rb') as f:
                            data = f.read()
                        
                        if 'acm' in key:
                            label = "📊 ACM ListedTrades"
                        elif 'error' in key:
                            label = "⚠️ Validation Errors"
                        elif 'schema' in key:
                            label = "📘 Schema Used"
                        else:
                            label = key.title()
                        
                        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if 'schema' in key else 'text/csv'
                        
                        st.download_button(
                            label,
                            data,
                            file_name=Path(path).name,
                            mime=mime,
                            key=f"dl_s2_{key}",
                            use_container_width=True
                        )
                    except:
                        pass
        else:
            st.info("No outputs yet")
    
    with cols[2]:
        st.markdown("### Enhanced Reports")
        
        # Deliverables download
        if st.session_state.get('deliverables_file'):
            try:
                with open(st.session_state.deliverables_file, 'rb') as f:
                    st.download_button(
                        "💰 Deliverables Report",
                        f.read(),
                        file_name=Path(st.session_state.deliverables_file).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_deliverables"
                    )
            except:
                pass
        
        # Reconciliation download
        if st.session_state.get('recon_file'):
            try:
                with open(st.session_state.recon_file, 'rb') as f:
                    st.download_button(
                        "🔄 Reconciliation Report",
                        f.read(),
                        file_name=Path(st.session_state.recon_file).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_recon"
                    )
            except:
                pass
        
        if not st.session_state.get('deliverables_file') and not st.session_state.get('recon_file'):
            st.info("Enable additional features in sidebar")
    
    # Add Expiry Deliveries column if available
    if st.session_state.get('expiry_deliveries_complete', False):
        with cols[3]:
            st.markdown("### 📅 Expiry Deliveries")
            
            files = st.session_state.get('expiry_delivery_files', {})
            if files:
                # Show count of files
                st.info(f"Generated {len(files)} expiry reports")
                
                # Download buttons for each expiry
                for expiry_date, file_path in sorted(files.items()):
                    try:
                        with open(file_path, 'rb') as f:
                            st.download_button(
                                f"📅 {expiry_date.strftime('%Y-%m-%d')}",
                                data=f.read(),
                                file_name=f"EXPIRY_{expiry_date.strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key=f"dl_main_expiry_{expiry_date.strftime('%Y%m%d')}"
                            )
                    except Exception as e:
                        st.error(f"Error loading {expiry_date}: {str(e)}")
            else:
                st.warning("No expiry files generated")

def display_schema_info():
    """Display schema information"""
    st.header("📘 ACM Schema Information")
    
    tab1, tab2, tab3 = st.tabs(["Current Schema", "Field Mappings", "Transaction Rules"])
    
    with tab1:
        st.subheader("Current Schema Structure")
        
        mapper = st.session_state.acm_mapper if st.session_state.acm_mapper else ACMMapper()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Output Columns")
            for i, col in enumerate(mapper.columns_order, 1):
                mandatory = "🔴" if col in mapper.mandatory_columns else "⚪"
                st.write(f"{i}. {mandatory} {col}")
        
        with col2:
            st.markdown("#### Mandatory Fields")
            for col in mapper.mandatory_columns:
                st.write(f"✔ {col}")
    
    with tab2:
        st.subheader("Field Mapping Rules")
        
        mapping_data = []
        for col, rule in mapper.mapping_rules.items():
            mapping_data.append({
                "ACM Field": col,
                "Source": rule,
                "Required": "Yes" if col in mapper.mandatory_columns else "No"
            })
        
        mapping_df = pd.DataFrame(mapping_data)
        st.dataframe(mapping_df, use_container_width=True)
    
    with tab3:
        st.subheader("Transaction Type Rules")
        
        st.markdown("""
        Transaction Type is determined by combining **B/S** and **Opposite?** flags:
        """)
        
        rules_df = pd.DataFrame([
            {"B/S": "Buy", "Opposite?": "No", "→ Transaction Type": "Buy"},
            {"B/S": "Buy", "Opposite?": "Yes", "→ Transaction Type": "BuyToCover"},
            {"B/S": "Sell", "Opposite?": "No", "→ Transaction Type": "SellShort"},
            {"B/S": "Sell", "Opposite?": "Yes", "→ Transaction Type": "Sell"}
        ])
        
        st.dataframe(rules_df, use_container_width=True, hide_index=True)

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    Enhanced Trade Processing Pipeline v3.5 | Complete with Expiry Physical Delivery
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
