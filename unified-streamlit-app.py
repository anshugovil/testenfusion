"""
Unified Trade Processing Pipeline - Fixed Import Version
With schema export and optional custom schema upload
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

# Import modules from root directory (no modules folder)
try:
    from input_parser import InputParser
    from Trade_Parser import TradeParser  # Note: Capital T and P based on your filename
    from position_manager import PositionManager
    from trade_processor import TradeProcessor
    from output_generator import OutputGenerator
    from acm_mapper import ACMMapper
except ModuleNotFoundError as e:
    st.error(f"Failed to import modules: {e}")
    st.error("Please ensure all module files are in the same directory as this app")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Trade Processing Pipeline",
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
    </style>
    """, unsafe_allow_html=True)

def ensure_directories():
    """Ensure required directories exist"""
    dirs = ["output", "output/stage1", "output/stage2", "temp"]
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
    st.title("🎯 Unified Trade Processing Pipeline")
    st.markdown("### Complete pipeline from raw trades to ACM ListedTrades format")
    
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
                st.success(f"✓ Using {Path(default_mapping).name}")
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
        
        # Stage 2 Section with Schema Options
        st.markdown("### Stage 2: ACM Mapping")
        
        # Schema options
        schema_option = st.radio(
            "Schema Configuration:",
            ["Use built-in schema (default)", "Upload custom schema"],
            index=0,
            key="schema_option"
        )
        
        custom_schema_file = None
        
        if schema_option == "Use built-in schema (default)":
            st.info("✅ Will use hardcoded ACM format")
            
            # Download schema button
            st.markdown("#### Download Schema Template")
            
            # Generate schema file
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
            
            st.markdown("""
            <div class='info-box'>
            <small>
            ℹ️ This template shows the default ACM format. 
            You can download it to understand the structure or 
            customize it for your needs.
            </small>
            </div>
            """, unsafe_allow_html=True)
            
        else:  # Upload custom schema
            st.warning("📤 Upload your custom schema file")
            custom_schema_file = st.file_uploader(
                "Upload ACM Schema",
                type=['xlsx', 'xls'],
                key='custom_schema_file',
                help="Excel file with 'Columns' sheet defining the ACM format"
            )
            
            if custom_schema_file:
                st.success(f"✓ Will use custom schema: {custom_schema_file.name}")
        
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
        
        if can_process_stage1:
            st.divider()
            if st.button("⚡ Run Complete Pipeline", type="primary", use_container_width=True):
                if process_stage1(position_file, trade_file, mapping_file, use_default_mapping, default_mapping):
                    process_stage2(schema_option, custom_schema_file)
        
        st.divider()
        if st.button("🔄 Reset All", type="secondary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Main content tabs
    tabs = st.tabs([
        "📊 Pipeline Overview",
        "📄 Stage 1: Strategy Processing", 
        "📋 Stage 2: ACM Mapping",
        "📥 Downloads",
        "📘 Schema Info"
    ])
    
    with tabs[0]:
        display_pipeline_overview()
    
    with tabs[1]:
        display_stage1_results()
    
    with tabs[2]:
        display_stage2_results()
    
    with tabs[3]:
        display_downloads()
    
    with tabs[4]:
        display_schema_info()

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
    """Process Stage 2: ACM Mapping with optional custom schema"""
    try:
        with st.spinner("Processing Stage 2: ACM Mapping..."):
            if 'processed_trades_for_acm' not in st.session_state:
                st.error("❌ Stage 1 must be completed first")
                return False
            
            processed_trades_df = st.session_state.processed_trades_for_acm
            
            # Initialize ACM Mapper based on schema option
            if schema_option == "Use built-in schema (default)":
                # Use hardcoded schema
                acm_mapper = ACMMapper()
                st.info("Using built-in ACM schema")
            else:
                # Use custom schema
                if not custom_schema_file:
                    st.error("❌ Please upload a custom schema file")
                    return False
                
                # Save custom schema to temp file
                temp_dir = get_temp_dir()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', dir=temp_dir) as tmp:
                    tmp.write(custom_schema_file.getbuffer())
                    schema_path = tmp.name
                
                acm_mapper = ACMMapper(schema_path)
                st.info(f"Using custom schema: {custom_schema_file.name}")
            
            # Store mapper for schema info display
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
            
            # Also save the schema used
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
                with st.expander("View Errors"):
                    st.dataframe(errors_df, use_container_width=True)
            
            return True
            
    except Exception as e:
        st.error(f"❌ Error in Stage 2: {str(e)}")
        st.code(traceback.format_exc())
        return False

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
        - ACM schema (built-in or custom)
        
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

def display_downloads():
    """Display download section"""
    st.header("📥 Download Outputs")
    
    col1, col2 = st.columns(2)
    
    with col1:
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
    
    with col2:
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

def display_schema_info():
    """Display schema information"""
    st.header("📘 ACM Schema Information")
    
    tab1, tab2, tab3 = st.tabs(["Current Schema", "Field Mappings", "Transaction Rules"])
    
    with tab1:
        st.subheader("Current Schema Structure")
        
        # Get current mapper
        mapper = st.session_state.acm_mapper if st.session_state.acm_mapper else ACMMapper()
        
        # Display columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Output Columns")
            for i, col in enumerate(mapper.columns_order, 1):
                mandatory = "🔴" if col in mapper.mandatory_columns else "⚪"
                st.write(f"{i}. {mandatory} {col}")
        
        with col2:
            st.markdown("#### Mandatory Fields")
            for col in mapper.mandatory_columns:
                st.write(f"✓ {col}")
    
    with tab2:
        st.subheader("Field Mapping Rules")
        
        # Create mapping table
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
    Trade Processing Pipeline v2.1 | With Schema Export & Custom Upload Support
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
