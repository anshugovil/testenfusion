"""
Unified Trade Processing Pipeline
Combines Strategy Processing (Stage 1) and ACM Mapping (Stage 2)
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

# Add modules directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

# Import all modules
from modules.input_parser import InputParser
from modules.trade_parser import TradeParser  
from modules.position_manager import PositionManager
from modules.trade_processor import TradeProcessor
from modules.output_generator import OutputGenerator
from modules.acm_mapper import ACMMapper

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
    </style>
    """, unsafe_allow_html=True)

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
    
    # Sidebar
    with st.sidebar:
        st.header("📁 Input Files")
        
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
        
        # Mapping file selection
        st.subheader("3. Mapping File")
        default_mapping = None
        for name in ["futures_mapping.csv", "futures mapping.csv", "data/futures_mapping.csv"]:
            if Path(name).exists():
                default_mapping = name
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
                key='mapping_file'
            )
            use_default_mapping = None
        
        st.divider()
        
        st.markdown("### Stage 2: ACM Mapping")
        
        # ACM Schema file
        default_schema = None
        for name in ["acm_schema.xlsx", "data/acm_schema.xlsx"]:
            if Path(name).exists():
                default_schema = name
                break
        
        if default_schema:
            use_default_schema = st.radio(
                "Schema source:",
                ["Use default from repository", "Upload custom"],
                index=0,
                key="schema_radio"
            )
            
            if use_default_schema == "Upload custom":
                schema_file = st.file_uploader(
                    "Upload ACM Schema",
                    type=['xlsx', 'xls'],
                    key='schema_file',
                    help="Excel file with 'Columns' sheet"
                )
            else:
                schema_file = None
                st.success(f"✔ Using {Path(default_schema).name}")
        else:
            st.info("Upload ACM schema file")
            schema_file = st.file_uploader(
                "Upload ACM Schema",
                type=['xlsx', 'xls'],
                key='schema_file',
                help="Excel file with 'Columns' sheet"
            )
            use_default_schema = None
        
        st.divider()
        
        # Process button
        can_process_stage1 = (
            position_file is not None and 
            trade_file is not None and 
            (mapping_file is not None or (use_default_mapping == "Use default from repository" and default_mapping))
        )
        
        can_process_stage2 = (
            st.session_state.stage1_complete and
            (schema_file is not None or (use_default_schema == "Use default from repository" and default_schema))
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚀 Run Stage 1", type="primary", use_container_width=True, disabled=not can_process_stage1):
                process_stage1(position_file, trade_file, mapping_file, use_default_mapping, default_mapping)
        
        with col2:
            if st.button("🎯 Run Stage 2", type="secondary", use_container_width=True, disabled=not can_process_stage2):
                process_stage2(schema_file, use_default_schema, default_schema)
        
        if can_process_stage1 and (schema_file or default_schema):
            st.divider()
            if st.button("⚡ Run Complete Pipeline", type="primary", use_container_width=True):
                # Run both stages
                if process_stage1(position_file, trade_file, mapping_file, use_default_mapping, default_mapping):
                    process_stage2(schema_file, use_default_schema, default_schema)
    
    # Main content area
    tabs = st.tabs([
        "📊 Pipeline Overview",
        "🔄 Stage 1: Strategy Processing", 
        "📋 Stage 2: ACM Mapping",
        "📥 Downloads"
    ])
    
    with tabs[0]:
        display_pipeline_overview()
    
    with tabs[1]:
        display_stage1_results()
    
    with tabs[2]:
        display_stage2_results()
    
    with tabs[3]:
        display_downloads()

def process_stage1(position_file, trade_file, mapping_file, use_default, default_path):
    """Process Stage 1: Strategy Assignment"""
    try:
        with st.spinner("Processing Stage 1: Strategy Assignment..."):
            # Save uploaded files
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(position_file.name).suffix) as tmp:
                tmp.write(position_file.getbuffer())
                pos_path = tmp.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(trade_file.name).suffix) as tmp:
                tmp.write(trade_file.getbuffer())
                trade_path = tmp.name
            
            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
                    tmp.write(mapping_file.getbuffer())
                    map_path = tmp.name
            else:
                map_path = default_path
            
            # Parse positions
            input_parser = InputParser(map_path)
            positions = input_parser.parse_file(pos_path)
            
            if not positions:
                st.error("❌ No positions found in Stage 1")
                return False
            
            # Parse trades
            trade_parser = TradeParser(map_path)
            
            if trade_path.endswith('.csv'):
                trade_df = pd.read_csv(trade_path, header=None)
            else:
                trade_df = pd.read_excel(trade_path, header=None)
            
            trades = trade_parser.parse_trade_file(trade_path)
            
            if not trades:
                st.error("❌ No trades found in Stage 1")
                return False
            
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
            
            # Store for Stage 2
            st.session_state.processed_trades_for_acm = processed_trades_df
            
            st.success("✅ Stage 1 Complete!")
            return True
            
    except Exception as e:
        st.error(f"❌ Error in Stage 1: {str(e)}")
        st.code(traceback.format_exc())
        return False

def process_stage2(schema_file, use_default, default_path):
    """Process Stage 2: ACM Mapping"""
    try:
        with st.spinner("Processing Stage 2: ACM Mapping..."):
            # Get processed trades from Stage 1
            if 'processed_trades_for_acm' not in st.session_state:
                st.error("❌ Stage 1 must be completed first")
                return False
            
            processed_trades_df = st.session_state.processed_trades_for_acm
            
            # Get schema path
            if schema_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                    tmp.write(schema_file.getbuffer())
                    schema_path = tmp.name
            else:
                schema_path = default_path
            
            # Initialize ACM Mapper
            acm_mapper = ACMMapper(schema_path)
            
            # Process to ACM format
            mapped_df, errors_df = acm_mapper.process_trades_to_acm(processed_trades_df)
            
            # Save outputs
            output_dir = Path("output/stage2")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save ACM mapped file
            acm_file = output_dir / f"acm_listedtrades_{timestamp}.csv"
            mapped_df.to_csv(acm_file, index=False)
            
            # Save errors file
            errors_file = output_dir / f"acm_listedtrades_{timestamp}_errors.csv"
            errors_df.to_csv(errors_file, index=False)
            
            # Store in session state
            st.session_state.stage2_outputs = {
                'acm_mapped': acm_file,
                'errors': errors_file
            }
            st.session_state.dataframes['stage2'] = {
                'mapped': mapped_df,
                'errors': errors_df
            }
            st.session_state.stage2_complete = True
            
            if len(errors_df) == 0:
                st.success("✅ Stage 2 Complete! No validation errors.")
            else:
                st.warning(f"⚠️ Stage 2 Complete with {len(errors_df)} validation errors.")
            
            return True
            
    except Exception as e:
        st.error(f"❌ Error in Stage 2: {str(e)}")
        st.code(traceback.format_exc())
        return False

def display_pipeline_overview():
    """Display pipeline overview"""
    st.header("Pipeline Overview")
    
    # Pipeline flow diagram
    col1, col2, col3 = st.columns([1, 0.1, 1])
    
    with col1:
        st.markdown('<div class="stage-header">Stage 1: Strategy Processing</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Position File (BOD/Contract/MS)
        - Trade File (MS format)
        - Symbol Mapping
        
        **Processing:**
        - Bloomberg ticker generation
        - Strategy assignment (FULO/FUSH)
        - Trade splitting for position flips
        - Position tracking
        
        **Output:**
        - Processed trades with strategies
        - Starting/final positions
        - Missing mappings report
        """)
        
        if st.session_state.stage1_complete:
            st.success("✅ Stage 1 Complete")
            if 'stage1' in st.session_state.dataframes:
                df = st.session_state.dataframes['stage1']['processed_trades']
                col1_1, col1_2, col1_3 = st.columns(3)
                with col1_1:
                    st.metric("Total Trades", len(df))
                with col1_2:
                    if 'Strategy' in df.columns:
                        st.metric("FULO", len(df[df['Strategy'] == 'FULO']))
                with col1_3:
                    if 'Strategy' in df.columns:
                        st.metric("FUSH", len(df[df['Strategy'] == 'FUSH']))
    
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("## →", unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="stage-header">Stage 2: ACM Mapping</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Processed trades from Stage 1
        - ACM schema definition
        
        **Processing:**
        - Field mapping to ACM format
        - Transaction type determination
        - Timestamp generation (Singapore TZ)
        - Mandatory field validation
        
        **Output:**
        - ACM ListedTrades CSV
        - Validation error report
        """)
        
        if st.session_state.stage2_complete:
            st.success("✅ Stage 2 Complete")
            if 'stage2' in st.session_state.dataframes:
                mapped_df = st.session_state.dataframes['stage2']['mapped']
                errors_df = st.session_state.dataframes['stage2']['errors']
                col3_1, col3_2 = st.columns(2)
                with col3_1:
                    st.metric("ACM Records", len(mapped_df))
                with col3_2:
                    st.metric("Validation Errors", len(errors_df))

def display_stage1_results():
    """Display Stage 1 results"""
    st.header("Stage 1: Strategy Processing Results")
    
    if not st.session_state.stage1_complete:
        st.info("Stage 1 has not been run yet. Please process files using the sidebar.")
        return
    
    if 'stage1' not in st.session_state.dataframes:
        st.warning("No data available from Stage 1")
        return
    
    data = st.session_state.dataframes['stage1']
    
    # Create sub-tabs
    sub_tabs = st.tabs([
        "Processed Trades",
        "Starting Positions",
        "Final Positions",
        "Parsed Trades"
    ])
    
    with sub_tabs[0]:
        df = data['processed_trades']
        st.subheader("Processed Trades with Strategy Assignment")
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total", len(df))
        if 'Strategy' in df.columns:
            with col2:
                st.metric("FULO", len(df[df['Strategy'] == 'FULO']))
            with col3:
                st.metric("FUSH", len(df[df['Strategy'] == 'FUSH']))
        if 'Split?' in df.columns:
            with col4:
                st.metric("Splits", len(df[df['Split?'] == 'Yes']))
        
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
        st.info("Stage 2 has not been run yet. Complete Stage 1 first, then run Stage 2.")
        return
    
    if 'stage2' not in st.session_state.dataframes:
        st.warning("No data available from Stage 2")
        return
    
    data = st.session_state.dataframes['stage2']
    
    # Create sub-tabs
    sub_tabs = st.tabs(["ACM Mapped Data", "Validation Errors"])
    
    with sub_tabs[0]:
        df = data['mapped']
        st.subheader("ACM ListedTrades Format")
        
        # Metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            if 'Transaction Type' in df.columns:
                unique_types = df['Transaction Type'].nunique()
                st.metric("Transaction Types", unique_types)
        with col3:
            if 'Strategy' in df.columns:
                strategies = df['Strategy'].nunique()
                st.metric("Strategies", strategies)
        
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[1]:
        errors_df = data['errors']
        if len(errors_df) == 0:
            st.success("✅ No validation errors found!")
        else:
            st.error(f"⚠️ Found {len(errors_df)} validation errors")
            st.dataframe(errors_df, use_container_width=True, height=400)

def display_downloads():
    """Display download section"""
    st.header("📥 Download Outputs")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Stage 1: Strategy Processing")
        
        if st.session_state.stage1_complete and st.session_state.stage1_outputs:
            for key, path in st.session_state.stage1_outputs.items():
                if path and Path(path).exists():
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
                        key=f"dl_stage1_{key}",
                        use_container_width=True
                    )
        else:
            st.info("No Stage 1 outputs available yet")
    
    with col2:
        st.markdown("### Stage 2: ACM Mapping")
        
        if st.session_state.stage2_complete and st.session_state.stage2_outputs:
            for key, path in st.session_state.stage2_outputs.items():
                if path and Path(path).exists():
                    with open(path, 'rb') as f:
                        data = f.read()
                    
                    label = "ACM ListedTrades" if 'acm' in key else "Validation Errors"
                    st.download_button(
                        f"📄 {label}",
                        data,
                        file_name=Path(path).name,
                        mime='text/csv',
                        key=f"dl_stage2_{key}",
                        use_container_width=True
                    )
        else:
            st.info("No Stage 2 outputs available yet")
    
    # Reset button
    st.divider()
    if st.button("🔄 Reset Pipeline", type="secondary", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# Footer
def display_footer():
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        Trade Processing Pipeline v2.0 | Combines Strategy Processing + ACM Mapping | 
        Built with Streamlit
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
    display_footer()