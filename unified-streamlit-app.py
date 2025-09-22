"""
Enhanced Unified Trade Processing Pipeline - STREAMLIT CLOUD VERSION
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

# Fix module imports for Streamlit Cloud
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Import modules
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
        from expiry_delivery_module import ExpiryDeliveryGenerator
        EXPIRY_DELIVERY_AVAILABLE = True
    except ImportError:
        EXPIRY_DELIVERY_AVAILABLE = False
        
except ModuleNotFoundError as e:
    st.error(f"Failed to import modules: {e}")
    st.error("Please ensure all module files are in the repository")
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

# Custom CSS (keep your existing CSS)
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
        border: 2px solid #007bff;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    .deliverable-header {
        background-color: #007bff;
        color: white;
        padding: 8px;
        border-radius: 4px;
        margin: 5px 0;
    }
    </style>
    """, unsafe_allow_html=True)

def ensure_directories():
    """Ensure required directories exist - Streamlit Cloud Compatible"""
    temp_dir = tempfile.gettempdir()
    dirs = [
        os.path.join(temp_dir, "output"),
        os.path.join(temp_dir, "output", "stage1"),
        os.path.join(temp_dir, "output", "stage2"),
        os.path.join(temp_dir, "output", "expiry_deliveries")
    ]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    return temp_dir

# Get temp directory base
TEMP_BASE = ensure_directories()

def get_temp_dir():
    """Get temporary directory that works on Streamlit Cloud"""
    return Path(TEMP_BASE)

# Update the process_stage1 function to use temp directories
def process_stage1(position_file, trade_file, mapping_file, use_default, default_path):
    """Process Stage 1: Strategy Assignment"""
    try:
        with st.spinner("Processing Stage 1: Strategy Assignment..."):
            temp_dir = get_temp_dir()
            
            # Save uploaded files
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(position_file.name).suffix, dir=str(temp_dir)) as tmp:
                tmp.write(position_file.getbuffer())
                pos_path = tmp.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(trade_file.name).suffix, dir=str(temp_dir)) as tmp:
                tmp.write(trade_file.getbuffer())
                trade_path = tmp.name
            
            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir=str(temp_dir)) as tmp:
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
            
            # Process trades - Use temp directory for outputs
            position_manager = PositionManager()
            starting_positions_df = position_manager.initialize_from_positions(positions)
            
            trade_processor = TradeProcessor(position_manager)
            output_gen = OutputGenerator(os.path.join(str(temp_dir), "output", "stage1"))
            
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

# Keep the rest of your main() function and other functions the same
# Just ensure all file operations use the temp directory
