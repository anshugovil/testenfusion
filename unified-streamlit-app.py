"""
Enhanced Unified Trade Processing Pipeline
Includes Deliverables, IV calculations, and PMS Reconciliation
ADDS TO EXISTING FUNCTIONALITY WITHOUT BREAKING IT
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

# Import existing modules (keep all existing imports)
from modules.input_parser import InputParser
from modules.trade_parser import TradeParser  
from modules.position_manager import PositionManager
from modules.trade_processor import TradeProcessor
from modules.output_generator import OutputGenerator
from modules.acm_mapper import ACMMapper

# Import NEW modules
from modules.deliverables_calculator import DeliverableCalculator
from modules.enhanced_recon_module import EnhancedReconciliation

# Keep all existing configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Trade Processing Pipeline - Enhanced",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# [KEEP ALL EXISTING CSS AND FUNCTIONS FROM YOUR ORIGINAL APP]
# ... (all the existing code remains the same)

def main():
    st.title("🎯 Enhanced Trade Processing Pipeline")
    st.markdown("### Complete pipeline with Deliverables & Reconciliation")
    
    # Initialize session state (ADD NEW STATES)
    if 'stage1_complete' not in st.session_state:
        st.session_state.stage1_complete = False
    if 'stage2_complete' not in st.session_state:
        st.session_state.stage2_complete = False
    if 'deliverables_complete' not in st.session_state:
        st.session_state.deliverables_complete = False
    if 'recon_complete' not in st.session_state:
        st.session_state.recon_complete = False
    # ... (keep all existing session states)
    
    # Sidebar with enhanced options
    with st.sidebar:
        st.header("📂 Input Files")
        
        # [KEEP ALL EXISTING FILE UPLOADERS]
        # Stage 1 inputs...
        # Stage 2 inputs...
        
        st.divider()
        
        # NEW: Deliverables & Reconciliation Section
        st.markdown("### 📊 Additional Features")
        
        # Enable deliverables calculation
        enable_deliverables = st.checkbox(
            "Enable Deliverables/IV Calculation",
            value=False,
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
                    step=0.1
                )
            with col2:
                fetch_prices = st.checkbox(
                    "Fetch Yahoo Prices",
                    value=True,
                    help="Fetch current prices from Yahoo Finance"
                )
        
        # Enable reconciliation
        enable_recon = st.checkbox(
            "Enable PMS Reconciliation",
            value=False,
            help="Compare positions with PMS file"
        )
        
        if enable_recon:
            pms_file = st.file_uploader(
                "Upload PMS Position File",
                type=['xlsx', 'xls', 'csv'],
                key='pms_file',
                help="File with Symbol and Position columns"
            )
        
        st.divider()
        
        # [KEEP ALL EXISTING PROCESS BUTTONS]
        # But add enhanced pipeline button
        if st.button("⚡ Run Full Enhanced Pipeline", type="primary", use_container_width=True):
            run_enhanced_pipeline(
                enable_deliverables=enable_deliverables,
                enable_recon=enable_recon,
                usdinr_rate=usdinr_rate if enable_deliverables else 88.0,
                fetch_prices=fetch_prices if enable_deliverables else False,
                pms_file=pms_file if enable_recon else None
            )
    
    # Main content tabs (ENHANCED)
    tab_list = ["📊 Pipeline Overview", "📄 Stage 1: Strategy", "📋 Stage 2: ACM"]
    
    if enable_deliverables:
        tab_list.append("💰 Deliverables & IV")
    
    if enable_recon:
        tab_list.append("🔄 PMS Reconciliation")
    
    tab_list.append("📥 Downloads")
    
    tabs = st.tabs(tab_list)
    
    # [KEEP ALL EXISTING TAB CONTENT]
    # ... existing tabs code ...
    
    # NEW: Deliverables Tab
    if enable_deliverables:
        deliverables_tab_index = tab_list.index("💰 Deliverables & IV")
        with tabs[deliverables_tab_index]:
            display_deliverables_tab()
    
    # NEW: Reconciliation Tab
    if enable_recon:
        recon_tab_index = tab_list.index("🔄 PMS Reconciliation")
        with tabs[recon_tab_index]:
            display_reconciliation_tab()
    
    # Enhanced Downloads Tab
    with tabs[-1]:
        display_enhanced_downloads()

def run_enhanced_pipeline(**kwargs):
    """Run the complete enhanced pipeline"""
    try:
        # First run existing Stage 1 & 2
        # [CALL YOUR EXISTING STAGE 1 & 2 FUNCTIONS]
        
        # Then run new features if enabled
        if kwargs.get('enable_deliverables'):
            run_deliverables_calculation(
                kwargs.get('usdinr_rate', 88.0),
                kwargs.get('fetch_prices', False)
            )
        
        if kwargs.get('enable_recon') and kwargs.get('pms_file'):
            run_pms_reconciliation(kwargs.get('pms_file'))
        
        st.success("✅ Enhanced pipeline complete!")
        st.balloons()
        
    except Exception as e:
        st.error(f"❌ Pipeline error: {str(e)}")
        logger.error(traceback.format_exc())

def run_deliverables_calculation(usdinr_rate: float, fetch_prices: bool):
    """Run deliverables and IV calculations"""
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        with st.spinner("Calculating deliverables and intrinsic values..."):
            # Get positions from Stage 1
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            # Fetch prices if enabled
            prices = {}
            if fetch_prices:
                # Get unique symbols
                all_symbols = set()
                if not starting_positions.empty and 'Symbol' in starting_positions.columns:
                    all_symbols.update(starting_positions['Symbol'].unique())
                if not final_positions.empty and 'Symbol' in final_positions.columns:
                    all_symbols.update(final_positions['Symbol'].unique())
                
                if all_symbols:
                    from modules.price_fetcher import PriceFetcher
                    fetcher = PriceFetcher()
                    prices = fetcher.fetch_prices_for_symbols(list(all_symbols))
            
            # Calculate deliverables
            calc = DeliverableCalculator(usdinr_rate)
            
            # Generate report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"DELIVERABLES_REPORT_{timestamp}.xlsx"
            
            calc.generate_deliverables_report(
                starting_positions,
                final_positions,
                prices,
                output_file,
                report_type="TRADE_PROCESSING"
            )
            
            # Store in session state
            st.session_state.deliverables_file = output_file
            st.session_state.deliverables_complete = True
            
            # Calculate summary for display
            pre_deliv = calc.calculate_deliverables_from_dataframe(starting_positions, prices)
            post_deliv = calc.calculate_deliverables_from_dataframe(final_positions, prices)
            
            st.session_state.deliverables_data = {
                'pre_trade': pre_deliv,
                'post_trade': post_deliv,
                'prices': prices
            }
            
            st.success(f"✅ Deliverables calculated and saved to {output_file}")
            
    except Exception as e:
        st.error(f"❌ Error calculating deliverables: {str(e)}")
        logger.error(traceback.format_exc())

def run_pms_reconciliation(pms_file):
    """Run PMS reconciliation"""
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        with st.spinner("Running PMS reconciliation..."):
            # Save PMS file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(pms_file.name).suffix) as tmp:
                tmp.write(pms_file.getbuffer())
                pms_path = tmp.name
            
            # Get positions
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            # Run reconciliation
            recon = EnhancedReconciliation()
            pms_df = recon.read_pms_file(pms_path)
            
            # Generate report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"PMS_RECONCILIATION_{timestamp}.xlsx"
            
            recon.create_comprehensive_recon_report(
                starting_positions,
                final_positions,
                pms_df,
                output_file
            )
            
            # Store results
            st.session_state.recon_file = output_file
            st.session_state.recon_complete = True
            
            # Store detailed results for display
            pre_recon = recon.reconcile_positions(starting_positions, pms_df, "Pre-Trade")
            post_recon = recon.reconcile_positions(final_positions, pms_df, "Post-Trade")
            
            st.session_state.recon_data = {
                'pre_trade': pre_recon,
                'post_trade': post_recon,
                'pms_df': pms_df
            }
            
            st.success(f"✅ Reconciliation complete and saved to {output_file}")
            
            # Clean up temp file
            try:
                os.unlink(pms_path)
            except:
                pass
                
    except Exception as e:
        st.error(f"❌ Error in reconciliation: {str(e)}")
        logger.error(traceback.format_exc())

def display_deliverables_tab():
    """Display deliverables and IV analysis"""
    st.header("💰 Deliverables & Intrinsic Value Analysis")
    
    if not st.session_state.deliverables_complete:
        st.info("Run the pipeline with deliverables enabled to see this analysis")
        return
    
    data = st.session_state.deliverables_data
    
    # Summary metrics
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
    
    # Detailed tables
    tab1, tab2, tab3 = st.tabs(["Pre-Trade Deliverables", "Post-Trade Deliverables", "Comparison"])
    
    with tab1:
        if not pre_deliv.empty:
            st.dataframe(pre_deliv, use_container_width=True, hide_index=True)
    
    with tab2:
        if not post_deliv.empty:
            st.dataframe(post_deliv, use_container_width=True, hide_index=True)
    
    with tab3:
        if not pre_deliv.empty and not post_deliv.empty:
            # Create comparison
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

def display_reconciliation_tab():
    """Display PMS reconciliation results"""
    st.header("🔄 PMS Position Reconciliation")
    
    if not st.session_state.recon_complete:
        st.info("Run the pipeline with PMS reconciliation enabled to see this analysis")
        return
    
    data = st.session_state.recon_data
    pre_recon = data['pre_trade']
    post_recon = data['post_trade']
    
    # Summary metrics
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
    
    # Detailed discrepancies
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

def display_enhanced_downloads():
    """Enhanced download section with all reports"""
    st.header("📥 Download All Reports")
    
    col1, col2, col3 = st.columns(3)
    
    # [KEEP ALL EXISTING DOWNLOAD BUTTONS]
    # ... existing download code ...
    
    # ADD NEW DOWNLOAD BUTTONS
    with col2:
        if st.session_state.get('deliverables_complete') and st.session_state.get('deliverables_file'):
            st.subheader("📊 Deliverables Report")
            try:
                with open(st.session_state.deliverables_file, 'rb') as f:
                    st.download_button(
                        "📥 Download Deliverables Report",
                        f.read(),
                        file_name=st.session_state.deliverables_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            except:
                st.error("Report file not found")
    
    with col3:
        if st.session_state.get('recon_complete') and st.session_state.get('recon_file'):
            st.subheader("🔄 Reconciliation Report")
            try:
                with open(st.session_state.recon_file, 'rb') as f:
                    st.download_button(
                        "📥 Download Reconciliation Report",
                        f.read(),
                        file_name=st.session_state.recon_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            except:
                st.error("Report file not found")

# [KEEP ALL YOUR EXISTING FUNCTIONS]
# process_stage1, process_stage2, etc. remain exactly the same

if __name__ == "__main__":
    main()
