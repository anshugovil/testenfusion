"""
Output Generator Module - STREAMLIT CLOUD VERSION
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import logging
from datetime import datetime
import tempfile
import os

logger = logging.getLogger(__name__)


class OutputGenerator:
    """Generates and saves all output files - Streamlit Cloud Compatible"""
    
    def __init__(self, output_dir: str = None):
        # Use temp directory for Streamlit Cloud
        if output_dir is None:
            temp_dir = tempfile.gettempdir()
            output_dir = os.path.join(temp_dir, "output")
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.missing_mappings = {'positions': [], 'trades': []}
        
    def save_all_outputs(self, 
                        parsed_trades_df: pd.DataFrame,
                        starting_positions_df: pd.DataFrame,
                        processed_trades_df: pd.DataFrame,
                        final_positions_df: pd.DataFrame,
                        file_prefix: str = "output",
                        input_parser=None,
                        trade_parser=None) -> Dict[str, Path]:
        """Save all output files with proper date formatting"""
        output_files = {}
        
        # Format dates in all dataframes
        parsed_trades_df = self._format_dates_in_dataframe(parsed_trades_df)
        starting_positions_df = self._format_dates_in_dataframe(starting_positions_df)
        processed_trades_df = self._format_dates_in_dataframe(processed_trades_df)
        final_positions_df = self._format_dates_in_dataframe(final_positions_df)
        
        # Save all output files
        try:
            # File 1: Parsed Trade File
            parsed_trades_file = self.output_dir / f"{file_prefix}_1_parsed_trades_{self.timestamp}.csv"
            parsed_trades_df.to_csv(str(parsed_trades_file), index=False, date_format='%Y-%m-%d')
            output_files['parsed_trades'] = parsed_trades_file
            
            # File 2: Starting Position File
            starting_pos_file = self.output_dir / f"{file_prefix}_2_starting_positions_{self.timestamp}.csv"
            starting_positions_df.to_csv(str(starting_pos_file), index=False, date_format='%Y-%m-%d')
            output_files['starting_positions'] = starting_pos_file
            
            # File 3: Processed Trade File
            processed_trades_file = self.output_dir / f"{file_prefix}_3_processed_trades_{self.timestamp}.csv"
            processed_trades_df.to_csv(str(processed_trades_file), index=False, date_format='%Y-%m-%d')
            output_files['processed_trades'] = processed_trades_file
            
            # Try Excel output
            try:
                processed_trades_excel = self.output_dir / f"{file_prefix}_3_processed_trades_{self.timestamp}.xlsx"
                with pd.ExcelWriter(str(processed_trades_excel), engine='openpyxl', date_format='YYYY-MM-DD') as writer:
                    processed_trades_df.to_excel(writer, sheet_name='Processed Trades', index=False)
                output_files['processed_trades_excel'] = processed_trades_excel
            except:
                pass
            
            # File 4: Final Position File
            final_pos_file = self.output_dir / f"{file_prefix}_4_final_positions_{self.timestamp}.csv"
            final_positions_df.to_csv(str(final_pos_file), index=False, date_format='%Y-%m-%d')
            output_files['final_positions'] = final_pos_file
            
            # File 5: Missing Mappings Report
            if input_parser or trade_parser:
                missing_mappings_file = self.create_missing_mappings_report(input_parser, trade_parser)
                if missing_mappings_file:
                    output_files['missing_mappings'] = missing_mappings_file
            
            # Create summary report
            summary_file = self._create_summary_report(
                parsed_trades_df, starting_positions_df, 
                processed_trades_df, final_positions_df,
                input_parser, trade_parser
            )
            output_files['summary'] = summary_file
            
        except Exception as e:
            logger.error(f"Error saving outputs: {e}")
            
        return output_files
    
    def _format_dates_in_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format date columns in DataFrame"""
        if df.empty:
            return df
        
        df = df.copy()
        date_columns = ['Expiry', 'expiry', 'Expiry_Date', 'expiry_date']
        
        for col in df.columns:
            if col in date_columns or 'expiry' in col.lower():
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                    except:
                        pass
        
        return df
    
    def create_missing_mappings_report(self, input_parser=None, trade_parser=None) -> Optional[Path]:
        """Create a report of unmapped symbols"""
        all_missing = []
        
        if input_parser and hasattr(input_parser, 'unmapped_symbols'):
            for item in input_parser.unmapped_symbols:
                expiry = item.get('expiry', '')
                if expiry and hasattr(expiry, 'strftime'):
                    expiry = expiry.strftime('%Y-%m-%d')
                
                all_missing.append({
                    'Source': 'Position File',
                    'Symbol': item.get('symbol', ''),
                    'Expiry': expiry,
                    'Quantity': item.get('position_lots', 0),
                    'Suggested_Ticker': self._suggest_ticker(item.get('symbol', '')),
                    'Underlying': '',
                    'Exchange': '',
                    'Lot_Size': ''
                })
        
        if trade_parser and hasattr(trade_parser, 'unmapped_symbols'):
            for item in trade_parser.unmapped_symbols:
                expiry = item.get('expiry', '')
                if expiry and hasattr(expiry, 'strftime'):
                    expiry = expiry.strftime('%Y-%m-%d')
                    
                all_missing.append({
                    'Source': 'Trade File',
                    'Symbol': item.get('symbol', ''),
                    'Expiry': expiry,
                    'Quantity': item.get('position_lots', 0),
                    'Suggested_Ticker': self._suggest_ticker(item.get('symbol', '')),
                    'Underlying': '',
                    'Exchange': '',
                    'Lot_Size': ''
                })
        
        if not all_missing:
            return None
        
        df = pd.DataFrame(all_missing)
        unique_symbols = df.groupby('Symbol').agg({
            'Source': lambda x: ', '.join(sorted(set(x))),
            'Expiry': 'first',
            'Quantity': 'sum',
            'Suggested_Ticker': 'first',
            'Underlying': 'first',
            'Exchange': 'first',
            'Lot_Size': 'first'
        }).reset_index()
        
        unique_symbols = unique_symbols.sort_values('Symbol')
        
        missing_file = self.output_dir / f"MISSING_MAPPINGS_{self.timestamp}.csv"
        unique_symbols.to_csv(str(missing_file), index=False, date_format='%Y-%m-%d')
        
        template_file = self.output_dir / f"MAPPING_TEMPLATE_{self.timestamp}.csv"
        template_df = unique_symbols[['Symbol', 'Suggested_Ticker', 'Underlying', 'Exchange', 'Lot_Size']]
        template_df.columns = ['Symbol', 'Ticker', 'Underlying', 'Exchange', 'Lot_Size']
        template_df.to_csv(str(template_file), index=False)
        
        return missing_file
    
    def _suggest_ticker(self, symbol: str) -> str:
        """Suggest a ticker based on common patterns"""
        symbol_upper = symbol.upper()
        cleaned = symbol_upper
        
        for suffix in ['EQ', 'FUT', 'OPT', 'CE', 'PE', '-EQ', '-FUT']:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)]
                break
        
        index_map = {
            'NIFTY': 'NZ',
            'BANKNIFTY': 'AF1',
            'FINNIFTY': 'FINNIFTY',
            'MIDCPNIFTY': 'RNS'
        }
        
        for key, value in index_map.items():
            if key in symbol_upper:
                return value
        
        return cleaned.strip('-').strip()
    
    def _create_summary_report(self, parsed_trades_df, starting_positions_df,
                              processed_trades_df, final_positions_df,
                              input_parser=None, trade_parser=None) -> Path:
        """Create summary report"""
        summary_file = self.output_dir / f"summary_report_{self.timestamp}.txt"
        
        try:
            with open(str(summary_file), 'w') as f:
                f.write("=" * 60 + "\n")
                f.write("TRADE PROCESSING SUMMARY REPORT\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                
                # Missing mappings section
                missing_count = 0
                if input_parser and hasattr(input_parser, 'unmapped_symbols'):
                    missing_count += len(input_parser.unmapped_symbols)
                if trade_parser and hasattr(trade_parser, 'unmapped_symbols'):
                    missing_count += len(trade_parser.unmapped_symbols)
                
                if missing_count > 0:
                    f.write("⚠️  MISSING MAPPINGS:\n")
                    f.write("-" * 30 + "\n")
                    
                    if input_parser and hasattr(input_parser, 'unmapped_symbols') and input_parser.unmapped_symbols:
                        f.write(f"Position file: {len(input_parser.unmapped_symbols)} unmapped symbols\n")
                    
                    if trade_parser and hasattr(trade_parser, 'unmapped_symbols') and trade_parser.unmapped_symbols:
                        f.write(f"Trade file: {len(trade_parser.unmapped_symbols)} unmapped symbols\n")
                    
                    f.write("\n📋 Check MISSING_MAPPINGS_*.csv for complete list\n\n")
                
                # Position summaries
                f.write("STARTING POSITIONS:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Total positions: {len(starting_positions_df)}\n\n")
                
                f.write("TRADES PROCESSED:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Total trades: {len(parsed_trades_df)}\n")
                f.write(f"Trades after processing: {len(processed_trades_df)}\n\n")
                
                f.write("FINAL POSITIONS:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Total positions: {len(final_positions_df)}\n\n")
                
                f.write("=" * 60 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 60 + "\n")
        except Exception as e:
            logger.error(f"Error creating summary: {e}")
        
        return summary_file
    
    def create_trade_dataframe_from_positions(self, positions: List) -> pd.DataFrame:
        """Convert Position objects to DataFrame"""
        trades_data = []
        
        for pos in positions:
            expiry_str = pos.expiry_date.strftime('%Y-%m-%d')
            
            trade_dict = {
                'Symbol': pos.symbol,
                'Bloomberg_Ticker': pos.bloomberg_ticker,
                'Expiry': expiry_str,
                'Strike': pos.strike_price,
                'Security_Type': pos.security_type,
                'Lots': pos.position_lots,
                'Lot_Size': pos.lot_size,
                'Quantity': pos.position_lots * pos.lot_size
            }
            trades_data.append(trade_dict)
        
        return pd.DataFrame(trades_data)
