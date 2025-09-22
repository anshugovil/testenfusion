"""
ACM Mapper Module - HARDCODED VERSION
No external schema file needed - transformation is built-in
"""

import pandas as pd
import numpy as np
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    ZoneInfo = lambda x: pytz.timezone(x)
    
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ACMMapper:
    """Maps processed trades to ACM ListedTrades format - HARDCODED VERSION"""
    
    def __init__(self, schema_file: str = None):
        """
        Initialize ACM Mapper with hardcoded schema
        
        Args:
            schema_file: Optional - not needed with hardcoded schema
        """
        # HARDCODED ACM SCHEMA
        self.columns_order = [
            "Trade Date",
            "Settle Date", 
            "Account Id",
            "Counterparty Code",
            "Identifier",
            "Identifier Type",
            "Quantity",
            "Trade Price",
            "Price",
            "Instrument Type",
            "Strike Price",
            "Lot Size",
            "Strategy",
            "Executing Broker Name",
            "Trade Venue",
            "Notes",
            "Transaction Type"
        ]
        
        # HARDCODED MANDATORY FIELDS
        self.mandatory_columns = {
            "Account Id",
            "Identifier",
            "Quantity",
            "Transaction Type"
        }
        
        try:
            self.singapore_tz = ZoneInfo("Asia/Singapore")
        except:
            import pytz
            self.singapore_tz = pytz.timezone("Asia/Singapore")
        
        logger.info("Using hardcoded ACM schema - no external file needed")
    
    def load_schema(self, schema_file: str) -> bool:
        """
        Kept for compatibility but not needed with hardcoded schema
        """
        logger.info("Schema already hardcoded - ignoring external file")
        return True
    
    def map_transaction_type(self, bs: str, opposite: str) -> str:
        """
        Map B/S and Opposite? to Transaction Type
        
        Based on these rules:
        - Buy + Opposite=Yes → BuyToCover
        - Buy + Opposite=No → Buy  
        - Sell + Opposite=Yes → Sell
        - Sell + Opposite=No → SellShort
        """
        b = str(bs).strip().lower() if pd.notna(bs) else ""
        o = str(opposite).strip().lower() if pd.notna(opposite) else ""
        truthy = {"yes", "y", "true", "1"}
        
        if b.startswith("b"):
            return "BuyToCover" if o in truthy else "Buy"
        elif b.startswith("s"):
            return "Sell" if o in truthy else "SellShort"
        return ""
    
    def process_mapping(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process mapping from Stage 1 output to ACM format
        HARDCODED FIELD MAPPINGS - No configuration needed
        
        Expected Stage 1 columns (from your trade processor):
        - Column 0: Scheme (TM Code)
        - Column 1: TM Name
        - Column 2: A/E
        - Column 3: Avg Price
        - Column 4: Instr (OPTSTK/OPTIDX/FUTSTK/FUTIDX)
        - Column 5: Symbol
        - Column 6: Expiry Dt
        - Column 7: Lot Size
        - Column 8: Strike Price
        - Column 9: Option Type
        - Column 10: B/S
        - Column 11: Qty
        - Column 12: Lots Traded
        - Column 13: CP Code
        - Strategy: Added by Stage 1
        - Opposite?: Added by Stage 1
        - Bloomberg_Ticker: Added by Stage 1
        """
        
        # Make a copy and handle both named and unnamed columns
        input_df = input_df.copy()
        
        # Initialize output with all ACM columns
        n = len(input_df)
        out = pd.DataFrame({col: [""] * n for col in self.columns_order})
        
        # Get current timestamps for Singapore timezone
        now_sg = datetime.now(self.singapore_tz)
        trade_date_str = now_sg.strftime("%m/%d/%Y %H:%M:%S")
        settle_date_str = now_sg.strftime("%m/%d/%Y")
        
        # ==================
        # HARDCODED MAPPINGS
        # ==================
        
        # 1. DATES - Always use current Singapore time
        out["Trade Date"] = trade_date_str
        out["Settle Date"] = settle_date_str
        
        # 2. ACCOUNT ID ← Scheme (Column 0)
        if 0 in input_df.columns:
            out["Account Id"] = input_df[0].astype(str)
        elif "Scheme" in input_df.columns:
            out["Account Id"] = input_df["Scheme"].astype(str)
        
        # 3. COUNTERPARTY CODE ← CP Code (Column 13)
        if 13 in input_df.columns:
            out["Counterparty Code"] = input_df[13].astype(str)
        elif "CP Code" in input_df.columns:
            out["Counterparty Code"] = input_df["CP Code"].astype(str)
        
        # 4. IDENTIFIER ← Bloomberg_Ticker (added by Stage 1)
        if "Bloomberg_Ticker" in input_df.columns:
            out["Identifier"] = input_df["Bloomberg_Ticker"].astype(str)
        
        # 5. IDENTIFIER TYPE - Always Bloomberg
        out["Identifier Type"] = "Bloomberg Yellow Key"
        
        # 6. QUANTITY ← Absolute value of Lots Traded (Column 12)
        if 12 in input_df.columns:
            out["Quantity"] = pd.to_numeric(input_df[12], errors="coerce").abs()
        elif "Lots Traded" in input_df.columns:
            out["Quantity"] = pd.to_numeric(input_df["Lots Traded"], errors="coerce").abs()
        
        # 7. TRADE PRICE & PRICE ← Avg Price (Column 3)
        if 3 in input_df.columns:
            price_val = pd.to_numeric(input_df[3], errors="coerce")
            out["Trade Price"] = price_val
            out["Price"] = price_val
        elif "Avg Price" in input_df.columns:
            price_val = pd.to_numeric(input_df["Avg Price"], errors="coerce")
            out["Trade Price"] = price_val
            out["Price"] = price_val
        
        # 8. INSTRUMENT TYPE ← Instr (Column 4)
        if 4 in input_df.columns:
            out["Instrument Type"] = input_df[4].astype(str)
        elif "Instr" in input_df.columns:
            out["Instrument Type"] = input_df["Instr"].astype(str)
        
        # 9. STRIKE PRICE ← Strike Price (Column 8)
        if 8 in input_df.columns:
            out["Strike Price"] = pd.to_numeric(input_df[8], errors="coerce")
        elif "Strike Price" in input_df.columns:
            out["Strike Price"] = pd.to_numeric(input_df["Strike Price"], errors="coerce")
        
        # 10. LOT SIZE ← Lot Size (Column 7)
        if 7 in input_df.columns:
            out["Lot Size"] = pd.to_numeric(input_df[7], errors="coerce")
        elif "Lot Size" in input_df.columns:
            out["Lot Size"] = pd.to_numeric(input_df["Lot Size"], errors="coerce")
        
        # 11. STRATEGY ← Strategy (added by Stage 1)
        if "Strategy" in input_df.columns:
            out["Strategy"] = input_df["Strategy"].astype(str)
        
        # 12. EXECUTING BROKER NAME ← TM Name (Column 1)
        if 1 in input_df.columns:
            out["Executing Broker Name"] = input_df[1].astype(str)
        elif "TM Name" in input_df.columns:
            out["Executing Broker Name"] = input_df["TM Name"].astype(str)
        
        # 13. TRADE VENUE - Always blank
        out["Trade Venue"] = ""
        
        # 14. NOTES ← A/E (Column 2)
        if 2 in input_df.columns:
            out["Notes"] = input_df[2].astype(str)
        elif "A/E" in input_df.columns:
            out["Notes"] = input_df["A/E"].astype(str)
        
        # 15. TRANSACTION TYPE ← Based on B/S (Column 10) and Opposite?
        bs_col = None
        if 10 in input_df.columns:
            bs_col = 10
        elif "B/S" in input_df.columns:
            bs_col = "B/S"
        
        opposite_col = "Opposite?" if "Opposite?" in input_df.columns else None
        
        if bs_col is not None:
            if opposite_col:
                # Use both B/S and Opposite flag
                out["Transaction Type"] = [
                    self.map_transaction_type(bs, op)
                    for bs, op in zip(input_df[bs_col], input_df[opposite_col])
                ]
            else:
                # No Opposite column - assume all are normal (not opposite)
                out["Transaction Type"] = [
                    self.map_transaction_type(bs, "No")
                    for bs in input_df[bs_col]
                ]
        
        # Clean up NaN values
        out = out.fillna("")
        
        # Replace 'nan' strings with empty
        for col in out.columns:
            out[col] = out[col].replace('nan', '')
        
        logger.info(f"Mapped {len(out)} records to ACM format using hardcoded schema")
        return out
    
    def validate_output(self, output_df: pd.DataFrame) -> List[Dict]:
        """
        Validate the output dataframe for mandatory fields
        
        Mandatory fields (hardcoded):
        - Account Id
        - Identifier
        - Quantity  
        - Transaction Type
        """
        errors = []
        
        for col in self.mandatory_columns:
            if col not in output_df.columns:
                errors.append({
                    "row": 0,
                    "column": col,
                    "reason": "mandatory column missing"
                })
                continue
            
            # Check for blank values
            col_values = output_df[col].astype(str).str.strip()
            blank_mask = (col_values == "") | (col_values.str.lower() == "nan") | (col_values.str.lower() == "none")
            
            for idx in output_df.index[blank_mask]:
                errors.append({
                    "row": int(idx) + 1,
                    "column": col,
                    "reason": "mandatory field is blank"
                })
        
        logger.info(f"Validation found {len(errors)} errors")
        return errors
    
    def process_trades_to_acm(self, processed_trades_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main method to process trades to ACM format
        No schema file needed - all mappings are hardcoded
        
        Args:
            processed_trades_df: Output from Stage 1 (trade processor)
            
        Returns:
            Tuple of (mapped_df, errors_df)
        """
        logger.info("Processing trades to ACM format with hardcoded mappings")
        
        # Map the data
        mapped_df = self.process_mapping(processed_trades_df)
        
        # Validate
        errors = self.validate_output(mapped_df)
        
        # Create errors dataframe
        if errors:
            errors_df = pd.DataFrame(errors)
        else:
            errors_df = pd.DataFrame(columns=["row", "column", "reason"])
        
        return mapped_df, errors_df
