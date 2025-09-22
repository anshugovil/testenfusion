"""
ACM Mapper Module
Transforms processed trades to ACM ListedTrades format
"""

import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ACMMapper:
    """Maps processed trades to ACM ListedTrades format"""
    
    def __init__(self, schema_file: str = None):
        """
        Initialize ACM Mapper
        
        Args:
            schema_file: Path to Excel schema file
        """
        self.schema_file = schema_file
        self.columns_order = []
        self.mandatory_columns = set()
        self.singapore_tz = ZoneInfo("Asia/Singapore")
        
        if schema_file and Path(schema_file).exists():
            self.load_schema(schema_file)
    
    def load_schema(self, schema_file: str) -> bool:
        """
        Load schema from Excel file
        
        Args:
            schema_file: Path to Excel file with 'Columns' sheet
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df = pd.read_excel(schema_file, sheet_name="Columns")
            df.columns = [c.strip() for c in df.columns]
            
            # Get column order
            self.columns_order = df["Column"].astype(str).tolist()
            
            # Get mandatory columns
            mandatory_mask = df["Mandatory"].astype(str).str.strip().str.lower() == "yes"
            self.mandatory_columns = set(df.loc[mandatory_mask, "Column"].astype(str).tolist())
            
            logger.info(f"Loaded schema with {len(self.columns_order)} columns, "
                       f"{len(self.mandatory_columns)} mandatory")
            return True
            
        except Exception as e:
            logger.error(f"Error loading schema: {e}")
            return False
    
    def map_transaction_type(self, bs: str, opposite: str) -> str:
        """
        Map B/S and Opposite? to Transaction Type
        
        Args:
            bs: Buy/Sell indicator
            opposite: Yes/No for opposite strategy
            
        Returns:
            Transaction type string
        """
        b = (bs or "").strip().lower()
        o = (opposite or "").strip().lower()
        truthy = {"yes", "y", "true", "1"}
        
        if b.startswith("b"):
            return "BuyToCover" if o in truthy else "Buy"
        elif b.startswith("s"):
            return "Sell" if o in truthy else "SellShort"
        return ""
    
    def process_mapping(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process mapping from input to ACM format
        
        Args:
            input_df: Processed trades dataframe
            
        Returns:
            Mapped dataframe in ACM format
        """
        # Ensure schema is loaded
        if not self.columns_order:
            logger.error("Schema not loaded")
            return pd.DataFrame()
        
        # Strip column names in input
        input_df = input_df.copy()
        input_df.columns = [str(c).strip() for c in input_df.columns]
        
        # Initialize output dataframe with schema columns
        n = len(input_df)
        out = pd.DataFrame({col: [""] * n for col in self.columns_order})
        
        # Get current timestamps
        now_sg = datetime.now(self.singapore_tz)
        trade_date_str = now_sg.strftime("%m/%d/%Y %H:%M:%S")
        settle_date_str = now_sg.strftime("%m/%d/%Y")
        
        # ==================
        # FIELD MAPPINGS
        # ==================
        
        # Dates
        if "Trade Date" in out.columns:
            out["Trade Date"] = trade_date_str
        if "Settle Date" in out.columns:
            out["Settle Date"] = settle_date_str
        
        # Account and Counterparty
        if "Account Id" in out.columns and "Scheme" in input_df.columns:
            out["Account Id"] = input_df["Scheme"].astype(str)
        
        if "Counterparty Code" in out.columns and "CP Code" in input_df.columns:
            out["Counterparty Code"] = input_df["CP Code"].astype(str)
        
        # Identifier (from Bloomberg Ticker)
        if "Identifier" in out.columns and "Bloomberg_Ticker" in input_df.columns:
            out["Identifier"] = input_df["Bloomberg_Ticker"].astype(str)
        
        if "Identifier Type" in out.columns:
            out["Identifier Type"] = "Bloomberg Yellow Key"
        
        # Quantity (absolute value of Lots Traded)
        if "Quantity" in out.columns:
            # Try different column names for lots
            lots_col = None
            for col_name in ["Lots Traded", "Lots", 12]:  # Column 12 for unnamed
                if col_name in input_df.columns:
                    lots_col = col_name
                    break
            
            if lots_col is not None:
                out["Quantity"] = pd.to_numeric(input_df[lots_col], errors="coerce").abs()
        
        # Price fields
        price_col = None
        for col_name in ["Avg Price", "Price", 3]:  # Column 3 for unnamed
            if col_name in input_df.columns:
                price_col = col_name
                break
        
        if price_col:
            if "Trade Price" in out.columns:
                out["Trade Price"] = pd.to_numeric(input_df[price_col], errors="coerce")
            if "Price" in out.columns:
                out["Price"] = pd.to_numeric(input_df[price_col], errors="coerce")
        
        # Instrument Type
        if "Instrument Type" in out.columns:
            instr_col = None
            for col_name in ["Instr", 4]:  # Column 4 for unnamed
                if col_name in input_df.columns:
                    instr_col = col_name
                    break
            if instr_col:
                out["Instrument Type"] = input_df[instr_col].astype(str)
        
        # Strike Price
        if "Strike Price" in out.columns:
            strike_col = None
            for col_name in ["Strike Price", 8]:  # Column 8 for unnamed
                if col_name in input_df.columns:
                    strike_col = col_name
                    break
            if strike_col:
                out["Strike Price"] = pd.to_numeric(input_df[strike_col], errors="coerce")
        
        # Lot Size
        if "Lot Size" in out.columns:
            lot_size_col = None
            for col_name in ["Lot Size", 7]:  # Column 7 for unnamed
                if col_name in input_df.columns:
                    lot_size_col = col_name
                    break
            if lot_size_col:
                out["Lot Size"] = pd.to_numeric(input_df[lot_size_col], errors="coerce")
        
        # Strategy (from Stage 1 processing)
        if "Strategy" in out.columns and "Strategy" in input_df.columns:
            out["Strategy"] = input_df["Strategy"].astype(str)
        
        # Executing Broker
        if "Executing Broker Name" in out.columns:
            broker_col = None
            for col_name in ["TM Name", 1]:  # Column 1 for unnamed
                if col_name in input_df.columns:
                    broker_col = col_name
                    break
            if broker_col:
                out["Executing Broker Name"] = input_df[broker_col].astype(str)
        
        # Trade Venue (blank)
        if "Trade Venue" in out.columns:
            out["Trade Venue"] = ""
        
        # Notes
        if "Notes" in out.columns:
            notes_col = None
            for col_name in ["A/E", 2]:  # Column 2 for unnamed
                if col_name in input_df.columns:
                    notes_col = col_name
                    break
            if notes_col:
                out["Notes"] = input_df[notes_col].astype(str)
        
        # Transaction Type (based on B/S and Opposite?)
        if "Transaction Type" in out.columns:
            bs_col = None
            opposite_col = None
            
            # Find B/S column
            for col_name in ["B/S", 10]:  # Column 10 for unnamed
                if col_name in input_df.columns:
                    bs_col = col_name
                    break
            
            # Find Opposite? column
            for col_name in ["Opposite?", "Opposite"]:
                if col_name in input_df.columns:
                    opposite_col = col_name
                    break
            
            if bs_col and opposite_col:
                out["Transaction Type"] = [
                    self.map_transaction_type(bs, op)
                    for bs, op in zip(input_df[bs_col], input_df[opposite_col])
                ]
            elif bs_col:
                # If no Opposite column, just use B/S
                out["Transaction Type"] = [
                    self.map_transaction_type(bs, "No")
                    for bs in input_df[bs_col]
                ]
        
        # Clean up NaN values
        out = out.fillna("")
        
        logger.info(f"Mapped {len(out)} records to ACM format")
        return out
    
    def validate_output(self, output_df: pd.DataFrame) -> List[Dict]:
        """
        Validate the output dataframe
        
        Args:
            output_df: Mapped output dataframe
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for col in self.mandatory_columns:
            if col not in output_df.columns:
                errors.append({
                    "row": 0,
                    "column": col,
                    "reason": "mandatory column missing in output structure"
                })
                continue
            
            # Check for blank values
            mask = output_df[col].astype(str).str.strip()
            mask = (mask == "") | (mask.str.lower() == "nan")
            
            for idx in output_df.index[mask]:
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
        
        Args:
            processed_trades_df: Output from Stage 1 (trade processor)
            
        Returns:
            Tuple of (mapped_df, errors_df)
        """
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