"""
Position Manager Module - COMPLETE VERSION
Tracks all position details and maintains consistency between starting and final positions
Ready for Yahoo Finance price integration
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
import pandas as pd
import logging
from datetime import datetime
from copy import deepcopy

logger = logging.getLogger(__name__)

@dataclass
class PositionDetails:
    """Complete position information with all attributes"""
    ticker: str
    symbol: str
    security_type: str
    expiry: datetime
    strike: float
    lots: float  # Signed quantity in lots
    lot_size: int
    qty: float  # lots * lot_size
    strategy: str
    direction: str  # Long/Short
    # Additional fields for price integration
    last_price: float = 0.0
    market_value: float = 0.0
    underlying_ticker: str = ""
    
    def update_qty(self):
        """Recalculate QTY from lots and lot_size"""
        self.qty = self.lots * self.lot_size
        self.direction = "Long" if self.lots > 0 else "Short" if self.lots < 0 else "Flat"
    
    def __repr__(self):
        return f"Position({self.ticker}, {self.lots} lots @ {self.lot_size}, {self.strategy})"


class PositionManager:
    """Manages positions with complete tracking of all attributes"""
    
    def __init__(self):
        self.positions: Dict[str, PositionDetails] = {}
        self.initial_positions_df = None
        # Store ticker to position details mapping for reference
        self.ticker_details_map = {}
        # Store trade history for new positions
        self.trade_details_cache = {}
    
    def initialize_from_positions(self, initial_positions: List) -> pd.DataFrame:
        """
        Initialize position manager with existing positions from input parser
        
        Args:
            initial_positions: List of Position objects from input parser
            
        Returns:
            DataFrame of starting positions with all columns
        """
        self.positions.clear()
        self.ticker_details_map.clear()
        positions_data = []
        
        for pos in initial_positions:
            # Determine initial strategy based on position direction and security type
            if pos.security_type == 'Put':
                # Puts are inverted
                strategy = 'FUSH' if pos.position_lots > 0 else 'FULO'
            else:  # Futures or Calls
                strategy = 'FULO' if pos.position_lots > 0 else 'FUSH'
            
            # Calculate QTY
            qty = pos.position_lots * pos.lot_size
            
            # Create Position object with all details
            position_details = PositionDetails(
                ticker=pos.bloomberg_ticker,
                symbol=pos.symbol,
                security_type=pos.security_type,
                expiry=pos.expiry_date,
                strike=pos.strike_price if pos.security_type != 'Futures' else 0,
                lots=pos.position_lots,
                lot_size=pos.lot_size,
                qty=qty,
                strategy=strategy,
                direction='Long' if pos.position_lots > 0 else 'Short',
                underlying_ticker=pos.underlying_ticker
            )
            
            # Store in positions dict
            self.positions[pos.bloomberg_ticker] = position_details
            
            # Store ticker details for reference
            self.ticker_details_map[pos.bloomberg_ticker] = {
                'symbol': pos.symbol,
                'security_type': pos.security_type,
                'expiry': pos.expiry_date,
                'strike': pos.strike_price,
                'lot_size': pos.lot_size,
                'underlying': pos.underlying_ticker
            }
            
            # Add to DataFrame data
            positions_data.append({
                'Ticker': pos.bloomberg_ticker,
                'Symbol': pos.symbol,
                'Security_Type': pos.security_type,
                'Expiry': pos.expiry_date,
                'Strike': pos.strike_price if pos.security_type != 'Futures' else 0,
                'Lots': pos.position_lots,
                'Lot_Size': pos.lot_size,
                'QTY': qty,
                'Strategy': strategy,
                'Direction': 'Long' if pos.position_lots > 0 else 'Short',
                'Underlying': pos.underlying_ticker,
                'Price': 0,  # Placeholder for Yahoo Finance
                'Market_Value': 0  # Placeholder for calculation
            })
            
            logger.info(f"Initialized position: {pos.bloomberg_ticker} with {pos.position_lots} lots @ {pos.lot_size} per lot, strategy={strategy}")
        
        # Create and store initial positions DataFrame
        self.initial_positions_df = pd.DataFrame(positions_data)
        return self.initial_positions_df
    
    def update_position(self, ticker: str, quantity_change: float, 
                       security_type: str, strategy: str,
                       trade_object=None):
        """
        Update position with a trade
        
        Args:
            ticker: Bloomberg ticker
            quantity_change: Signed quantity to add (positive = buy, negative = sell) IN LOTS
            security_type: Type of security
            strategy: Strategy to assign
            trade_object: Original trade object with all details
        """
        if ticker not in self.positions:
            # NEW POSITION - need to get details from trade
            if trade_object:
                # Cache trade details for new positions
                self.trade_details_cache[ticker] = {
                    'symbol': trade_object.symbol,
                    'security_type': trade_object.security_type,
                    'expiry': trade_object.expiry_date,
                    'strike': trade_object.strike_price,
                    'lot_size': trade_object.lot_size,
                    'underlying': trade_object.underlying_ticker
                }
                
                position_details = PositionDetails(
                    ticker=ticker,
                    symbol=trade_object.symbol,
                    security_type=security_type,
                    expiry=trade_object.expiry_date,
                    strike=trade_object.strike_price if security_type != 'Futures' else 0,
                    lots=quantity_change,
                    lot_size=trade_object.lot_size,
                    qty=quantity_change * trade_object.lot_size,
                    strategy=strategy,
                    direction='Long' if quantity_change > 0 else 'Short',
                    underlying_ticker=trade_object.underlying_ticker
                )
            else:
                # Fallback if no trade object (shouldn't happen)
                logger.warning(f"Creating position {ticker} without full trade details")
                position_details = PositionDetails(
                    ticker=ticker,
                    symbol=ticker.split(' ')[0],
                    security_type=security_type,
                    expiry=datetime.now(),
                    strike=0,
                    lots=quantity_change,
                    lot_size=100,  # Default
                    qty=quantity_change * 100,
                    strategy=strategy,
                    direction='Long' if quantity_change > 0 else 'Short',
                    underlying_ticker=""
                )
            
            self.positions[ticker] = position_details
            logger.info(f"Created new position: {position_details}")
        else:
            # UPDATE EXISTING POSITION
            old_position = self.positions[ticker]
            old_lots = old_position.lots
            new_lots = old_lots + quantity_change
            
            if abs(new_lots) < 0.0001:  # Effectively zero
                # Position closed
                del self.positions[ticker]
                logger.info(f"Closed position for {ticker}")
            else:
                # Update position - keep all original attributes except quantity and strategy
                old_position.lots = new_lots
                old_position.strategy = strategy
                old_position.update_qty()  # Recalculate QTY
                
                logger.info(f"Updated {ticker}: {old_lots} -> {new_lots} lots, strategy={strategy}")
    
    def get_position(self, ticker: str) -> Optional[PositionDetails]:
        """Get current position for a ticker"""
        return self.positions.get(ticker)
    
    def is_trade_opposing(self, ticker: str, trade_quantity: float, security_type: str) -> bool:
        """Check if a trade opposes the current position"""
        position = self.get_position(ticker)
        if position is None:
            return False
        
        # Check if signs are different (opposing)
        return (position.lots > 0 and trade_quantity < 0) or \
               (position.lots < 0 and trade_quantity > 0)
    
    def get_final_positions(self) -> pd.DataFrame:
        """
        Get final positions as a DataFrame with same structure as starting positions
        
        Returns:
            DataFrame with all position details matching starting positions structure
        """
        if not self.positions:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'Ticker', 'Symbol', 'Security_Type', 'Expiry', 'Strike',
                'Lots', 'Lot_Size', 'QTY', 'Strategy', 'Direction',
                'Underlying', 'Price', 'Market_Value'
            ])
        
        positions_data = []
        
        for ticker, position in self.positions.items():
            # Get original details if available
            original_details = self.ticker_details_map.get(ticker, {})
            
            # If this was a new position from trades, get cached details
            if not original_details and ticker in self.trade_details_cache:
                original_details = self.trade_details_cache[ticker]
            
            positions_data.append({
                'Ticker': ticker,
                'Symbol': position.symbol,
                'Security_Type': position.security_type,
                'Expiry': position.expiry,
                'Strike': position.strike,
                'Lots': position.lots,
                'Lot_Size': position.lot_size,
                'QTY': position.qty,  # This is lots * lot_size
                'Strategy': position.strategy,
                'Direction': position.direction,
                'Underlying': position.underlying_ticker,
                'Price': 0,  # Placeholder for Yahoo Finance
                'Market_Value': 0  # Placeholder for calculation
            })
        
        final_df = pd.DataFrame(positions_data)
        
        # Sort by ticker for consistency
        final_df = final_df.sort_values('Ticker').reset_index(drop=True)
        
        # Log summary
        logger.info(f"Final positions: {len(final_df)} positions")
        if len(final_df) > 0:
            logger.info(f"  Long positions: {len(final_df[final_df['Direction'] == 'Long'])}")
            logger.info(f"  Short positions: {len(final_df[final_df['Direction'] == 'Short'])}")
        
        return final_df
    
    def add_yahoo_prices(self, df: pd.DataFrame, price_fetcher=None) -> pd.DataFrame:
        """
        Add Yahoo Finance prices to positions DataFrame
        
        Args:
            df: Positions DataFrame
            price_fetcher: Function/object to fetch prices from Yahoo
            
        Returns:
            DataFrame with prices and market values added
        """
        if price_fetcher is None:
            logger.warning("No price fetcher provided, skipping price updates")
            return df
        
        df = df.copy()
        
        for idx, row in df.iterrows():
            try:
                symbol = row['Symbol']
                security_type = row['Security_Type']
                
                # Get price based on security type
                if security_type == 'Futures':
                    # For futures, might need special handling
                    price = price_fetcher.get_futures_price(symbol, row['Expiry'])
                elif security_type in ['Call', 'Put']:
                    # For options, need underlying price and option pricing
                    price = price_fetcher.get_option_price(
                        symbol, row['Expiry'], row['Strike'], security_type
                    )
                else:
                    # For stocks/ETFs
                    price = price_fetcher.get_stock_price(symbol)
                
                df.at[idx, 'Price'] = price
                df.at[idx, 'Market_Value'] = price * row['QTY']
                
            except Exception as e:
                logger.warning(f"Could not fetch price for {row['Symbol']}: {e}")
                df.at[idx, 'Price'] = 0
                df.at[idx, 'Market_Value'] = 0
        
        return df
    
    def get_position_summary(self) -> Dict:
        """Get summary statistics of current positions"""
        if not self.positions:
            return {
                'total_positions': 0,
                'long_positions': 0,
                'short_positions': 0,
                'flat_positions': 0,
                'by_security_type': {},
                'by_strategy': {}
            }
        
        long_count = sum(1 for p in self.positions.values() if p.lots > 0)
        short_count = sum(1 for p in self.positions.values() if p.lots < 0)
        flat_count = sum(1 for p in self.positions.values() if abs(p.lots) < 0.0001)
        
        # Count by security type
        by_type = {}
        for p in self.positions.values():
            by_type[p.security_type] = by_type.get(p.security_type, 0) + 1
        
        # Count by strategy
        by_strategy = {}
        for p in self.positions.values():
            by_strategy[p.strategy] = by_strategy.get(p.strategy, 0) + 1
        
        return {
            'total_positions': len(self.positions),
            'long_positions': long_count,
            'short_positions': short_count,
            'flat_positions': flat_count,
            'by_security_type': by_type,
            'by_strategy': by_strategy
        }
    
    def clear_all_positions(self):
        """Clear all positions (for reset)"""
        self.positions.clear()
        self.ticker_details_map.clear()
        self.trade_details_cache.clear()
        logger.info("Cleared all positions")
