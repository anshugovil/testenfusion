"""
Position Manager Module - WITH YAHOO PRICE FETCHING
Fetches underlying security prices based on Symbol column for ITM/OTM analysis
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
import pandas as pd
import logging
from datetime import datetime
from copy import deepcopy

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logging.warning("yfinance not installed. Install with: pip install yfinance")

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetch prices from Yahoo Finance"""
    
    def __init__(self):
        self.price_cache = {}
    
    def fetch_price_for_symbol(self, symbol: str) -> float:
        """Fetch current price for a single symbol"""
        # Check cache first
        if symbol in self.price_cache:
            return self.price_cache[symbol]
        
        if not YFINANCE_AVAILABLE:
            # Return dummy price for testing
            return self._get_dummy_price(symbol)
        
        price_found = False
        price = 0.0
        
        # Special handling for Indian indices
        if symbol.upper() == 'NIFTY':
            yahoo_symbols = ['^NSEI']  # NIFTY 50 index
        elif symbol.upper() == 'BANKNIFTY':
            yahoo_symbols = ['^NSEBANK']  # Bank Nifty index
        elif symbol.upper() == 'FINNIFTY':
            yahoo_symbols = ['^CNXFIN']  # Nifty Financial Services
        elif symbol.upper() == 'MIDCPNIFTY':
            yahoo_symbols = ['^NSEMDCP50']  # Nifty Midcap 50
        else:
            # Regular stocks - try different Yahoo formats
            yahoo_symbols = [
                f"{symbol}.NS",  # NSE
                f"{symbol}.BO",  # BSE  
                symbol           # Direct ticker
            ]
        
        # Try history() method first
        for yahoo_symbol in yahoo_symbols:
            if price_found:
                break
                
            try:
                ticker_obj = yf.Ticker(yahoo_symbol)
                hist = ticker_obj.history(period="1d")
                
                if not hist.empty and 'Close' in hist:
                    price = float(hist['Close'].iloc[-1])
                    if price and price > 0:
                        price_found = True
                        logger.info(f"Found price for {symbol}: {price:.2f} using {yahoo_symbol}")
                        break
            except Exception as e:
                continue
        
        # Try info() method as fallback
        if not price_found:
            for yahoo_symbol in yahoo_symbols:
                try:
                    ticker_obj = yf.Ticker(yahoo_symbol)
                    info = ticker_obj.info
                    
                    if 'currentPrice' in info and info['currentPrice']:
                        price = float(info['currentPrice'])
                    elif 'regularMarketPrice' in info and info['regularMarketPrice']:
                        price = float(info['regularMarketPrice'])
                    elif 'previousClose' in info and info['previousClose']:
                        price = float(info['previousClose'])
                    
                    if price and price > 0:
                        price_found = True
                        logger.info(f"Found price for {symbol}: {price:.2f} from info using {yahoo_symbol}")
                        break
                except:
                    continue
        
        if not price_found:
            logger.warning(f"Could not fetch price for {symbol}, using dummy price")
            price = self._get_dummy_price(symbol)
        
        # Cache the price
        self.price_cache[symbol] = price
        return price
    
    def _get_dummy_price(self, symbol: str) -> float:
        """Provides realistic dummy prices for testing when Yahoo Finance unavailable"""
        dummy_prices = {
            'NIFTY': 22500, 'BANKNIFTY': 48000, 'FINNIFTY': 21500,
            'MIDCPNIFTY': 11000, 'RELIANCE': 2900, 'TCS': 4000,
            'INFY': 1600, 'HDFCBANK': 1500, 'ICICIBANK': 1100,
            'SBIN': 800, 'ITC': 430, 'LT': 3600, 'AXISBANK': 1150,
            'HDFC': 2800, 'WIPRO': 450, 'TATAMOTORS': 1050,
            'BHARTIARTL': 1200, 'ASIANPAINT': 3200, 'MARUTI': 10500,
            'TITAN': 3400, 'BAJFINANCE': 6800, 'NESTLEIND': 2400,
            'HINDUNILVR': 2500, 'KOTAKBANK': 1700, 'ADANIPORTS': 1200,
        }
        price = dummy_prices.get(symbol.upper())
        if price:
            return float(price)
        # Generate consistent dummy price based on symbol hash
        return float(500 + (hash(symbol) % 3500))


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
    underlying_ticker: str = ""
    
    def update_qty(self):
        """Recalculate QTY from lots and lot_size"""
        self.qty = self.lots * self.lot_size
        self.direction = "Long" if self.lots > 0 else "Short" if self.lots < 0 else "Flat"
    
    def __repr__(self):
        return f"Position({self.ticker}, {self.lots} lots @ {self.lot_size}, {self.strategy})"


class PositionManager:
    """Manages positions with complete tracking and Yahoo price fetching"""
    
    def __init__(self):
        self.positions: Dict[str, PositionDetails] = {}
        self.initial_positions_df = None
        self.ticker_details_map = {}
        self.trade_details_cache = {}
        self.price_fetcher = PriceFetcher()
    
    def initialize_from_positions(self, initial_positions: List) -> pd.DataFrame:
        """
        Initialize position manager with existing positions
        Returns DataFrame with Yahoo prices
        """
        self.positions.clear()
        self.ticker_details_map.clear()
        positions_data = []
        
        for pos in initial_positions:
            # Determine initial strategy
            if pos.security_type == 'Put':
                strategy = 'FUSH' if pos.position_lots > 0 else 'FULO'
            else:
                strategy = 'FULO' if pos.position_lots > 0 else 'FUSH'
            
            # Calculate QTY
            qty = pos.position_lots * pos.lot_size
            
            # Create Position object
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
            
            # Store ticker details
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
                'Underlying': pos.underlying_ticker
            })
            
            logger.info(f"Initialized: {pos.bloomberg_ticker} with {pos.position_lots} lots @ {pos.lot_size}/lot")
        
        # Create DataFrame
        self.initial_positions_df = pd.DataFrame(positions_data)
        
        # Add Yahoo prices
        self.initial_positions_df = self.add_yahoo_prices(self.initial_positions_df)
        
        return self.initial_positions_df
    
    def update_position(self, ticker: str, quantity_change: float, 
                       security_type: str, strategy: str,
                       trade_object=None):
        """Update position with a trade"""
        if ticker not in self.positions:
            # NEW POSITION
            if trade_object:
                # Cache trade details
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
                # Fallback
                logger.warning(f"Creating position {ticker} without full trade details")
                position_details = PositionDetails(
                    ticker=ticker,
                    symbol=ticker.split(' ')[0],
                    security_type=security_type,
                    expiry=datetime.now(),
                    strike=0,
                    lots=quantity_change,
                    lot_size=100,
                    qty=quantity_change * 100,
                    strategy=strategy,
                    direction='Long' if quantity_change > 0 else 'Short'
                )
            
            self.positions[ticker] = position_details
            logger.info(f"Created new position: {position_details}")
        else:
            # UPDATE EXISTING
            old_position = self.positions[ticker]
            old_lots = old_position.lots
            new_lots = old_lots + quantity_change
            
            if abs(new_lots) < 0.0001:
                # Position closed
                del self.positions[ticker]
                logger.info(f"Closed position for {ticker}")
            else:
                # Update position
                old_position.lots = new_lots
                old_position.strategy = strategy
                old_position.update_qty()
                logger.info(f"Updated {ticker}: {old_lots} -> {new_lots} lots")
    
    def get_position(self, ticker: str) -> Optional[PositionDetails]:
        """Get current position for a ticker"""
        return self.positions.get(ticker)
    
    def is_trade_opposing(self, ticker: str, trade_quantity: float, security_type: str) -> bool:
        """Check if trade opposes current position"""
        position = self.get_position(ticker)
        if position is None:
            return False
        return (position.lots > 0 and trade_quantity < 0) or \
               (position.lots < 0 and trade_quantity > 0)
    
    def get_final_positions(self) -> pd.DataFrame:
        """Get final positions with Yahoo prices"""
        if not self.positions:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'Ticker', 'Symbol', 'Security_Type', 'Expiry', 'Strike',
                'Lots', 'Lot_Size', 'QTY', 'Strategy', 'Direction',
                'Underlying', 'Yahoo_Price', 'Moneyness'
            ])
        
        positions_data = []
        
        for ticker, position in self.positions.items():
            positions_data.append({
                'Ticker': ticker,
                'Symbol': position.symbol,
                'Security_Type': position.security_type,
                'Expiry': position.expiry,
                'Strike': position.strike,
                'Lots': position.lots,
                'Lot_Size': position.lot_size,
                'QTY': position.qty,
                'Strategy': position.strategy,
                'Direction': position.direction,
                'Underlying': position.underlying_ticker
            })
        
        final_df = pd.DataFrame(positions_data)
        
        # Sort by ticker
        final_df = final_df.sort_values('Ticker').reset_index(drop=True)
        
        # Add Yahoo prices
        final_df = self.add_yahoo_prices(final_df)
        
        logger.info(f"Final positions: {len(final_df)} positions")
        
        return final_df
    
    def add_yahoo_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Yahoo prices and calculate moneyness for options"""
        if df.empty:
            return df
        
        df = df.copy()
        
        # Add columns if they don't exist
        if 'Yahoo_Price' not in df.columns:
            df['Yahoo_Price'] = 0.0
        if 'Moneyness' not in df.columns:
            df['Moneyness'] = ''
        
        logger.info(f"Fetching Yahoo prices for {len(df)} positions...")
        
        # Get unique symbols
        unique_symbols = df['Symbol'].unique()
        
        # Fetch price for each unique symbol
        for symbol in unique_symbols:
            price = self.price_fetcher.fetch_price_for_symbol(symbol)
            
            # Update all rows with this symbol
            symbol_mask = df['Symbol'] == symbol
            df.loc[symbol_mask, 'Yahoo_Price'] = price
            
            # Calculate moneyness for options
            for idx in df[symbol_mask].index:
                row = df.loc[idx]
                
                if row['Security_Type'] == 'Call' and price > 0:
                    strike = row['Strike']
                    if price > strike * 1.01:  # 1% buffer
                        df.at[idx, 'Moneyness'] = 'ITM'
                    elif price < strike * 0.99:
                        df.at[idx, 'Moneyness'] = 'OTM'
                    else:
                        df.at[idx, 'Moneyness'] = 'ATM'
                elif row['Security_Type'] == 'Put' and price > 0:
                    strike = row['Strike']
                    if price < strike * 0.99:  # 1% buffer
                        df.at[idx, 'Moneyness'] = 'ITM'
                    elif price > strike * 1.01:
                        df.at[idx, 'Moneyness'] = 'OTM'
                    else:
                        df.at[idx, 'Moneyness'] = 'ATM'
                elif row['Security_Type'] == 'Futures':
                    df.at[idx, 'Moneyness'] = 'N/A'
        
        # Round prices for display
        df['Yahoo_Price'] = df['Yahoo_Price'].round(2)
        
        return df
    
    def get_position_summary(self) -> Dict:
        """Get summary statistics"""
        if not self.positions:
            return {
                'total_positions': 0,
                'long_positions': 0,
                'short_positions': 0,
                'by_security_type': {},
                'by_strategy': {}
            }
        
        long_count = sum(1 for p in self.positions.values() if p.lots > 0)
        short_count = sum(1 for p in self.positions.values() if p.lots < 0)
        
        by_type = {}
        for p in self.positions.values():
            by_type[p.security_type] = by_type.get(p.security_type, 0) + 1
        
        by_strategy = {}
        for p in self.positions.values():
            by_strategy[p.strategy] = by_strategy.get(p.strategy, 0) + 1
        
        return {
            'total_positions': len(self.positions),
            'long_positions': long_count,
            'short_positions': short_count,
            'by_security_type': by_type,
            'by_strategy': by_strategy
        }
    
    def clear_all_positions(self):
        """Clear all positions"""
        self.positions.clear()
        self.ticker_details_map.clear()
        self.trade_details_cache.clear()
        self.price_fetcher.price_cache.clear()
        logger.info("Cleared all positions and price cache")
