import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

load_dotenv()
connection_string = os.environ.get('ConnectionString')

# Load stock data from the database
def load_stock_data(symbol):
    engine = create_engine(connection_string)
    query = f'SELECT * FROM "{symbol}"'
    df = pd.read_sql(query, engine)
    return df

# Generate buy and sell signals based on the specified condition
def generate_signals(df):
    # Calculate moving averages
    df['50_day_ma'] = df['Adj Close'].rolling(window=50).mean()
    df['500_day_ma'] = df['Adj Close'].rolling(window=500).mean()
    df['200_day_ma'] = df['Adj Close'].rolling(window=200).mean()
    df['10_day_ma'] = df['Adj Close'].rolling(window=10).mean()
    df['20_day_ma'] = df['Adj Close'].rolling(window=20).mean()
    df['5_day_ma'] = df['Adj Close'].rolling(window=5).mean()

    # Generate signals
    df['Signal'] = 0
    df['Position'] = 0
    df['Reason'] = ''

    # Generate buy and sell signals with explanations
    # Buy Signal: 50-day crossover 500-day
    # Buy Signal: 50-day crossover 500-day
    df.loc[(df['50_day_ma'] > df['500_day_ma']) & (df['50_day_ma'].shift(1) <= df['500_day_ma'].shift(1)), 'Signal'] = 1
    df.loc[(df['50_day_ma'] > df['500_day_ma']) & (df['50_day_ma'].shift(1) <= df['500_day_ma'].shift(1)), 'Reason'] = 'Buy Signal: 50-day crossover 500-day'

    # Sell Signal: 20-day crossover 200-day
    df.loc[(df['20_day_ma'] > df['200_day_ma']) & (df['20_day_ma'].shift(1) <= df['200_day_ma'].shift(1)), 'Signal'] = -1
    df.loc[(df['20_day_ma'] > df['200_day_ma']) & (df['20_day_ma'].shift(1) <= df['200_day_ma'].shift(1)), 'Reason'] = 'Sell Signal: 20-day crossover 200-day'

    # Close Buy Position: 10-day crossover 20-day
    df.loc[(df['10_day_ma'] > df['20_day_ma']) & (df['10_day_ma'].shift(1) <= df['20_day_ma'].shift(1)), 'Position'] = 1
    df.loc[(df['10_day_ma'] > df['20_day_ma']) & (df['10_day_ma'].shift(1) <= df['20_day_ma'].shift(1)), 'Reason'] = 'Close Buy Position: 10-day crossover 20-day'

    # Close Sell Position: 5-day crossover 10-day
    df.loc[(df['5_day_ma'] > df['10_day_ma']) & (df['5_day_ma'].shift(1) <= df['10_day_ma'].shift(1)), 'Position'] = -1
    df.loc[(df['5_day_ma'] > df['10_day_ma']) & (df['5_day_ma'].shift(1) <= df['10_day_ma'].shift(1)), 'Reason'] = 'Close Sell Position: 5-day crossover 10-day'
    
    return df

def simulate_trading(df, initial_amount=10000):
    # Initialize variables
    amount = initial_amount
    position = 0  # 0 for no position, 1 for long (buy), -1 for short (sell)
    buy_price = 0  # Store buy price for calculating profit/loss

    # Iterate through the DataFrame rows
    for i in range(1, len(df)):
        # Buy Signal: 50-day crossover 500-day
        if df['Signal'].iloc[i] == 1 and position == 0:
            position = 1  # Set position to long (buy)
            buy_price = df['Adj Close'].iloc[i]
            amount -= buy_price  # Deduct buy price from the amount

        # Sell Signal: 20-day crossover 200-day
        elif df['Signal'].iloc[i] == -1 and position == 0:
            position = -1  # Set position to short (sell)
            amount += df['Adj Close'].iloc[i]  # Add sell price to the amount

        # Close Buy Position: 10-day crossover 20-day
        elif df['Position'].iloc[i] == 1 and position == 1:
            position = 0  # Close the long position
            amount += df['Adj Close'].iloc[i]  # Add sell price to the amount

        # Close Sell Position: 5-day crossover 10-day
        elif df['Position'].iloc[i] == -1 and position == -1:
            position = 0  # Close the short position
            amount -= df['Adj Close'].iloc[i]  # Deduct buy price from the amount

    # Calculate the final amount after all trades
    final_amount = amount + position * df['Adj Close'].iloc[-1]

    # Calculate profit/loss
    profit_loss = final_amount - initial_amount

    return profit_loss


# Visualize results with Plotly
def visualize_results(df, symbol):
    # Convert the index to DatetimeIndex
    df.index = pd.to_datetime(df.Date)

    # Create a candlestick figure with Plotly
    fig = go.Figure(data=[go.Candlestick(x=df.index,
                                         open=df['Open'],
                                         high=df['High'],
                                         low=df['Low'],
                                         close=df['Close'])])

    # Highlight buy and sell positions based on 'Position' column
    buys = df[df['Position'] == 1]  # Buy signals are indicated by 'Position' = 1
    sells = df[df['Position'] == -1]  # Sell signals are indicated by 'Position' = -1

    # Add buy and sell markers to the figure
    fig.add_trace(go.Scatter(x=buys.index, y=buys['Adj Close'], mode='markers', marker=dict(color='green', symbol='triangle-up', size=8), name='Buy'))
    fig.add_trace(go.Scatter(x=sells.index, y=sells['Adj Close'], mode='markers', marker=dict(color='red', symbol='triangle-down', size=8), name='Sell'))

    # Customize the layout
    fig.update_layout(title=f'{symbol} Stock Data',
                      xaxis_title='Date',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False,
                      showlegend=True)

    # Show the figure
    fig.show()

# Main function
def main():
    # Replace with the actual stock symbols you have
    symbols = ['AAPL', 'HDB', 'INR=X', 'JIOFIN.NS', 'MARA', 'TATAMOTORS.NS','TSLA']
    for symbol in symbols:
        df = load_stock_data(symbol)

        # Generate signals
        df = generate_signals(df)

        # Simulate trading and calculate profit/loss
        profit_loss= simulate_trading(df.copy())  # Avoid modifying original DataFrame
        print(f"\nProfit/Loss for {symbol}:")
        print(f"Total Profit/Loss: {profit_loss:.2f}")

        # Store trading results in the database (you need to create a table for results)
        engine = create_engine(connection_string)
        df.to_sql(f'{symbol}_trading_results', engine, if_exists='replace', index=False)

        # Visualize results
        visualize_results(df, symbol)

if __name__ == "__main__":
    main()
