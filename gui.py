import gradio as gr
import plotly.graph_objs as go
import pandas as pd
import random

# Generate example Forex data (replace with real data in practice)
def generate_forex_data():
    dates = pd.date_range(start="2024-01-01", periods=100, freq='H')
    data = {
        'date': dates,
        'open': [random.uniform(1.1, 1.5) for _ in range(100)],
        'high': [random.uniform(1.5, 1.6) for _ in range(100)],
        'low': [random.uniform(1.0, 1.4) for _ in range(100)],
        'close': [random.uniform(1.2, 1.5) for _ in range(100)]
    }
    df = pd.DataFrame(data)
    return df

# Create an initial chart with the data
def create_chart(df, orders):
    fig = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )])

    # Plot orders on the chart
    for order in orders:
        fig.add_shape(type="line",
                      x0=order['date'], x1=order['date'],
                      y0=min(df['low']), y1=max(df['high']),
                      line=dict(color="blue" if order['type'] == 'Buy' else 'red', width=2, dash='dot'))
        fig.add_annotation(x=order['date'], y=order['price'],
                           text=f"{order['type']} {order['size']} Lot",
                           showarrow=True, arrowhead=2, ax=0, ay=-40)
    return fig

# Order handler function
# Order handler function - made asynchronous for responsiveness
async def place_order(order_type, size, price, date, orders):
    new_order = {'type': order_type, 'size': size, 'price': price, 'date': date}
    orders.append(new_order)
    
    # Only add new annotations/shapes instead of recreating the entire chart
    updated_chart = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )])

    # Plot new orders (incremental updates)
    for order in orders:
        updated_chart.add_shape(type="line",
                                x0=order['date'], x1=order['date'],
                                y0=min(df['low']), y1=max(df['high']),
                                line=dict(color="blue" if order['type'] == 'Buy' else 'red', width=2, dash='dot'))
        updated_chart.add_annotation(x=order['date'], y=order['price'],
                                     text=f"{order['type']} {order['size']} Lot",
                                     showarrow=True, arrowhead=2, ax=0, ay=-40)

    return updated_chart, orders


# Generate initial data
df = generate_forex_data()
orders = []

# Gradio interface
with gr.Blocks() as demo:
    gr.Markdown("# Interactive Forex Chart and Order Placement")

    # Create components
    chart = gr.Plot(create_chart(df, orders))
    order_type = gr.Radio(["Buy", "Sell"], label="Order Type")
    size = gr.Number(value=1, label="Lot Size")
    # Using a slider for the price input
    price = gr.Slider(minimum=min(df['low']), maximum=max(df['high']), step=0.01, value=1.2, label="Order Price")
    date = gr.Dropdown(choices=df['date'].astype(str).tolist(), label="Order Date")
    place_order_button = gr.Button("Place Order")
    
    # Display outputs
    output_chart = gr.Plot()
    orders_list = gr.State(orders)

    # Button click interaction
    place_order_button.click(
        place_order,
        inputs=[order_type, size, price, date, orders_list],
        outputs=[output_chart, orders_list]
    )

demo.launch()
