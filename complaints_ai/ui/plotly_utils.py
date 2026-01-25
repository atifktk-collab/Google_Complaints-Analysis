import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Premium Color Palette
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ffbb78',
    'info': '#17becf',
    'neutral': '#7f7f7f',
    'background': 'rgba(31, 119, 180, 0.1)',
    'multi': px.colors.qualitative.Prism
}

def create_area_chart(df, x_col, y_col, title, color=None, color_discrete_sequence=None):
    """Creates a premium infographic-style area chart."""
    if color_discrete_sequence is None:
        color_discrete_sequence = [color] if color else [COLORS['primary']]
        
    fig = px.area(
        df,
        x=x_col,
        y=y_col,
        title=title,
        template="plotly_white",
        color_discrete_sequence=color_discrete_sequence
    )
    
    fig.update_traces(
        line_shape='spline',
        line_width=3,
        fillcolor=COLORS['background'] if not color else f"rgba({hex_to_rgb(color)}, 0.1)"
    )
    
    fig.update_layout(
        hovermode="x unified",
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=0, r=0, t=40, b=0),
        height=350,
        font=dict(family="Inter, sans-serif", size=12),
        title_font=dict(size=18),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def create_multi_line_chart(df, x_col, y_cols, title):
    """Creates a premium multi-line chart for regional/exchange comparisons."""
    fig = px.line(
        df,
        x=x_col,
        y=y_cols,
        title=title,
        template="plotly_white",
        color_discrete_sequence=COLORS['multi']
    )
    
    fig.update_traces(line_shape='spline', line_width=2)
    
    fig.update_layout(
        hovermode="x unified",
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=0, r=0, t=40, b=0),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def hex_to_rgb(hex_color):
    """Helper to convert hex to comma-separated RGB string."""
    hex_color = hex_color.lstrip('#')
    rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f"{rgb[0]}, {rgb[1]}, {rgb[2]}"
