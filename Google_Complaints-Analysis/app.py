"""
Streamlit Dashboard for Complaints Analysis
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

from src.models.complaint_analyzer import ComplaintAnalyzer
from src.data.data_loader import load_complaints_data
from config import PROCESSED_DATA_DIR

# Page configuration
st.set_page_config(
    page_title="Complaints Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)


def load_data():
    """Load processed complaints data"""
    try:
        data_path = PROCESSED_DATA_DIR / "analyzed_complaints.csv"
        if data_path.exists():
            return pd.read_csv(data_path)
        return None
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None


def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">📊 Complaints Analysis Dashboard</h1>', 
                unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", 
                            ["Overview", "Analyze Single Complaint", "Detailed Analytics"])
    
    if page == "Overview":
        show_overview()
    elif page == "Analyze Single Complaint":
        show_single_analysis()
    elif page == "Detailed Analytics":
        show_detailed_analytics()


def show_overview():
    """Display overview dashboard"""
    st.header("📈 Overview")
    
    # Load data
    df = load_data()
    
    if df is None or df.empty:
        st.warning("No data available. Please run the analysis first.")
        st.info("Run: `python main.py --input data/raw/complaints.csv`")
        return
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Complaints", len(df))
    with col2:
        negative_pct = (df['sentiment'] == 'negative').sum() / len(df) * 100
        st.metric("Negative Sentiment", f"{negative_pct:.1f}%")
    with col3:
        high_priority = (df['priority'].isin(['high', 'critical'])).sum()
        st.metric("High Priority", high_priority)
    with col4:
        categories = df['category'].nunique()
        st.metric("Categories", categories)
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Sentiment Distribution")
        sentiment_counts = df['sentiment'].value_counts()
        fig = px.pie(values=sentiment_counts.values, 
                     names=sentiment_counts.index,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Category Distribution")
        category_counts = df['category'].value_counts().head(8)
        fig = px.bar(x=category_counts.values, 
                     y=category_counts.index,
                     orientation='h',
                     color=category_counts.values,
                     color_continuous_scale='Blues')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Priority distribution
    st.subheader("Priority Levels")
    priority_counts = df['priority'].value_counts()
    fig = px.bar(x=priority_counts.index, 
                 y=priority_counts.values,
                 color=priority_counts.index,
                 color_discrete_map={
                     'low': '#2ecc71',
                     'medium': '#f39c12',
                     'high': '#e74c3c',
                     'critical': '#8e44ad'
                 })
    st.plotly_chart(fig, use_container_width=True)
    
    # Recent complaints table
    st.subheader("Recent Complaints")
    display_df = df[['complaint_id', 'sentiment', 'category', 'priority']].head(10)
    st.dataframe(display_df, use_container_width=True)


def show_single_analysis():
    """Analyze a single complaint"""
    st.header("🔍 Analyze Single Complaint")
    
    # Text input
    complaint_text = st.text_area(
        "Enter complaint text:",
        height=150,
        placeholder="Type or paste the complaint here..."
    )
    
    if st.button("Analyze", type="primary"):
        if complaint_text.strip():
            with st.spinner("Analyzing complaint..."):
                try:
                    # Initialize analyzer
                    analyzer = ComplaintAnalyzer()
                    
                    # Analyze
                    result = analyzer.analyze(complaint_text)
                    
                    # Display results
                    st.success("Analysis Complete!")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        sentiment_color = {
                            'positive': '🟢',
                            'neutral': '🟡',
                            'negative': '🔴'
                        }
                        st.metric("Sentiment", 
                                 f"{sentiment_color.get(result['sentiment'], '⚪')} {result['sentiment'].title()}")
                    
                    with col2:
                        st.metric("Category", result['category'])
                    
                    with col3:
                        priority_emoji = {
                            'low': '🟢',
                            'medium': '🟡',
                            'high': '🟠',
                            'critical': '🔴'
                        }
                        st.metric("Priority", 
                                 f"{priority_emoji.get(result['priority'], '⚪')} {result['priority'].title()}")
                    
                    # Sentiment score
                    st.subheader("Sentiment Score")
                    score = result.get('sentiment_score', 0)
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=score,
                        domain={'x': [0, 1], 'y': [0, 1]},
                        gauge={
                            'axis': {'range': [-1, 1]},
                            'bar': {'color': "darkblue"},
                            'steps': [
                                {'range': [-1, -0.3], 'color': "#ffcccc"},
                                {'range': [-0.3, 0.3], 'color': "#ffffcc"},
                                {'range': [0.3, 1], 'color': "#ccffcc"}
                            ],
                        }
                    ))
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Keywords
                    if 'keywords' in result and result['keywords']:
                        st.subheader("Key Topics")
                        keywords_str = ", ".join(result['keywords'])
                        st.info(keywords_str)
                    
                except Exception as e:
                    st.error(f"Error analyzing complaint: {str(e)}")
        else:
            st.warning("Please enter a complaint to analyze.")


def show_detailed_analytics():
    """Show detailed analytics"""
    st.header("📊 Detailed Analytics")
    
    df = load_data()
    
    if df is None or df.empty:
        st.warning("No data available. Please run the analysis first.")
        return
    
    # Filters
    st.sidebar.subheader("Filters")
    
    selected_sentiments = st.sidebar.multiselect(
        "Sentiment",
        options=df['sentiment'].unique(),
        default=df['sentiment'].unique()
    )
    
    selected_categories = st.sidebar.multiselect(
        "Category",
        options=df['category'].unique(),
        default=df['category'].unique()
    )
    
    # Apply filters
    filtered_df = df[
        (df['sentiment'].isin(selected_sentiments)) &
        (df['category'].isin(selected_categories))
    ]
    
    st.write(f"Showing {len(filtered_df)} of {len(df)} complaints")
    
    # Sentiment by category heatmap
    st.subheader("Sentiment by Category")
    heatmap_data = pd.crosstab(filtered_df['category'], filtered_df['sentiment'])
    fig = px.imshow(heatmap_data, 
                    color_continuous_scale='RdYlGn',
                    aspect='auto')
    st.plotly_chart(fig, use_container_width=True)
    
    # Download button
    st.subheader("Download Data")
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="Download Filtered Data as CSV",
        data=csv,
        file_name="filtered_complaints.csv",
        mime="text/csv"
    )


if __name__ == "__main__":
    main()

