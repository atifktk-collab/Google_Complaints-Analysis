"""
Dashboard and visualization generation
"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path
import logging

from config import VIZ_CONFIG, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = VIZ_CONFIG['figure_size']
plt.rcParams['font.size'] = VIZ_CONFIG['font_size']


def plot_sentiment_distribution(df: pd.DataFrame, save_path: str = None):
    """
    Plot sentiment distribution
    
    Args:
        df: DataFrame with sentiment column
        save_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    sentiment_counts = df['sentiment'].value_counts()
    colors = {'positive': '#2ecc71', 'neutral': '#f39c12', 'negative': '#e74c3c'}
    
    bars = ax.bar(sentiment_counts.index, sentiment_counts.values,
                  color=[colors.get(s, '#95a5a6') for s in sentiment_counts.index])
    
    ax.set_xlabel('Sentiment')
    ax.set_ylabel('Number of Complaints')
    ax.set_title('Sentiment Distribution of Complaints', fontsize=16, fontweight='bold')
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=12)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved sentiment distribution plot to {save_path}")
    
    plt.close()


def plot_category_distribution(df: pd.DataFrame, save_path: str = None):
    """
    Plot category distribution
    
    Args:
        df: DataFrame with category column
        save_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    category_counts = df['category'].value_counts().head(10)
    
    bars = ax.barh(range(len(category_counts)), category_counts.values,
                   color=sns.color_palette(VIZ_CONFIG['color_palette'], len(category_counts)))
    
    ax.set_yticks(range(len(category_counts)))
    ax.set_yticklabels(category_counts.index)
    ax.set_xlabel('Number of Complaints')
    ax.set_title('Top Complaint Categories', fontsize=16, fontweight='bold')
    
    # Add value labels
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width, bar.get_y() + bar.get_height()/2.,
                f'{int(width)}',
                ha='left', va='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved category distribution plot to {save_path}")
    
    plt.close()


def plot_priority_distribution(df: pd.DataFrame, save_path: str = None):
    """
    Plot priority distribution
    
    Args:
        df: DataFrame with priority column
        save_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    
    priority_order = ['low', 'medium', 'high', 'critical']
    priority_counts = df['priority'].value_counts()
    
    # Reorder according to priority levels
    ordered_counts = [priority_counts.get(p, 0) for p in priority_order]
    colors = ['#2ecc71', '#f39c12', '#e74c3c', '#8e44ad']
    
    bars = ax.bar(priority_order, ordered_counts, color=colors)
    
    ax.set_xlabel('Priority Level')
    ax.set_ylabel('Number of Complaints')
    ax.set_title('Priority Distribution of Complaints', fontsize=16, fontweight='bold')
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontsize=12)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved priority distribution plot to {save_path}")
    
    plt.close()


def plot_sentiment_by_category(df: pd.DataFrame, save_path: str = None):
    """
    Plot sentiment distribution by category
    
    Args:
        df: DataFrame with sentiment and category columns
        save_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create crosstab
    ct = pd.crosstab(df['category'], df['sentiment'])
    
    # Plot
    ct.plot(kind='bar', stacked=False, ax=ax,
            color=['#2ecc71', '#f39c12', '#e74c3c'])
    
    ax.set_xlabel('Category')
    ax.set_ylabel('Number of Complaints')
    ax.set_title('Sentiment Distribution by Category', fontsize=16, fontweight='bold')
    ax.legend(title='Sentiment')
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Saved sentiment by category plot to {save_path}")
    
    plt.close()


def generate_dashboard_report(df: pd.DataFrame, output_dir: str = None):
    """
    Generate complete dashboard report with all visualizations
    
    Args:
        df: DataFrame with analyzed complaints
        output_dir: Directory to save plots
    """
    try:
        if output_dir is None:
            output_dir = PROCESSED_DATA_DIR / "visualizations"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Generating dashboard visualizations...")
        
        # Generate all plots
        plot_sentiment_distribution(df, output_dir / "sentiment_distribution.png")
        plot_category_distribution(df, output_dir / "category_distribution.png")
        plot_priority_distribution(df, output_dir / "priority_distribution.png")
        plot_sentiment_by_category(df, output_dir / "sentiment_by_category.png")
        
        logger.info(f"Dashboard report generated successfully in {output_dir}")
        print(f"\n✅ Visualizations saved to: {output_dir}")
        
    except Exception as e:
        logger.error(f"Error generating dashboard report: {str(e)}")
        raise

