import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set non-interactive backend
plt.switch_backend('Agg')

def create_visualizations(df, output_dir='visualizations'):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 1. Views vs Likes Correlation
    plt.figure(figsize=(10, 6))
    plt.scatter(df['views'], df['likes'], alpha=0.5)
    plt.xlabel('Views')
    plt.ylabel('Likes')
    plt.title('Correlation between Views and Likes')
    plt.savefig(f'{output_dir}/views_likes_correlation.png')
    plt.close()

    # 2. Top 10 Most Viewed Videos
    plt.figure(figsize=(12, 6))
    top_videos = df.nlargest(10, 'views')
    sns.barplot(data=top_videos, x='views', y='title')
    plt.title('Top 10 Most Viewed Videos')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/top_videos.png')
    plt.close()

    # 3. Video Publishing Trends Over Time
    df['publish_time'] = pd.to_datetime(df['publish_time'])
    df['publish_date'] = df['publish_time'].dt.date
    daily_videos = df.groupby('publish_date').size()
    
    plt.figure(figsize=(15, 6))
    daily_videos.plot(kind='line')
    plt.title('Video Publishing Trends Over Time')
    plt.xlabel('Date')
    plt.ylabel('Number of Videos Published')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/publishing_trends.png')
    plt.close()

    # 4. Engagement Analysis (Comments vs Dislikes)
    plt.figure(figsize=(10, 6))
    plt.scatter(df['comment_count'], df['dislikes'], alpha=0.5)
    plt.xlabel('Comment Count')
    plt.ylabel('Dislikes')
    plt.title('Relationship between Comments and Dislikes')
    plt.savefig(f'{output_dir}/comments_dislikes.png')
    plt.close()

    # 5. Category Distribution
    plt.figure(figsize=(10, 6))
    category_counts = df['category_id'].value_counts()
    sns.barplot(x=category_counts.index, y=category_counts.values)
    plt.title('Distribution of Videos by Category')
    plt.xlabel('Category ID')
    plt.ylabel('Number of Videos')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/category_distribution.png')
    plt.close()

    # 4. Comment vs Like Ratio Distribution
    plt.figure(figsize=(10, 6))
    df['comment_like_ratio'] = df['comment_count'] / df['likes']
    sns.histplot(data=df, x='comment_like_ratio', bins=50)
    plt.title('Distribution of Comment to Like Ratio')
    plt.xlabel('Comment/Like Ratio')
    plt.savefig(f'{output_dir}/comment_like_ratio.png')
    plt.close()

    # 5. Video Duration Analysis
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='category_id', y='duration')
    plt.title('Video Duration by Category')
    plt.xlabel('Category ID')
    plt.ylabel('Duration (seconds)')
    plt.xticks(rotation=45)
    plt.savefig(f'{output_dir}/duration_by_category.png')
    plt.close()

def analyze_youtube_data(processed_dir):
    # Read and combine data
    dfs = []
    for file in os.listdir(processed_dir):
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(processed_dir, file))
            dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Print basic statistics
    print("\nDataset Overview:")
    print(f"Total number of videos: {len(combined_df)}")
    print("\nNumerical Columns Statistics:")
    print(combined_df.describe())
    
    # Create visualizations
    create_visualizations(combined_df)

if __name__ == "__main__":
    analyze_youtube_data("processed_data")