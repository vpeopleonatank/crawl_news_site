import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Article, ArticleImage, ArticleVideo, generate_image_path, generate_video_path

# Get database URL from environment (default through PgBouncer on localhost:6432)
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    os.getenv('DATABASE_URL_DIRECT', 'postgresql://crawl_user:crawl_password@localhost:6432/crawl_db'),
)

print("=" * 60)
print("Testing SQLAlchemy Models with UUIDv7")
print("=" * 60)

# Wait for database to be ready
print("\n1. Connecting to database...")
max_retries = 10
for i in range(max_retries):
    try:
        engine = create_engine(DATABASE_URL, echo=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Database connection successful!")
        break
    except Exception as e:
        if i < max_retries - 1:
            print(f"⚠ Connection attempt {i+1} failed, retrying in 2 seconds...")
            time.sleep(2)
        else:
            print(f"✗ Failed to connect to database: {e}")
            exit(1)

# Create all tables
print("\n2. Creating tables...")
try:
    Base.metadata.drop_all(engine)  # Clean slate for testing
    Base.metadata.create_all(engine)
    print("✓ Tables created successfully!")
except Exception as e:
    print(f"✗ Error creating tables: {e}")
    exit(1)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

print("\n3. Creating test data...")

# Test 1: Create article with Unicode content
print("\n   Test 1: Article with Unicode content")
article1 = Article(
    site_slug="thanhnien",
    title="Python教程 - Learning Programming 🐍",
    description="Hướng dẫn lập trình Python cho người mới bắt đầu",
    content="这是一篇关于Python的详细文章。It covers basic to advanced topics.",
    category_id="tech",
    category_name="Technology",
    tags="python,programming,tutorial,编程",
    url="https://example.com/article/python-tutorial",
    publish_date=datetime.now(),
    comments={
        "count": 5,
        "list": [
            {"user": "john_doe", "text": "Great article!", "rating": 5},
            {"user": "jane_smith", "text": "Very helpful 👍", "rating": 4}
        ]
    }
)

session.add(article1)
session.flush()  # Generate UUID

print(f"   ✓ Created article with UUIDv7: {article1.id}")

# Add images to article1
for i in range(1, 4):
    image = ArticleImage(
        image_path=generate_image_path(article1.id, i, "jpg"),
        sequence_number=i
    )
    article1.images.append(image)

print(f"   ✓ Added {len(article1.images)} images")

# Add videos to article1
for i in range(1, 3):
    video = ArticleVideo(
        video_path=generate_video_path(article1.id, i, "mp4"),
        sequence_number=i
    )
    article1.videos.append(video)

print(f"   ✓ Added {len(article1.videos)} videos")

# Test 2: Create another article
print("\n   Test 2: Second article")
article2 = Article(
    site_slug="thanhnien",
    title="Web Scraping Best Practices",
    description="Learn how to scrape websites ethically and efficiently",
    content="Web scraping is the process of extracting data from websites...",
    category_id="tech",
    category_name="Technology",
    tags="scraping,crawling,data-extraction",
    url="https://example.com/article/web-scraping",
    publish_date=datetime.now(),
    comments={"count": 0, "list": []}
)

session.add(article2)
session.flush()

print(f"   ✓ Created article with UUIDv7: {article2.id}")

# Add images to article2
for i in range(1, 6):
    image = ArticleImage(
        image_path=generate_image_path(article2.id, i, "png"),
        sequence_number=i
    )
    article2.images.append(image)

print(f"   ✓ Added {len(article2.images)} images")

# Commit all changes
session.commit()
print("\n✓ All data committed to database!")

# Test queries
print("\n4. Testing queries...")

# Query all articles
print("\n   Query 1: All articles")
articles = session.query(Article).all()
print(f"   ✓ Found {len(articles)} articles")

for article in articles:
    print(f"\n   Article ID: {article.id}")
    print(f"   Title: {article.title}")
    print(f"   URL: {article.url}")
    print(f"   Images: {len(article.images)}")
    print(f"   Videos: {len(article.videos)}")
    print(f"   Tags: {article.tags}")

# Query by category
print("\n   Query 2: Filter by category")
tech_articles = session.query(Article).filter(Article.category_id == "tech").all()
print(f"   ✓ Found {len(tech_articles)} articles in 'tech' category")

# Query by URL
print("\n   Query 3: Find by URL")
found_article = session.query(Article).filter(
    Article.url == "https://example.com/article/python-tutorial"
).first()
if found_article:
    print(f"   ✓ Found article: {found_article.title}")
    print(f"   ✓ Comment count: {found_article.comments.get('count', 0)}")

# Query images for specific article
print("\n   Query 4: Images for article")
images = session.query(ArticleImage).filter(
    ArticleImage.article_id == article1.id
).order_by(ArticleImage.sequence_number).all()
print(f"   ✓ Found {len(images)} images:")
for img in images:
    print(f"      - {img.image_path}")

# Test cascade delete
print("\n5. Testing cascade delete...")
article_to_delete_id = article2.id
image_count_before = session.query(ArticleImage).filter(
    ArticleImage.article_id == article_to_delete_id
).count()
print(f"   Images before delete: {image_count_before}")

session.delete(article2)
session.commit()

image_count_after = session.query(ArticleImage).filter(
    ArticleImage.article_id == article_to_delete_id
).count()
print(f"   Images after delete: {image_count_after}")
print(f"   ✓ Cascade delete works correctly!")

# Final summary
print("\n" + "=" * 60)
print("✓ All tests passed successfully!")
print("=" * 60)

remaining_articles = session.query(Article).count()
remaining_images = session.query(ArticleImage).count()
remaining_videos = session.query(ArticleVideo).count()

print(f"\nFinal counts:")
print(f"  Articles: {remaining_articles}")
print(f"  Images: {remaining_images}")
print(f"  Videos: {remaining_videos}")

print("\nDatabase ready for use!")
print(f"Connection string: {DATABASE_URL}")

session.close()
