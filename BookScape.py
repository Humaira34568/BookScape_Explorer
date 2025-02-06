import streamlit as st
import requests
import pandas as pd
import sqlite3
import time

# Constants
API_URL = "https://www.googleapis.com/books/v1/volumes"
DB_FILE = "books.db"


# Function to fetch books from Google Books API
def fetch_books(query, max_results=40):
    params = {"q": query, "maxResults": max_results}
    retries = 3
    for _ in range(retries):
        response = requests.get(API_URL, params=params)
        if response.status_code == 200:
            return response.json().get("items", [])
        elif response.status_code == 403:
            st.warning("API request limit reached. Retrying in 10 seconds...")
            time.sleep(10)
        else:
            st.error(f"Error fetching data: {response.status_code}")
            return []
    return []


# Function to transform API data into a DataFrame
def transform_data(books):
    data = []
    for book in books:
        volume_info = book.get("volumeInfo", {})
        sale_info = book.get("saleInfo", {})
        list_price = sale_info.get("listPrice", {})
        retail_price = sale_info.get("retailPrice", {})

        published_date = volume_info.get("publishedDate", "Unknown")
        published_year = "Unknown"
        if isinstance(published_date, str) and len(published_date) >= 4 and published_date[:4].isdigit():
            published_year = published_date[:4]

        data.append({
            "book_id": book.get("id"),
            "title": volume_info.get("title", "N/A"),
            "subtitle": volume_info.get("subtitle", "N/A"),
            "authors": ", ".join(volume_info.get("authors", ["Unknown Author"])) or "Unknown Author",
            "description": volume_info.get("description", "No description available."),
            "categories": ", ".join(volume_info.get("categories", ["N/A"])) or "N/A",
            "page_count": volume_info.get("pageCount", 0),
            "language": volume_info.get("language", "Unknown"),
            "image_link": volume_info.get("imageLinks", {}).get("thumbnail", ""),
            "average_rating": volume_info.get("averageRating", 0),
            "ratings_count": volume_info.get("ratingsCount", 0),
            "publisher": volume_info.get("publisher", "Unknown Publisher"),
            "published_year": published_year,
            "is_ebook": 1 if sale_info.get("isEbook", False) else 0,  # Fixed as 1 or 0
            "saleability": sale_info.get("saleability", "Not for Sale"),
            "amount_list_price": list_price.get("amount", 0),
            "currency_code_list_price": list_price.get("currencyCode", ""),
            "amount_retail_price": retail_price.get("amount", 0),
            "currency_code_retail_price": retail_price.get("currencyCode", ""),
            "buy_link": sale_info.get("buyLink", ""),
            "country": sale_info.get("country", "N/A")
        })
    return pd.DataFrame(data)


# Function to create or update database schema
def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            book_id TEXT PRIMARY KEY,
            title TEXT,
            subtitle TEXT,
            authors TEXT,
            description TEXT,
            categories TEXT,
            page_count INTEGER,
            language TEXT,
            image_link TEXT,
            average_rating REAL,
            ratings_count INTEGER,
            publisher TEXT,
            published_year TEXT,
            is_ebook INTEGER,
            saleability TEXT,
            amount_list_price REAL,
            currency_code_list_price TEXT,
            amount_retail_price REAL,
            currency_code_retail_price TEXT,
            buy_link TEXT,
            country TEXT
        )
    ''')
    conn.commit()
    conn.close()


# Function to save data to SQLite
def save_to_database(conn, df):
    df.to_sql("books", conn, if_exists="append", index=False)


# Function to query the database
def query_database(conn, query):
    return pd.read_sql_query(query, conn)


# Initialize database
initialize_database()

# Streamlit UI
st.set_page_config(page_title="BookScape Explorer", page_icon="📚", layout="wide")
st.title("🚀📚 BookScape Explorer")

# Sidebar Styling
st.sidebar.header("Search Books 🔍")

# Search input (at the top of the sidebar)
query = st.sidebar.text_input("Enter a search term", "Data Science")
if st.sidebar.button("Search"):
    with st.spinner("Fetching books..."):
        books = fetch_books(query)
        if books:
            df = transform_data(books)
            conn = sqlite3.connect(DB_FILE)
            save_to_database(conn, df)
            st.success(f"Fetched and saved {len(df)} books to the database.")
        else:
            st.error("No books found. Please try a different search term.")

# Dropdown for Data Analysis
st.sidebar.subheader("📊 Data Analytics")

# Define your 20 queries here
queries = {
    "Availability of eBooks vs Physical Books": "SELECT CASE WHEN is_ebook = 1 THEN 'eBook' ELSE 'Physical Book' END AS book_type, COUNT(*) AS count FROM books GROUP BY is_ebook",
    "Publisher with Most Books Published": "SELECT publisher, COUNT(*) AS count FROM books GROUP BY publisher ORDER BY count DESC LIMIT 1",
    "Publisher with Highest Average Rating": "SELECT publisher, AVG(average_rating) AS avg_rating FROM books GROUP BY publisher ORDER BY avg_rating DESC LIMIT 1",
    "Top 5 Most Expensive Books": "SELECT title, amount_retail_price FROM books ORDER BY amount_retail_price DESC LIMIT 5",
    "Books Published After 2010 with at Least 500 Pages": "SELECT title, page_count FROM books WHERE published_year > '2010' AND page_count >= 500",
    "Books with Discounts Greater than 20%": "SELECT title, amount_list_price, amount_retail_price FROM books WHERE (amount_list_price - amount_retail_price) / amount_list_price > 0.2",
    "Top 3 Authors with Most Books": "SELECT authors, COUNT(*) AS book_count FROM books GROUP BY authors ORDER BY book_count DESC LIMIT 3",
    "Books with More than 3 Authors": "SELECT title, authors FROM books WHERE LENGTH(authors) - LENGTH(REPLACE(authors, ',', '')) + 1 > 3",
    "Books Published in the Last 5 Years": "SELECT title, published_year FROM books WHERE published_year >= strftime('%Y', 'now') - 5",
    "Top 3 Most Popular Books by Rating": "SELECT title, average_rating FROM books ORDER BY average_rating DESC LIMIT 3",
    "Top 5 Most Expensive eBooks": "SELECT title, amount_retail_price FROM books WHERE is_ebook = 1 ORDER BY amount_retail_price DESC LIMIT 5",
    "Top 5 Books by Ratings Count": "SELECT title, ratings_count FROM books ORDER BY ratings_count DESC LIMIT 5",
    "Books with Missing Descriptions": "SELECT title FROM books WHERE description = 'No description available.'",
    "Books with Missing Images": "SELECT title FROM books WHERE image_link = ''",
    "Books with a Rating Below 3": "SELECT title FROM books WHERE average_rating < 3",
    "Books by Publisher with the Most Ratings": "SELECT publisher, SUM(ratings_count) FROM books GROUP BY publisher ORDER BY SUM(ratings_count) DESC LIMIT 1",
    "Books Over 1000 Pages": "SELECT title, page_count FROM books WHERE page_count > 1000",
    "Top 5 Books by Category": "SELECT categories, COUNT(*) FROM books GROUP BY categories ORDER BY COUNT(*) DESC LIMIT 5",
    "Top 3 Publishers with the Most eBooks": "SELECT publisher, COUNT(*) FROM books WHERE is_ebook = 1 GROUP BY publisher ORDER BY COUNT(*) DESC LIMIT 3",
    "Most Expensive Books by Publisher": "SELECT publisher, MAX(amount_retail_price) FROM books GROUP BY publisher ORDER BY MAX(amount_retail_price) DESC LIMIT 1"
}

# Dropdown selection for analytics
selected_query = st.sidebar.selectbox("Select Data Analysis", list(queries.keys()))

# Show the results of the selected query
if selected_query:
    conn = sqlite3.connect(DB_FILE)
    result = query_database(conn, queries[selected_query])
    st.sidebar.write(result)

# Display book details with images
st.header("📚 Book Details 🚀")
data = query_database(conn, "SELECT * FROM books")

# Display each book's details and image
for idx, row in data.iterrows():
    st.subheader(row["title"])
    st.write(f"**Authors**: {row['authors']}")
    st.write(f"**Description**: {row['description']}")
    st.write(f"**Categories**: {row['categories']}")
    st.write(f"**Published Year**: {row['published_year']}")
    st.write(f"**Average Rating**: {row['average_rating']} (based on {row['ratings_count']} ratings)")
    st.write(f"**Publisher**: {row['publisher']}")
    st.write(f"**Page Count**: {row['page_count']} pages")

    # Show book image if available
    if row['image_link']:
        st.image(row['image_link'], width=150)

    # Provide a buy link if available
    if row['buy_link']:
        st.write(f"[Buy the book here]({row['buy_link']})")

    st.markdown("---")
