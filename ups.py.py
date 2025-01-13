import streamlit as st
import requests
import pandas as pd
import sqlite3
import numpy as np

# Constants
API_URL = "https://www.googleapis.com/books/v1/volumes"
DB_FILE = "books.db"

# Function to fetch data from Google Books API
def fetch_books(query, max_results=40):
    params = {
        "q": query,
        "maxResults": max_results
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        return response.json().get("items", [])
    return []

# Function to transform API data into a DataFrame
def transform_data(books):
    data = []
    for book in books:
        volume_info = book.get("volumeInfo", {})
        sale_info = book.get("saleInfo", {})
        list_price = sale_info.get("listPrice", {})
        retail_price = sale_info.get("retailPrice", {})

        data.append({
            "book_id": book.get("id"),
            "title": volume_info.get("title"),
            "subtitle": volume_info.get("subtitle"),
            "authors": ", ".join(volume_info.get("authors", [])),
            "description": volume_info.get("description"),
            "categories": ", ".join(volume_info.get("categories", [])),
            "page_count": volume_info.get("pageCount"),
            "language": volume_info.get("language"),
            "image_link": volume_info.get("imageLinks", {}).get("thumbnail"),
            "average_rating": volume_info.get("averageRating"),
            "ratings_count": volume_info.get("ratingsCount"),
            "publisher": volume_info.get("publisher"),
            "published_year": volume_info.get("publishedDate", "")[:4],
            "is_ebook": sale_info.get("isEbook"),
            "saleability": sale_info.get("saleability"),
            "amount_list_price": list_price.get("amount"),
            "currency_code_list_price": list_price.get("currencyCode"),
            "amount_retail_price": retail_price.get("amount"),
            "currency_code_retail_price": retail_price.get("currencyCode"),
            "buy_link": sale_info.get("buyLink"),
            "country": sale_info.get("country")
        })
    df = pd.DataFrame(data)
    df.dropna(inplace=True)  # Drop all rows with any null values
    return df

# Function to save data to SQLite database
def save_to_database(df):
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("books", conn, if_exists="replace", index=False)
    conn.close()

# Function to query the database
def query_database(query):
    conn = sqlite3.connect(DB_FILE)
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

# Streamlit UI
st.title("🚀📚 BookScape Explorer")
st.sidebar.header("Search Books")

# Search input
query = st.sidebar.text_input("Enter a search term", "Data Science")
if st.sidebar.button("Search"):
    with st.spinner("Fetching books..."):
        books = fetch_books(query)
        if books:
            df = transform_data(books)
            save_to_database(df)
            st.success(f"Fetched and saved {len(df)} books to the database.")
        else:
            st.error("No books found.")

# Display stored data
st.header("Stored Book Data 🚀")
data = query_database("SELECT * FROM books")
st.dataframe(data)

# SQL Query interface
st.sidebar.header("Run SQL Queries 📚")
sql_query = st.sidebar.text_area("Enter your SQL query", "SELECT * FROM books LIMIT 10")
if st.sidebar.button("Run Query"):
    try:
        query_result = query_database(sql_query)
        st.header("Query Results")
        st.dataframe(query_result)
    except Exception as e:
        st.error(f"Error running query: {e}")

# Analysis options
st.sidebar.header("Analysis 🔍")
analysis_options = [
    "Availability of eBooks vs Physical Books",
    "Top 5 Most Expensive Books",
    "Books with High Ratings",
    "Books by Year and Publisher",
    "Find the Publisher with the Most Books Published",
    "Identify the Publisher with the Highest Average Rating",
    "Find Books Published After 2010 with at Least 500 Pages",
    "List Books with Discounts Greater than 20%",
    "Find the Average Page Count for eBooks vs Physical Books",
    "Find the Top 3 Authors with the Most Books",
    "List Publishers with More than 10 Books",
    "Find the Average Page Count for Each Category",
    "Retrieve Books with More than 3 Authors",
    "Books with Ratings Count Greater Than the Average",
    "Books with the Same Author Published in the Same Year",
    "Books with a Specific Keyword in the Title",
    "Year with the Highest Average Book Price",
    "Count Authors Who Published 3 Consecutive Years",
    "Authors Published in Same Year but Different Publishers"
]
analysis_choice = st.sidebar.selectbox("Choose Analysis", analysis_options)

if st.sidebar.button("Run Analysis"):
    try:
        if analysis_choice == "Availability of eBooks vs Physical Books":
            result = query_database("""
                SELECT is_ebook, COUNT(*) as count
                FROM books
                GROUP BY is_ebook
            """)
            st.bar_chart(result.set_index("is_ebook"))
        elif analysis_choice == "Top 5 Most Expensive Books":
            result = query_database("""
                SELECT title, amount_retail_price, currency_code_retail_price
                FROM books
                ORDER BY amount_retail_price DESC
                LIMIT 5
            """)
            st.table(result)
        elif analysis_choice == "Books with High Ratings":
            result = query_database("""
                SELECT title, average_rating, ratings_count
                FROM books
                WHERE average_rating > 4.5
                ORDER BY average_rating DESC
            """)
            st.table(result)
        elif analysis_choice == "Books by Year and Publisher":
            result = query_database("""
                SELECT published_year, publisher, COUNT(*) as book_count
                FROM books
                GROUP BY published_year, publisher
                ORDER BY published_year DESC
            """)
            st.table(result)
        elif analysis_choice == "Find the Publisher with the Most Books Published":
            result = query_database("""
                SELECT publisher, COUNT(*) as book_count
                FROM books
                GROUP BY publisher
                ORDER BY book_count DESC
                LIMIT 1
            """)
            st.table(result)
        elif analysis_choice == "Identify the Publisher with the Highest Average Rating":
            result = query_database("""
                SELECT publisher, AVG(average_rating) as avg_rating, COUNT(*) as book_count
                FROM books
                GROUP BY publisher
                HAVING COUNT(*) > 10
                ORDER BY avg_rating DESC
                LIMIT 1
            """)
            st.table(result)
        elif analysis_choice == "Find Books Published After 2010 with at Least 500 Pages":
            result = query_database("""
                SELECT title, authors, page_count, published_year
                FROM books
                WHERE published_year > '2010' AND page_count >= 500
            """)
            st.table(result)
        elif analysis_choice == "List Books with Discounts Greater than 20%":
            result = query_database("""
                SELECT title, amount_list_price, amount_retail_price, 
                       ((amount_list_price - amount_retail_price) / amount_list_price) * 100 AS discount_percentage
                FROM books
                WHERE amount_list_price > 0 AND 
                      (amount_list_price - amount_retail_price) / amount_list_price >= 0.2
            """)
            st.table(result)
        elif analysis_choice == "Find the Average Page Count for eBooks vs Physical Books":
            result = query_database("""
                SELECT is_ebook, AVG(page_count) as avg_page_count
                FROM books
                GROUP BY is_ebook
            """)
            st.bar_chart(result.set_index("is_ebook"))
        elif analysis_choice == "Find the Top 3 Authors with the Most Books":
            result = query_database("""
                SELECT authors, COUNT(*) as book_count
                FROM books
                GROUP BY authors
                ORDER BY book_count DESC
                LIMIT 3
            """)
            st.table(result)
        elif analysis_choice == "List Publishers with More than 10 Books":
            result = query_database("""
                SELECT publisher, COUNT(*) as book_count
                FROM books
                GROUP BY publisher
                HAVING COUNT(*) > 10
                ORDER BY book_count DESC
            """)
            st.table(result)
        elif analysis_choice == "Find the Average Page Count for Each Category":
            result = query_database("""
                SELECT categories, AVG(page_count) as avg_page_count
                FROM books
                GROUP BY categories
                ORDER BY avg_page_count DESC
            """)
            st.table(result)
        elif analysis_choice == "Retrieve Books with More than 3 Authors":
            result = query_database("""
                SELECT title, authors
                FROM books
                WHERE LENGTH(authors) - LENGTH(REPLACE(authors, ',', '')) + 1 > 3
            """)
            st.table(result)
        elif analysis_choice == "Books with Ratings Count Greater Than the Average":
            result = query_database("""
                SELECT title, average_rating, ratings_count
                FROM books
                WHERE ratings_count > (SELECT AVG(ratings_count) FROM books)
                ORDER BY ratings_count DESC
            """)
            st.table(result)
        elif analysis_choice == "Books with the Same Author Published in the Same Year":
            result = query_database("""
                SELECT authors, published_year, COUNT(*) as book_count
                FROM books
                GROUP BY authors, published_year
                HAVING COUNT(*) > 1
            """)
            st.table(result)
        elif analysis_choice == "Books with a Specific Keyword in the Title":
            keyword = st.sidebar.text_input("Enter keyword", "Python")
            result = query_database(f"""
                SELECT title, authors, published_year
                FROM books
                WHERE title LIKE '%{keyword}%'
            """)
            st.table(result)
        elif analysis_choice == "Year with the Highest Average Book Price":
            result = query_database("""
                SELECT published_year, AVG(amount_retail_price) as avg_price
                FROM books
                GROUP BY published_year
                ORDER BY avg_price DESC
                LIMIT 1
            """)
            st.table(result)
        elif analysis_choice == "Count Authors Who Published 3 Consecutive Years":
            result = query_database("""
                SELECT authors, COUNT(DISTINCT published_year) as consecutive_years
                FROM books
                GROUP BY authors
                HAVING consecutive_years >= 3
            """)
            st.table(result)
        elif analysis_choice == "Authors Published in Same Year but Different Publishers":
            result = query_database("""
                SELECT authors, published_year, COUNT(DISTINCT publisher) as publisher_count
                FROM books
                GROUP BY authors, published_year
                HAVING publisher_count > 1
            """)
            st.table(result)
    except Exception as e:
        st.error(f"Error running analysis: {e}")

# Display detailed book information
st.header("Book Details with Images 🚀📚")
for _, row in data.iterrows():
    st.subheader(f"{row['title']} ({row['published_year']})")
    if row['image_link']:
        st.image(row['image_link'], width=150)
    st.write(f"**Authors:** {row['authors']}")
    st.write(f"**Description:** {row['description']}")
    st.write(f"**Categories:** {row['categories']}")
    st.write(f"**Page Count:** {row['page_count']}")
    st.write(f"**Language:** {row['language']}")
    st.write(f"**Average Rating:** {row['average_rating']} ({row['ratings_count']} ratings)")
    st.write(f"**Price:** {row['amount_retail_price']} {row['currency_code_retail_price']}")
    st.write(f"**Saleability:** {row['saleability']}")
    st.write(f"**Publisher:** {row['publisher']}")
    st.write(f"**Country:** {row['country']}")
    if row['buy_link']:
        st.markdown(f"[Buy this book]({row['buy_link']})")
    st.markdown("---")

# Footer
st.markdown("---")
st.text("Developed with ❤️ using Streamlit and Google Books API")
