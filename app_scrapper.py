import sqlite3
import requests
from bs4 import BeautifulSoup


conn = sqlite3.connect("database/booksdatabase.db")
cur = conn.cursor()


cur.execute(
    """CREATE TABLE IF NOT EXISTS books
    (name text, price text, availability text, rating integer)"""
)


conn.commit()


base_url = "http://books.toscrape.com/catalogue/page-{}.html"

for i in range(1, 51):
    req = requests.get(base_url.format(i))
    soup = BeautifulSoup(req.text, "lxml")
    books = soup.select(".product_pod")

    for j in books:
        name = j.select("a")[1]["title"]
        price = j.select(".price_color")[0].text[1:]
        availability = j.select(".availability")[0].text.strip()
        rating = ["Zero", "One", "Two", "Three", "Four", "Five"].index(
            j.select(".star-rating")[0]["class"][1]
        )

        cur.execute(
            "INSERT INTO books VALUES (?, ?, ?, ?)", (name, price, availability, rating)
        )


conn.commit()
conn.close()
