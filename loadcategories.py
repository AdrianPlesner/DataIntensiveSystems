import pandas as pd
import psycopg2
import pygrametl
from pygrametl.tables import Dimension

cat = pd.read_csv("stregsystem_product_categories.sql")

stregsystem = psycopg2.connect(dbname="stregsystem", user="postgres", password="password")
connection = pygrametl.ConnectionWrapper(stregsystem)

cats = Dimension(
    name='stregsystem.stregsystem_product_categories',
    key='id',
    attributes=['product_id', 'category_id']
)


for row in cat.values:
    inset = {'id': int(row[0]), 'product_id': int(row[1]), 'category_id': int(row[2])}
    cats.insert(inset)

connection.commit()
connection.close()
