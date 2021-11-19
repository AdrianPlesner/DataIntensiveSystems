import pygrametl
import psycopg2

from pygrametl.tables import FactTable, SlowlyChangingDimension, Dimension
from pygrametl.datasources import SQLSource

pgconn = psycopg2.connect(dbname="fklubdw", user='postgres', password='password')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')

stregsystem = psycopg2.connect(dbname="stregsystem", user="postgres", password="password")
category_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_category")
member_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_member")
old_price_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_oldprice")
product_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_product")
pc_rel = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_product_category")
sale_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_sale")

date_dim = Dimension(
    name='fklubdw.date',
    key='date_id',
    attributes=['day', 'weekday', 'month', 'year']
)

member_dim = SlowlyChangingDimension(
    name='member',
    key='memberID',
    attributes=['year', 'want_spam', 'active', 'gender', 'validFrom', 'validTo', 'version'],
    lookupatts=['year', 'want_spam', 'active', 'gender'],
    versionatt='version',
    fromatt='validFrom',
    toatt='validTo'
)

product_dim = SlowlyChangingDimension(
    name='product',
    key='productID',
    attributes=['name', 'category', 'active', 'startDate', 'deactivateDate', 'price', 'validFrom', 'validTo',
                'version'],
    lookupatts=['name'],
    versionatt='version',
    fromatt='validFrom',
    toatt='validTo'
)

sale_fact = FactTable(
    name='sales',
    keyrefs=['productID', 'memberID', 'dateID'],
    measures=['amount', 'price']
)

if __name__ == '__main__':
    print('here')
    # Fill tables
