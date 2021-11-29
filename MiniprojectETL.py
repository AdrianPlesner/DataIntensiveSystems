import pygrametl
import psycopg2

from pygrametl.tables import FactTable, SlowlyChangingDimension, Dimension
from pygrametl.datasources import SQLSource

import datetime
import time

joinquery = "select pro.id, pro.name, pro.active, pro.deactivate_date, pro.price as currentprice, pro.start_date, " \
            "cat.category, op.price as nowprice, op.changed_on from stregsystem.stregsystem_product as pro, " \
            "stregsystem.stregsystem_product_categories as pc, stregsystem.stregsystem_category as cat, " \
            "stregsystem.stregsystem_oldprice as op where pro.id = pc.product_id and cat.id = pc.category_id and " \
            "op.product_id = pro.id "

product_category_join = "SELECT * FROM stregsystem.stregsystem_product, stregsystem.stregsystem_product_categories," \
                        " stregsystem.stregsystem_category " \
                        "WHERE stregsystem_product.id = stregsystem_product_categories.product_id and " \
                        "stregsystem_category.id = stregsystem_product_categories.category_id"

sales_join = "SELECT pro.name as name, mem.id as buyer_id, SUM(sal.price) as price, SUM(1) as amount, " \
             "date_trunc('day', sal.timestamp) as date FROM stregsystem.stregsystem_sale as sal, " \
             "stregsystem.stregsystem_member as mem, stregsystem.stregsystem_product as pro where sal.product_id = " \
             "pro.id and sal.member_id = mem.id group by pro.id, mem.id, date"

pgconn = psycopg2.connect(dbname="fklubdw", user='postgres', password='password')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')

stregsystem = psycopg2.connect(dbname="stregsystem", user="postgres", password="password")
member_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_member")
old_price_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_oldprice")
sale_source = SQLSource(stregsystem, sales_join)
products_with_categories = SQLSource(stregsystem, product_category_join)


def datehandling(row, namemapping):
    # This method is called from ensure(row) when the lookup of a date fails.
    # In the Real World, you would probably prefill the date dimension, but
    # we use this to illustrate "rowexpanders" that make it possible to
    # calculate derived attributes on demand (such that the - possibly
    # expensive - calculations only are done when needed and not for each
    # seen data row).
    #
    # Here, we calculate all date related fields and add them to the row.
    date = pygrametl.getvalue(row, 'date', namemapping)
    day = date.day
    month = date.month
    year = date.year
    (isoyear, isoweek, isoweekday) = datetime.date(year, month, day).isocalendar()
    row['day'] = day
    row['month'] = month
    row['year'] = year
    row['week'] = isoweek
    row['weekday'] = isoweekday
    return row


date_dim = Dimension(
    name='fklubdw.date',
    key='date_id',
    attributes=['date', 'day', 'weekday', 'month', 'year'],
    lookupatts=['date'],
    rowexpander=datehandling
)

member_dim = SlowlyChangingDimension(
    name='fklubdw.member',
    key='member_id',
    attributes=['buyer_id', 'year', 'want_spam', 'active', 'gender', 'valid_from', 'valid_to', 'version'],
    lookupatts=['buyer_id'],
    versionatt='version',
    fromatt='valid_from',
    toatt='valid_to'
)

product_dim = SlowlyChangingDimension(
    name='fklubdw.product',
    key='product_id',
    attributes=['name', 'category', 'active', 'start_date', 'deactivate_date', 'price', 'valid_from', 'valid_to',
                'version'],
    lookupatts=['name'],
    versionatt='version',
    fromatt='valid_from',
    toatt='valid_to'
)

sale_fact = FactTable(
    name='fklubdw.sales',
    keyrefs=['product_id', 'member_id', 'date_id'],
    measures=['amount', 'price']
)


def want_spam(val):
    if val:
        return "Yes"
    else:
        return "No"


def active_member(val):
    if val:
        return "Active"
    else:
        return "Inactive"


def gender(val):
    if val == 'U':
        return 'Unknown'
    elif val == 'F':
        return 'Female'
    elif val == 'M':
        return 'Male'
    else:
        return None


def load_member_dim(table):
    validto = datetime.datetime(year=9999, month=1, day=1)

    for row in table:
        if int(row['year']) > 1900:
            inset = {'buyer_id': row['id'], 'year': row['year'], 'want_spam': want_spam(row['want_spam']), 'active': active_member(row['active']),
                     'gender': gender(row['gender']), 'valid_from': datetime.datetime(int(row['year']), 1, 1),
                     'valid_to': validto, 'version': 1}
            member_dim.insert(inset)
    connection.commit()


def load_product_dim(table):
    validto = datetime.datetime(year=9999, month=1, day=1)
    for row in table:
        inset = row
        inset['active'] = active_member(inset['active'])
        inset['price'] /= 100
        inset['valid_from'] = datetime.datetime(1990, 1, 1)
        inset['valid_to'] = validto
        inset['version'] = 1

        product_dim.insert(inset)
    connection.commit()


def load_sales(table):
    fails = 0
    for row in table:
        print(row)
        row['date_id'] = date_dim.ensure(row)
        row['member_id'] = member_dim.lookup(row)
        row['product_id'] = product_dim.lookup(row)
        if row['date_id'] and row['member_id'] and row['product_id']:
            sale_fact.insert(row)
        else:
            fails += 1
    connection.commit()
    print(f'{fails} fails')


if __name__ == '__main__':
    print('here')
    # load_member_dim(member_source)
    # load_product_dim(products_with_categories)
    load_sales(sale_source)
    connection.close()
    # Fill tables
