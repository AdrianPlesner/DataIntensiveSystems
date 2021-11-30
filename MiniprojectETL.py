import pygrametl
import psycopg2

from pygrametl.tables import FactTable, SlowlyChangingDimension, Dimension
from pygrametl.datasources import SQLSource
from pygrametl.steps import SourceStep, SCDimensionStep, MappingStep, PrintStep, connectsteps, RenamingStep, \
    ConditionalStep, Step

import datetime

joinquery = "select pro.id, pro.name, pro.active, pro.deactivate_date, pro.price as currentprice, pro.start_date, " \
            "cat.category, op.price as nowprice, op.changed_on from stregsystem.stregsystem_product as pro, " \
            "stregsystem.stregsystem_product_categories as pc, stregsystem.stregsystem_category as cat, " \
            "stregsystem.stregsystem_oldprice as op where pro.id = pc.product_id and cat.id = pc.category_id and " \
            "op.product_id = pro.id "

product_category_join = "SELECT * FROM stregsystem.stregsystem_product, stregsystem.stregsystem_product_categories," \
                        " stregsystem.stregsystem_category " \
                        "WHERE stregsystem_product.id = stregsystem_product_categories.product_id and " \
                        "stregsystem_category.id = stregsystem_product_categories.category_id " \
                        "order by stregsystem.stregsystem_product.id"

sales_join = "SELECT pro.name as name, mem.id as buyer_id, SUM(sal.price) as price, SUM(1) as amount, " \
             "date_trunc('day', sal.timestamp) as date FROM stregsystem.stregsystem_sale as sal, " \
             "stregsystem.stregsystem_member as mem, stregsystem.stregsystem_product as pro where sal.product_id = " \
             "pro.id and sal.member_id = mem.id group by pro.id, mem.id, date"

old_price_join = "select op.price, op.changed_on, pro.name, pro.active, pro.deactivate_date, pro.start_date, " \
                 "cat.category from stregsystem.stregsystem_oldprice as op, stregsystem.stregsystem_product as pro, " \
                 "stregsystem.stregsystem_product_categories as pc, stregsystem.stregsystem_category as cat where " \
                 "op.product_id = pro.id and pro.id = pc.product_id and pc.category_id = cat.id order by pro.id"

initial_product_join = "select distinct times.mintime, sal.price, pro.name, pro.active, " \
                       "pro.start_date, pro.deactivate_date, cat.category from stregsystem.stregsystem_sale as sal, " \
                       "( select min(sal.timestamp) as mintime, sal.product_id from stregsystem.stregsystem_sale as" \
                       " sal group by sal.product_id) as times, stregsystem.stregsystem_product as pro, " \
                       "stregsystem.stregsystem_product_categories as pc, stregsystem.stregsystem_category as cat " \
                       "where sal.product_id = times.product_id and sal.timestamp = times.mintime and pro.id = " \
                       "times.product_id and pro.id = pc.product_id and pc.category_id = cat.id"

pgconn = psycopg2.connect(dbname="fklubdw", user='postgres', password='password')
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to pygrametlexa')

stregsystem = psycopg2.connect(dbname="stregsystem", user="postgres", password="password")
member_source = SQLSource(stregsystem, "SELECT * FROM stregsystem.stregsystem_member")
old_price_source = SQLSource(stregsystem, old_price_join)
sale_source = SQLSource(stregsystem, sales_join)
products_with_categories = SQLSource(stregsystem, product_category_join)
initial_products = SQLSource(stregsystem, initial_product_join)


def datehandling(row, namemapping=None):
    if namemapping is None:
        namemapping = {}
    date = pygrametl.getvalue(row, 'date', namemapping)
    day = date.day
    month = date.month
    year = date.year
    (isoyear, isoweek, isoweekday) = datetime.date(year, month, day).isocalendar()
    row['day'] = day
    row['month'] = month
    row['year'] = year
    row['week'] = isoweek
    row['weekday'] = "Weekday" if isoweekday <= 5 else "Weekend"
    return row


validmax = datetime.datetime(year=9999, month=12, day=31)


date_dim = Dimension(
    name='fklubdw.date',
    key='date_id',
    attributes=['day', 'weekday', 'month', 'year'],
    lookupatts=['day', 'month', 'year'],
    rowexpander=datehandling
)

member_dim = SlowlyChangingDimension(
    name='fklubdw.member',
    key='member_id',
    attributes=['buyer_id', 'year', 'want_spam', 'active', 'gender', 'valid_from', 'valid_to', 'version'],
    lookupatts=['buyer_id'],
    versionatt='version',
    fromatt='valid_from',
    toatt='valid_to',
    srcdateatt='year',
    srcdateparser=lambda y: datetime.datetime(int(y), 1, 1),
    maxto=validmax
)

product_dim = SlowlyChangingDimension(
    name='fklubdw.product',
    key='product_id',
    attributes=['name', 'category', 'active', 'start_date', 'deactivate_date', 'price', 'valid_from', 'valid_to',
                'version'],
    lookupatts=['name'],
    versionatt='version',
    srcdateatt='valid_from',
    srcdateparser=lambda date: date,
    maxto=validmax,
    fromatt='valid_from',
    toatt='valid_to'
)

sale_fact = FactTable(
    name='fklubdw.sales',
    keyrefs=['product_id', 'member_id', 'date_id'],
    measures=['amount', 'price']
)


def transform_spam(val):
    if val:
        return "Yes"
    else:
        return "No"


def transform_active(val):
    if val:
        return "Active"
    else:
        return "Inactive"


def transform_gender(val):
    if val == 'U':
        return 'Unknown'
    elif val == 'F':
        return 'Female'
    elif val == 'M':
        return 'Male'
    else:
        return None


def load_sales(table):
    fails = 0
    i = 0
    for row in table:
        print(i)
        i += 1
        row = datehandling(row)
        row['date_id'] = date_dim.ensure(row)
        row['member_id'] = member_dim.lookup(row)
        row['product_id'] = product_dim.lookup(row)
        row['price'] /= 100
        if row['date_id'] and row['member_id'] and row['product_id']:
            sale_fact.insert(row)

        else:
            fails += 1
    print(f'{fails} fails')
    connection.commit()


def remove_duplicate_prices(productlst):
    id = productlst[0]['name']
    result = []
    oldp = 0
    for p in productlst:
        if p['name'] == id:
            if p['price'] != oldp:
                result.append(p)
                oldp = p['price']
        else:
            id = p['name']
            oldp = p['price']
    return result


def member_flow():
    mem_extr_step = SourceStep(member_source)
    mem_rename_step = RenamingStep({'id': "buyer_id"})
    mem_year_fix = MappingStep([('year', lambda y: "1970")])
    mem_trans_step = MappingStep(
        [('want_spam', transform_spam), ('active', transform_active), ('gender', transform_gender)])
    mem_year_cond = ConditionalStep(lambda row: True if int(row['year']) > 1970 else False, mem_trans_step,
                                    mem_year_fix)
    print_members = PrintStep()
    mem_load_step = SCDimensionStep(member_dim)
    connectsteps(mem_extr_step, mem_rename_step, mem_year_cond)
    connectsteps(mem_year_fix, mem_trans_step, print_members, mem_load_step)
    mem_extr_step.start()
    connection.commit()


def product_flow():
    extr_init = SourceStep(initial_products)
    rename_init = RenamingStep({'mintime': "valid_from"})
    price_map = MappingStep([('price', lambda x: int(x) / 100)])
    load_initial = SCDimensionStep(product_dim)
    print_init = PrintStep()
    connectsteps(extr_init, rename_init, price_map, load_initial)
    extr_init.start()
    connection.commit()

    pro_extr_step = SourceStep(remove_duplicate_prices(list(old_price_source)))
    rename_date = RenamingStep({'changed_on': "valid_from"})
    connectsteps(pro_extr_step, rename_date, price_map, load_initial)
    pro_extr_step.start()
    connection.commit()


if __name__ == '__main__':
    print('here')

    # member_flow()
    # product_flow()
    load_sales(sale_source)

    connection.commit()
    connection.close()
    # Fill tables
