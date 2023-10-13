# psycopg2 is a database driver allowing CPython to access PostgreSQL
import psycopg2


# pygrametl's __init__ file provides a set of helper functions and more
# importantly the class ConnectionWrapper for wrapping PEP 249 connections
import pygrametl

# pygrametl makes it simple to read external data through datasources
from pygrametl.datasources import SQLSource, CSVSource

# Interacting with the dimensions and the fact table is done through a set
# of classes. A suitable object must be created for each table
from pygrametl.tables import CachedDimension, FactTable

from decimal import Decimal

from datetime import date

dw_string = "host='localhost' dbname='fklubdw' user='postgres' password='dwpass'"
dw_conn = psycopg2.connect(dw_string)
print(dw_conn);
# Although the ConnectionWrapper is shared automatically between pygrametl
# abstractions, it is saved in a variable so the connection can be closed
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_conn);


# Creation of dimension and fact table abstractions for use in the ETL flow
time_dim = CachedDimension(
        name='time_dim',
        key='time_id',
        attributes=['year','quarter','month','day']
    )


product_dim = CachedDimension(
        name='product_dim',
        key='product_id',
        attributes=['product_name', 'alcohol_ml','price']
    )

user_dim = CachedDimension(
        name='user_dim',
        key='user_id',
        attributes=['member_id','gender','year_joined']
    )




sale_fact = FactTable(
        name='sale_fact',
        keyrefs=['fk_time', 'fk_product', 'fk_user'],
        measures=['price','quantity'])


product_handler = open('product.csv', 'r', 16384, "utf-8")
product_source = CSVSource(f=product_handler, delimiter=';')


member_handler = open('member.csv', 'r', 16384, "utf-8")
member_source = CSVSource(f=member_handler, delimiter=';')

sale_handler = open('sale.csv', 'r', 16384, "utf-8")
sale_source = CSVSource(f=sale_handler, delimiter=';')

def ParseGender(gender):
    if gender == "M":
        return "Male";
    if gender == "F":
        return "Female";
    if gender == "U":
        return "Unknown"
    raise Exception("hell")


def CreateUserDim(member_source):
    user_dict = {}

    for row in member_source:
        datasetrow = {}
        datasetrow['gender'] = ParseGender(row['gender'])
        datasetrow['year_joined'] = int(row['year'])
        datasetrow['member_id'] = int(row['id'])
        datasetrow['user_id'] = user_dim.ensure(datasetrow)
        user_dict[row['id']] = datasetrow
    return user_dict

def CreateProductDim(product_source):
    product_dict = {}
    for row in product_source:
        datasetrow = {}
        datasetrow['product_name'] = row['name']
        datasetrow['alcohol_ml'] = int(Decimal(row['alcohol_content_ml'])*1000)
        datasetrow['price'] = int(row['price'])
        datasetrow['product_id'] = product_dim.ensure(datasetrow)
        product_dict[row['id']] = datasetrow
    return product_dict

def CreateSalesForTime(sale_source):
  
    timeSalesDict = {}
    for row in sale_source:

        sale_date = row['timestamp'].split(" ")[0].split("-")
        key = f'{sale_date[0]}-{sale_date[1]}-{sale_date[2]}-{row["product_id"]}-{row["member_id"]}'
        if key not in timeSalesDict:

            row['quantity'] = 1
            timeSalesDict[key] = row
            timeSalesDict[key]['time_dim'] = CreateTimeDim(sale_date)

        else:
            timeSalesDict[key]['quantity'] += 1
    return timeSalesDict



def CreateTimeDim(sale_date):
    time_obj = {}
    time_obj['year'] = int(sale_date[0])
    time_obj['quarter'] = (int(sale_date[1])-1)//3 + 1
    time_obj['month'] = int(sale_date[1])
    time_obj['day'] = int(sale_date[2])
    time_obj['time_id'] = time_dim.ensure(time_obj)
    return time_obj
    




user_dict = CreateUserDim(member_source)



product_dict = CreateProductDim(product_source)

salesdict = CreateSalesForTime(sale_source)

for key in salesdict:

    sale = salesdict[key]
    if  sale['member_id'] not in user_dict or sale['product_id'] not in product_dict:
        continue
    sale['fk_time'] = sale['time_dim']['time_id']
    sale['fk_product'] = product_dict[sale['product_id']]['product_id']
    sale['fk_user'] =user_dict[sale['member_id']]['user_id']
    sale_fact.insert(sale)


dw_conn_wrapper.commit()

dw_conn_wrapper.close()

dw_conn.close()