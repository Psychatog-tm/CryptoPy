import re
import sqlite3

conn = sqlite3.connect('tradedb2022.sqlite')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS Trades')
cur.execute('CREATE TABLE Trades (Date_UTC TEXT, OrderNo INTEGER, Pair TEXT, Type TEXT, Side TEXT, Order_Price REAL, Order_Amount TEXT, Time TEXT, Executed TEXT, Average_Price REAL, Trading_total TEXT, Status TEXT)')


fname = input('Enter file name: ')

fh = open(fname)

tradeset = set()
tradelist = list()
for line in fh:
    lines = line.split()
    x = re.findall('.*FILLED\"', line)
    if len(x) < 1 : continue
    for item in x:
       items = item.split(',')
       date_utc = items[0]
       orderno = items[1]
       pair = items[2]
       type = items[3]
       side = items[4]
       order_price = items[5]
       order_amount = items[6]
       time = items[7]
       executed = items[8]
       ave_price = items[9]
       trading_total = items[10]
       status = items[11]
       row = cur.fetchone()
       if row is None:
           cur.execute('INSERT INTO Trades (Date_UTC, OrderNo, Pair, Type, Side, Order_Price, Order_Amount, Time, Executed, Average_Price, Trading_total, Status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (date_utc, orderno, pair, type, side, order_price, order_amount, time, executed, ave_price, trading_total, status))
       else: continue
   
    tradelist.append(items)



    conn.commit()
cur.close()
print('-------------END---------------')
print('-------Check sqlite file in your folder-------')
