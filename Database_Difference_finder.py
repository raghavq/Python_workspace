import itertools
import mysql.connector

# Connect to the HO database
db1_conn  = mysql.connector.connect(
  host=" ",
  user=" ",
  password=" ",
  database=" "
)
db1_cursor = db1_conn.cursor()

# Connect to the Slave database
db2_conn = mysql.connector.connect(
  host=" ",
  user=" ",
  password=" ",
  database=" "
)
db2_cursor = db2_conn.cursor()

# Connect to the BI database
db3_conn = mysql.connector.connect(
  host=" ",
  user=" ",
  password=" ",
  database=" "
)
db3_cursor = db3_conn.cursor()

# Query and calculate total price from the HO database
db1_cursor.execute('SELECT')

rows1 = db1_cursor.fetchall()

# Query and calculate total price from the Slave database
db2_cursor.execute('SELECT')
rows = db2_cursor.fetchall()


db2_cursor.execute('SELECT')
rows2 = db2_cursor.fetchall()


result_union = set(rows1).union(set(rows)).union(set(rows2))

# Insert fetched data into the destination table
for row2 in result_union:
    print(row2[0])
    db3_cursor.execute("INSERT INTO ) VALUES (%s)", (row2[0]))

db3_cursor.execute('SELECT ')
rows=db3_cursor.fetchall()
for row2 in rows:
  print(row2)
  db3_cursor.execute("INSERT INTO ) VALUES (%s)", (row2[0]))


print("Inserted Successfully !!")

# Commit the changes
db3_conn.commit()

# Close the database connections
db1_conn.close()
db2_conn.close()
db3_conn.close()


