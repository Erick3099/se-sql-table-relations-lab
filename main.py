import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

print("\n--- Part 1: Join and Filter ---")

q1 = """
SELECT firstName, lastName, jobTitle
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
WHERE o.city = 'Boston';
"""
print("\n1. Employees in Boston:")
print(pd.read_sql(q1, conn))



q2 = """
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
GROUP BY o.officeCode, o.city
HAVING COUNT(e.employeeNumber) = 0;
"""
print("\n2. Offices with zero employees:")
print(pd.read_sql(q2, conn))


print("\n--- Part 2: Type of Join ---")

q3 = """
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o
ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName;
"""
print("\n3. Employees with office info:")
print(pd.read_sql(q3, conn))

q4 = """
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName;
"""
print("\n4. Customers with no orders:")
print(pd.read_sql(q4, conn))


print("\n--- Part 3: Built-In Function ---")

q5 = """
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p
ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC;
"""
print("\n5. Customer payments:")
print(pd.read_sql(q5, conn))


print("\n--- Part 4: Joining and Grouping ---")


q6 = """
SELECT e.employeeNumber, e.firstName, e.lastName,
       COUNT(c.customerNumber) AS numCustomers
FROM employees e
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber, e.firstName, e.lastName
HAVING AVG(c.creditLimit) > 90000
ORDER BY numCustomers DESC;
"""
print("\n6. High-value employees:")
print(pd.read_sql(q6, conn))


q7 = """
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
GROUP BY p.productName
ORDER BY totalunits DESC;
"""
print("\n7. Product sales summary:")
print(pd.read_sql(q7, conn))


print("\n--- Part 5: Multiple Joins ---")

q8 = """
SELECT p.productName, p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
JOIN orders o
ON od.orderNumber = o.orderNumber
GROUP BY p.productName, p.productCode
ORDER BY numpurchasers DESC;
"""
print("\n8. Customers per product:")
print(pd.read_sql(q8, conn))


q9 = """
SELECT o.officeCode, o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
LEFT JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode, o.city;
"""
print("\n9. Customers per office:")
print(pd.read_sql(q9, conn))


print("\n--- Part 6: Subquery ---")
q10 = """
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord
ON c.customerNumber = ord.customerNumber
JOIN orderdetails od
ON ord.orderNumber = od.orderNumber
WHERE od.productCode IN (
    SELECT od2.productCode
    FROM orderdetails od2
    JOIN orders o2
    ON od2.orderNumber = o2.orderNumber
    GROUP BY od2.productCode
    HAVING COUNT(DISTINCT o2.customerNumber) < 20
);
"""
print("\n10. Employees selling low-performing products:")
print(pd.read_sql(q10, conn))

conn.close()

print("\n--- Done ---")