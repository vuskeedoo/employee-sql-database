import sqlite3
from sqlite3 import Error
from pathlib import Path

#
# Create connection with database
#
def create_connection(db_file):
	""" create a database connection to a database that resides
	in the memory
	"""
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)
	return None

#
# Create_table function takes in SQL string to create table
#
def create_table(conn, create_table_sql):
	""" create a table from the create_table_sql statement
	:param conn: Connection object
	:param create_table_sql: a CREATE TABLE statement
	:return:
	"""
	try:
		c = conn.cursor()
		c.execute(create_table_sql)
	except Error as e:
		print(e)
#
# Main create table function creates all tables in database
#
def main_create_table(conn):
	sql_create_employee = """CREATE TABLE EMPLOYEE (
								Fname		VARCHAR(15)	NOT NULL,
								Minit		CHAR,
								Lname		VARCHAR(15)	NOT NULL,
								Ssn		CHAR(9)		NOT NULL,
								Bdate		DATE,
								Address		VARCHAR(30),
								Sex		CHAR,
								Salary		DECIMAL(10,2),
								Super_ssn	CHAR(9),
								Dno		INT		NOT NULL,
								PRIMARY KEY (Ssn)
							);"""
	sql_create_department = """CREATE TABLE DEPARTMENT (
							Dname		VARCHAR(15)	NOT NULL,
							Dnumber		INT		NOT NULL,
							Mgr_ssn		CHAR(9)		NOT NULL,
							Mgr_start_date	DATE,
							PRIMARY KEY (Dnumber),
							UNIQUE (Dname),
							FOREIGN KEY (Mgr_ssn) REFERENCES EMPLOYEE(Ssn)
						);"""

	sql_create_dept_locations = """CREATE TABLE DEPT_LOCATIONS (
								Dnumber		INT		NOT NULL,
								Dlocation 	VARCHAR(15)	NOT NULL,
								PRIMARY KEY (Dnumber, Dlocation),
								FOREIGN KEY (Dnumber) REFERENCES DEPARTMENT (Dnumber)
							);"""
	sql_create_project = """CREATE TABLE PROJECT (
							Pname		VARCHAR(15)	NOT NULL,
							Pnumber		INT		NOT NULL,
							Plocation	VARCHAR(15),
							Dnum		INT		NOT NULL,
							PRIMARY KEY (Pnumber),
							UNIQUE (Pname),
							FOREIGN KEY (Dnum) REFERENCES DEPARTMENT (Dnumber)
						);"""
	sql_create_works_on = """CREATE TABLE WORKS_ON (
							Essn	CHAR(9)	NOT NULL,
							Pno	INT	NOT NULL,
							Hours	DECIMAL(3,1),
							PRIMARY KEY (Essn, Pno),
							FOREIGN KEY (Essn) REFERENCES EMPLOYEE (Ssn),
							FOREIGN KEY (Pno) REFERENCES PROJECT (Pnumber)
						);"""
	sql_create_dependent = """CREATE TABLE DEPENDENT (
							Essn	CHAR(9)	NOT NULL,
							Dependent_name	VARCHAR(15)	NOT NULL,
							Sex	CHAR,
							Bdate	DATE,
							Relationship	VARCHAR(8),
							PRIMARY KEY (Essn, Dependent_name),
							FOREIGN KEY (Essn) REFERENCES EMPLOYEE (Ssn)
						);"""

	if conn is not None:
		create_table(conn, sql_create_employee)
		create_table(conn, sql_create_department)
		create_table(conn, sql_create_dept_locations)
		create_table(conn, sql_create_project)
		create_table(conn, sql_create_works_on)
		create_table(conn, sql_create_dependent)
		print('SQL table created successfully...')
	else:
		print("Error! Failed to create database.")

###########################################
#
# Function to insert employee into table
#
###########################################
def create_employee(conn, employee):
	sql = ''' INSERT INTO EMPLOYEE(Fname,Minit,Lname,Ssn,Bdate,Address,Sex,
				Salary,Super_ssn,Dno) VALUES(?,?,?,?,?,?,?,?,?,?) '''

	cur = conn.cursor()
	cur.execute(sql, employee)
	return cur.lastrowid

###########################################
#
# Function to insert department into table
#
###########################################
def create_department(conn, department):
	sql = ''' INSERT INTO DEPARTMENT(Dname,Dnumber,Mgr_ssn,Mgr_start_date) 
				VALUES(?,?,?,?)'''
	cur = conn.cursor()
	cur.execute(sql, department)
	return cur.lastrowid

###########################################
#
# Function to insert dept locations into table
#
###########################################
def create_dept_locations(conn, dept_locations):
	sql = ''' INSERT INTO DEPT_LOCATIONS(Dnumber,Dlocation) VALUES(?,?) '''

	cur = conn.cursor()
	cur.execute(sql, dept_locations)
	return cur.lastrowid

###########################################
#
# Function to insert project into table
#
###########################################
def create_project(conn, project):
	sql = ''' INSERT INTO PROJECT(Pname,Pnumber,Plocation,Dnum) VALUES(?,?,?,?) '''

	cur = conn.cursor()
	cur.execute(sql, project)
	return cur.lastrowid

###########################################
#
# Function to insert works_on into table
#
###########################################
def create_works_on(conn, works_on):
	sql = ''' INSERT INTO WORKS_ON(Essn,Pno,Hours) VALUES(?,?,?) '''

	cur = conn.cursor()
	cur.execute(sql, works_on)
	return cur.lastrowid

###########################################
#
# Function to insert dependent into table
#
###########################################
def create_dependent(conn, dependent):
	sql = ''' INSERT INTO DEPENDENT (Essn,Dependent_name,Sex,Bdate,Relationship) VALUES(?,?,?,?,?) '''

	cur = conn.cursor()
	cur.execute(sql, dependent)
	return cur.lastrowid

###########################################
#
# Function to insert data into tables
#
###########################################
def main_insert_data(conn):
	if conn is not None:
		employee = ("John", "B", "Smith", 123456789, "1965-01-09", 
			"731 Fondren, Houston, TX", "M", 30000, 333445555, 5);
		employee2 = ("Franklin", "T", "Wong", 333445555, "1955-12-08",
			"683 Voss, Houston, TX", "M", 40000, 888665555, 5);
		employee3 = ("Alicia", "J", "Zelaya", 999887777, "1968-01-19", 
			"3321 Castle, Spring, TX", "F", 25000, 987654321, 4);
		employee4 = ("Jennifer", "S", "Wallace", 987654321, "1941-06-20", 
			"291 Berry, Bellaire, TX", "F", 43000, 888665555, 4);
		employee5 = ("Ramesh", "K", "Narayan", 666884444, "1962-09-15", 
			"975 Fire Oak, Humble, TX", "M", 38000, 333445555, 5);
		employee6 = ("Joyce", "A", "English", 453453453, "1972-07-31", 
			"5631 Rice, Houston, TX", "F", 25000, 333445555, 5);
		employee7 = ("Ahmad", "V", "Jabbar", 987987987, "1969-03-29", 
			"980 Dallas, Houston, TX", "M", 25000, 987654321, 4);
		employee8 = ("James", "E", "Borg", 888665555, "1937-11-10", 
			"450 Stone, Houston, TX", "M", 55000, None, 1);
		employee9 = ("Hillbilly", "E", "Smith", 111122222, "1947-03-10", 
			"420 Stoner, Houston, TX", "M", 99000, 987654321, 1);
		employee10 = ("Franko", "E", "Smith", 424242424, "1947-04-10", 
			"420 Stoner, Houston, TX", "M", 92200, 987654321, 1);
		employee11 = ("Jennifer", "S", "Wallace", 987654311, "1941-06-20",
			"291 Berry, Bellaire, TX", "F", 343021, 888665555, 4);

		create_employee(conn, employee)
		create_employee(conn, employee2)
		create_employee(conn, employee3)
		create_employee(conn, employee4)
		create_employee(conn, employee5)
		create_employee(conn, employee6)
		create_employee(conn, employee7)
		create_employee(conn, employee8)
		create_employee(conn, employee9)
		create_employee(conn, employee10)
		create_employee(conn, employee11)

		department = ("Research", 5, 333445555, "1988-05-22");
		department2 = ("Administration", 4, 987654321, "1995-01-01");
		department3 = ("Headquarters", 1, 888665555, "1981-06-19");

		create_department(conn, department)
		create_department(conn, department2)
		create_department(conn, department3)
		
		dept_locations = (1, "Houston");
		dept_locations2 = (4, "Stafford");
		dept_locations3 = (5, "Bellaire");
		dept_locations4 = (5, "Sugarland");
		dept_locations5 = (5, "Houston");

		create_dept_locations(conn, dept_locations)
		create_dept_locations(conn, dept_locations2)
		create_dept_locations(conn, dept_locations3)
		create_dept_locations(conn, dept_locations4)
		create_dept_locations(conn, dept_locations5)

		project = ("ProductX", 1, "Bellaire", 5);
		project2 = ("ProductY", 2, "Sugarland", 5);
		project3 = ("ProductZ", 3, "Houston", 5);
		project4 = ("Computerization", 10, "Stafford", 4);
		project5 = ("Reorganization", 20, "Houston", 1);
		project6 = ("Newbenefits", 30, "Stafford", 4);

		create_project(conn, project)
		create_project(conn, project2)
		create_project(conn, project3)
		create_project(conn, project4)
		create_project(conn, project5)
		create_project(conn, project6)

		works_on = (123456789, 1, 32.5);
		works_on2 = (123456789, 2, 7.5);
		works_on3 = (666884444, 3, 40.0);
		works_on4 = (435435435, 1, 20.0);
		works_on5 = (435435435, 2, 20.0);
		works_on6 = (333445555, 2, 10.0);
		works_on7 = (333445555, 3, 10.0);
		works_on8 = (333445555, 10, 10.0);
		works_on9 = (333445555, 20, 10.0);
		works_on10 = (999887777, 30, 30.0);
		works_on11 = (999887777, 10, 10.0);
		works_on12 = (987987987, 10, 35.0);
		works_on13 = (987987987, 30, 5.0);
		works_on14 = (987654321, 30, 20.0);
		works_on15 = (987654321, 20, 15.0);
		works_on16 = (888665555, 20, None);

		create_works_on(conn, works_on)
		create_works_on(conn, works_on2)
		create_works_on(conn, works_on3)
		create_works_on(conn, works_on4)
		create_works_on(conn, works_on5)
		create_works_on(conn, works_on6)
		create_works_on(conn, works_on7)
		create_works_on(conn, works_on8)
		create_works_on(conn, works_on9)
		create_works_on(conn, works_on10)
		create_works_on(conn, works_on11)
		create_works_on(conn, works_on12)
		create_works_on(conn, works_on13)
		create_works_on(conn, works_on14)
		create_works_on(conn, works_on15)
		create_works_on(conn, works_on16)

		dependent = (333445555, "Alice", "F", "1986-04-05", "Daughter");
		dependent2 = (333445555, "Theodore", "M", "1983-10-25", "Son");
		dependent3 = (333445555, "Joy", "F", "1958-05-03", "Spouse");
		dependent4 = (987654321, "Abner", "M", "1942-02-28", "Spouse");
		dependent5 = (123456789, "Michael", "M", "1988-01-04", "Son");
		dependent6 = (12345789, "Alice", "F", "1988-12-30", "Daughter");
		dependent7 = (123456789, "Elizabeth", "F", "1967-05-05", "Spouse");

		create_dependent(conn, dependent)
		create_dependent(conn, dependent2)
		create_dependent(conn, dependent3)
		create_dependent(conn, dependent4)
		create_dependent(conn, dependent5)
		create_dependent(conn, dependent6)
		create_dependent(conn, dependent7)

		conn.commit()
		print('SQL data inserted successfully...')
	else:
		print('Error! Failed to insert data into table.')

'''Find supervisees at all levels: In this option, the user is prompted 
for the last name of an employee. If there are several employees with 
the same last name, the user is presented with a list of social security 
numbers of employees with the same last name and asked to choose one. The 
program then proceeds to list all the supervisees of the employee and all 
levels below him or her in the employee hierarchy.'''
def getSupervisee(conn):
	lastName = input("Enter last name of employee: ")
	sql = "SELECT Lname, Fname, Ssn, Super_ssn FROM EMPLOYEE WHERE Lname = '"+lastName+"';"
	cur = conn.cursor()
	cur.execute(sql)
	rows = cur.fetchall()
	cur.close()
	for row in rows:
		print(row[0]+", "+row[1]+" "+str(row[2]))
	#print(rows[0][2])

	# If multiple last names, prompt social security to pick
	findSsn = input("Select a social security from list: ")
	for row in rows:
		if row[2] == findSsn:
			super_ssn = row[3]
	sql = "SELECT Lname, Fname, Ssn, Super_ssn FROM EMPLOYEE WHERE Super_ssn = "+super_ssn+";"
	cur = conn.cursor()
	cur.execute(sql)
	rows = cur.fetchall()
	print('SUPERVISEES')
	print('LNAME   FNAME   SSN')
	print('-------------------------------------')
	for row in rows:
		print(row[0]+" "+row[1]+" "+str(row[2]))

'''Find the top 5 highest paid employees: In this option, the program 
finds five employees who rank in the top 5 in salary and lists them.'''
def getHighestPaid(conn):
	cur = conn.cursor()
	cur.execute("SELECT Ssn, Lname, Fname, Salary FROM EMPLOYEE ORDER BY Salary DESC LIMIT 5;")
	rows = cur.fetchall()
	print("HIGHEST PAID WORKERS")
	print("SSN   LNAME   FNAME   SALARY")
	print("-------------------------------------")
	for r in rows:
		print(str(r[0])+" "+r[1]+" "+r[2]+" "+str(r[3]))

'''Find the top 5 highest worked employees: In this option, the program 
finds five employees who rank in the top 5 in number of hours worked and 
lists them.'''
def getMostWorked(conn):
	cur = conn.cursor()
	cur.execute("SELECT Ssn, Lname, Fname, sum(WORKS_ON.Hours) FROM EMPLOYEE, WORKS_ON WHERE EMPLOYEE.Ssn = WORKS_ON.Essn GROUP BY Ssn ORDER BY WORKS_ON.Hours desc LIMIT 5;")
	rows = cur.fetchall()
	print("MOST WORKED WORKERS")
	print("SSN   LNAME   FNAME")
	print("-------------------------------------")
	for r in rows:
		print(str(r[0])+" "+r[1]+" "+r[2]+" "+str(r[3]))

# Print menu
def printMenu():
	global conn
	while True:
		print("* * * * * * * * * * * * * * *")
		print("QUERY OPTIONS")
		print("(a) Find supervisees at all levels.")
		print("(b) Find highest paid workers.")
		print("(c) Find the most worked workers.")
		print("(q) Quit.")
		option = input("Type in your option: ")
		if option == 'a':
			getSupervisee(conn)
			pass
		if option == 'b':
			getHighestPaid(conn)
			pass
		if option == 'c':
			getMostWorked(conn)
			pass
		if option == 'q':
			print("\nExiting program...BYE!")
			exit(1)
############################################
#
# Main function
#
############################################
def main():
	database = "company.db"
	global conn
	conn = create_connection(database)

	try:
		main_create_table(conn)
		main_insert_data(conn)
	except Error as e:
		print('Database exists...')

	print('\n')
	username = input("Enter a username: ")
	password = input("Enter a password: ")
	print('\n')
	printMenu()

# Run Program
if __name__ == "__main__":
	main()