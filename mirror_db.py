import sys

from memory_profiler import profile

import MySQLdb
import MySQLdb.cursors


def main(argv):
    """
    Main body of the script, connect to databases and call link_databases method
    :param argv: Parameters sent with script execution
    """
    if len(argv) != 8:
        sys.exit("Arguments are not correct, please execute this script with following arguments: "
                 "[db 1 user] [db 1 password] [host 1 adress/'localhost'] [db 1 name] "
                 "[db 2 user] [db 2 password] [host 2 adress/'localhost'] [db 2 name]")
    else:
        user_1 = argv[0]
        password_1 = argv[1]
        host_1 = argv[2]
        db_name_1 = argv[3]
        user_2 = argv[4]
        password_2 = argv[5]
        host_2 = argv[6]
        db_name_2 = argv[7]
        limit_of_rows = 100000  # This variable impacts memory usage

        database_1 = connect_to_database(user=user_1, password=password_1, db_name=db_name_1, host=host_1)
        validate_database(database_1, db_name_1)
        database_2 = connect_to_database(user=user_2, password=password_2, db_name=db_name_2, host=host_2)
        validate_database(database_2, db_name_2)

        link_databases(database_1, database_2, limit_of_rows)


def validate_database(database, db_name):
    """
    This method exist to assure that database object initiated properly and is compatible with this algorithm
    If any of those conditions aren't fulfilled, script execution is aborted
    :param database:
    :param db_name:
    :return:
    """
    if not database:
        sys.exit("Connecting to {} failed".format(db_name))
    elif not check_database_compatibility(database):
        sys.exit("Database: {} is not compatible for this operation".format(db_name))


def link_databases(database_1, database_2, number_of_rows):
    """
    Initialize cursor objects, sets off target database autocommit
    :param database_1:
    :param database_2:
    :param number_of_rows: This integer determine how big chunks of data are used at once. Impacts memory usage
    """
    cursor_1 = database_1.cursor()
    cursor_2 = database_2.cursor()

    try:
        cursor_2.execute("SET autocommit=0")  # set session autocommit off

    except MySQLdb.Error, e:
        print "MySQL set autocommit Error [%d]: %s" % (e.args[0], e.args[1])

    copy_data_between_databases(cursor_1, cursor_2, number_of_rows)
    database_2.commit()
    print 'Operation was successful!'


# @profile
def copy_data_between_databases(db_cursor_source, db_cursor_target, limit):
    """
    This method is responsible of copying data from source to target database
    :param db_cursor_source:
    :param db_cursor_target:
    :param limit: Value of titles inserted at once
    """
    db_cursor_source.execute("SELECT * FROM titles")
    while True:
        titles = db_cursor_source.fetchmany(limit)
        if not titles:
            break

        insert_titles_to_target_db(titles, db_cursor_target)


def insert_titles_to_target_db(titles, target_db_cursor):
    """
    Inserts titles to target database from list
    :param titles: list of titles data rows
    :param target_db_cursor:
    :return:
    """
    try:
        stmt = "INSERT INTO titles (emp_no, title, from_date, to_date) VALUES (%s, %s, %s, %s)"
        target_db_cursor.executemany(stmt, list(titles))

    except MySQLdb.Error, e:
        print "MySQL Titles Insert Error [%d]: %s" % (e.args[0], e.args[1])


def connect_to_database(user, password, db_name='', host='localhost'):
    """
    This methods connects to database using MySQLdb library, and if successful returns MySQLdb database object.
    Otherwise prints Error message and returns None
    :param user: MySQL database user
    :param password: MySQL database password
    :param db_name: MySQL database name
    :param host: MySQL database host adress, if blank uses localhost
    :return: MySQLdb database object
    """
    database = None
    try:
        database = MySQLdb.connect(user=user, passwd=password, db=db_name, host=host,
                                   cursorclass=MySQLdb.cursors.SSCursor)  # using SSCursor limits memory consumption

    except MySQLdb.Error, e:
        print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
    return database


def check_database_compatibility(database):
    """
    Checks if given database is compatible with Employees sample database from https://launchpad.net/test-db/
    :param database:
    :return True or False, depending on database compatibility:
    """
    c = database.cursor()
    c.execute("SHOW TABLES")

    if ('titles',) and ('employees',) not in c.fetchall():
        return False

    c.execute("DESCRIBE titles")
    if (
            'emp_no', 'int(11)', 'NO', 'PRI', None, '') and \
            ('title', 'varchar(50)', 'NO', 'PRI', None, '') and \
            ('from_date', 'date', 'NO', 'PRI', None, '') and \
            ('to_date', 'date', 'YES', '', None, '') not in c.fetchall():

        return False
    return True


if __name__ == '__main__':
    main(sys.argv[1:])
