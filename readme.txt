#MIRROR_DB SCRIPT
#this script connects to two databases compatible to Employees sample database from https://launchpad.net/test-db/
#and copies titles from source database to target database

#SCRIPT REQUIREMENTS
    # apt-get mysql-server
    apt-get build-dep python-mysqldb
    pip install MySQL-python==1.2.5
    pip install memory-profiler==0.41
    pip install mock==2.0.0
    pip install nose==1.3.7

#RUNNING THE TESTS
    nosetests tests/nose_tests.py

#RUNNING THE MIRROR_DB SCRIPT
    python mirror_db.py [db 1 user] [db 1 password] [host 1 adress/'localhost'] [db 1 name] [db 2 user] [db 2 password] [host 2 adress/'localhost'] [db 2 name]
    #Where db 1 is source database and db 2 is target database

    #Please write parameters as stings inside ''
    #for example

    python mirror_db.py 'root' 'vagrant' 'localhost' 'employees_1' 'root' 'vagrant' 'localhost' 'employees_2'

    #You can even use time command before python mirror_db to print execution time

    time python mirror_db.py 'root' 'vagrant' 'localhost' 'employees' 'root' 'vagrant' 'localhost' 'employees_2'

