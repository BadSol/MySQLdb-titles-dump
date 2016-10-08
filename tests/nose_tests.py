import sys

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_false, assert_greater_equal, with_setup
from mock import MagicMock, patch, mock

import MySQLdb
from mirror_db import connect_to_database, check_database_compatibility, insert_titles_to_target_db, \
    copy_data_between_databases, validate_database, main


class Test_mirror_db:
    @classmethod
    def setup_class(self):
        self.database = MagicMock()
        self.database.cursor.return_value = self.cursor = MagicMock(name='mock cursor')

    def setup_cursor_for_compatibility_test(self):
        """
        Mocking compatible database
        """
        # simulating fetch_all method of mocked cursor

        def insert_execute(args):
            self.cursor.last_execute = args

        def fetch_all():
            if self.cursor.last_execute == "SHOW TABLES":
                return ('titles',), ('employees',)
            elif self.cursor.last_execute == "DESCRIBE titles":
                return (('emp_no', 'int(11)', 'NO', 'PRI', None, ''),
                        ('title', 'varchar(50)', 'NO', 'PRI', None, ''),
                        ('from_date', 'date', 'NO', 'PRI', None, ''),
                        ('to_date', 'date', 'YES', '', None, ''))

        self.cursor.execute.side_effect = insert_execute
        self.cursor.fetchall.side_effect = fetch_all
        self.cursor.last_execute = None

    def test_connect_to_database(self):
        """
        Asserting database object return when connect_to_database doesn't fail
        """
        with patch.object(MySQLdb, 'connect') as self.database:
            database = connect_to_database(user='test', password='test', db_name='test', host='localhost')

            assert_not_equal(database, None)

    # def test_connect_to_database_when_exception_is_raised(self):
    #     """
    #     Asserting method returns None when MySQLdb.Error is raised
    #     """
    #     with patch.object(MySQLdb, 'connect') as mock_db:
    #         # todo: MySQLdb.connect is mocked, but doesn't raise exc. At target location
    #         mock_db.connect.side_effect = MySQLdb.Error()
    #         with patch.object(sys, 'exit') as sys_mock:
    #             database = connect_to_database(user='test', password='test', db_name='test', host='localhost')
    #
    #             assert_equals(database, None)

    @with_setup(setup_cursor_for_compatibility_test)
    def test_check_database_compatibility(self):
        """
        Asserting method successfully validates database and returns True when database is compatible
        """
        result = check_database_compatibility(self.database)

        assert_true(result)

    def test_check_database_compatibility_fail(self):
        """
        Asserting method fails database validation and returns False
        """
        database = self.database
        database.cursor.return_value = cursor = MagicMock()
        cursor.fetchall.return_value = ('wrong', 'data')
        result = check_database_compatibility(database)

        assert_false(result)

    def test_insert_titles_to_target_db(self):
        """
        Asserting that executemany is called with right parameters
        """
        cursor = self.cursor
        cursor.executemany.return_value = mock.Mock

        list_of_titles = (('a', 'b', 'c', 'd'), ('e', 'f', 'g', 'h'))

        # test if method execute 'executemany' call with "correct" values

        insert_titles_to_target_db(list_of_titles, cursor)

        cursor.executemany.\
            assert_called_with('INSERT INTO titles (emp_no, title, from_date, to_date) VALUES (%s, %s, %s, %s)',
                               list(list_of_titles))

    def test_validate_database_when_object_is_none(self):
        """
        Asserting sys.exit when validated database is None
        """
        with patch.object(sys, 'exit') as sys_mock:
            validate_database(None, 'Test_database')

            assert_true(sys_mock.called)

    def test_validate_database_when_not_compatible(self):
        """
        asserting sys.exit when validated database isn't compatible
        """
        with patch.object(sys, 'exit') as sys_mock:
            with mock.patch('mirror_db.check_database_compatibility') as check_mock:
                check_mock.return_value = False
                validate_database(self.database, 'Test_database')

                assert_true(sys_mock.called)

    def test_main_fail_with_bad_parameters(self):
        """
        asserting sys.exit when method isn't called with exactly 8 parameters
        """
        with patch.object(sys, 'exit') as sys_mock:
            main(('a', 'b', 'c'))

            assert_true(sys_mock.called)

    def test_main_with_correct_number_of_parameters(self):
        """
        Asserting that sys.exit won't occur with correct number of parameters
        """
        with mock.patch('mirror_db.connect_to_database'):
            with mock.patch('mirror_db.validate_database'):
                with mock.patch('mirror_db.link_databases'):
                    with patch.object(sys, 'exit') as sys_mock:
                        main(('1', '2', '3', '4', '5', '6', '7', '8'))

                        assert_false(sys_mock.called)

    def test_copy_data_between_databases(self):
        """
        Testing method behaviour with different sets of titles
        """
        # executing without titles in database won't call insert_titles_to_target_db method
        with mock.patch('mirror_db.insert_titles_to_target_db') as mock_insert_method:
            # asserting that insert_titles_to_target_db won't be called when there are no more titles

            self.cursor.fetchmany.return_value = ()
            copy_data_between_databases(self.cursor, None, 5)  # in this case target database cursor can be None

            assert_false(mock_insert_method.called)

            # executing with less titles in source database than limit for this method

            mock_insert_method.reset_mock()
            self.cursor.fetchmany.side_effect = ['title', None]

            copy_data_between_databases(self.cursor, None, 5)

            assert_equals(mock_insert_method.call_count, 1)

            # executing with more titles in source database than limit for this method

            mock_insert_method.reset_mock()
            self.cursor.fetchmany.side_effect = ['title_list1', 'title_list2', 'title_list3', None]

            copy_data_between_databases(self.cursor, None, 5)

            assert_greater_equal(mock_insert_method.call_count, 2)

