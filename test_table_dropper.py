import sys
import unittest
from unittest.mock import MagicMock, patch

# Mocking ipywidgets and IPython.display modules
# We need to do this before importing table_dropper because it imports them at the top level
mock_widgets = MagicMock()
mock_ipython_display = MagicMock()
sys.modules['ipywidgets'] = mock_widgets
sys.modules['IPython.display'] = mock_ipython_display

# Now we can import the module
import table_dropper

class TestTableDropper(unittest.TestCase):
    def setUp(self):
        # Mock Spark Session
        self.mock_spark = MagicMock()
        self.app = table_dropper.TableDropper(self.mock_spark)

        # Setup common mock widget behaviors
        self.app.catalog_schema_input.value = "my_catalog.my_schema"
        self.app.dry_run_checkbox.value = True

        # Mock the Output widget context manager
        self.app.output = MagicMock()
        self.app.output.__enter__ = MagicMock()
        self.app.output.__exit__ = MagicMock()

    def test_get_tables(self):
        # Setup mock return for spark.sql query
        mock_df = MagicMock()
        row1 = MagicMock()
        row1.table_name = "table1"
        row1.created = "2023-01-01"
        row2 = MagicMock()
        row2.table_name = "table2"
        row2.created = "2023-01-02"
        mock_df.collect.return_value = [row1, row2]
        self.mock_spark.sql.return_value = mock_df

        tables = self.app.get_tables("my_catalog.my_schema")

        # Verify the query was constructed correctly
        # We clean whitespace for easier comparison or just check key parts
        actual_query = self.mock_spark.sql.call_args[0][0]
        self.assertIn("FROM my_catalog.information_schema.tables", actual_query)
        self.assertIn("WHERE table_schema = 'my_schema'", actual_query)
        self.assertIn("ORDER BY created ASC", actual_query)

        expected_tables = [
            {'name': "table1", 'created': "2023-01-01"},
            {'name': "table2", 'created': "2023-01-02"}
        ]
        self.assertEqual(tables, expected_tables)

    def test_on_load_click(self):
        # Mock get_tables to return some tables
        mock_data = [
            {'name': 't1', 'created': '2023-01-01'},
            {'name': 't2', 'created': '2023-01-02'}
        ]
        with patch.object(self.app, 'get_tables', return_value=mock_data) as mock_get_tables:
            # Reset the mock_widgets.Checkbox calls before this test action
            mock_widgets.Checkbox.reset_mock()

            self.app.on_load_click(None)

            mock_get_tables.assert_called_with("my_catalog.my_schema")
            # Should create 2 checkboxes + 1 select all
            self.assertEqual(len(self.app.checkboxes), 2)

            # Verify Checkbox calls
            # We expect 3 calls: 2 for tables + 1 for "Select All"
            calls = mock_widgets.Checkbox.call_args_list
            # Check for descriptions containing date
            t1_calls = [c for c in calls if c[1].get('description') == 't1 (2023-01-01)']
            t2_calls = [c for c in calls if c[1].get('description') == 't2 (2023-01-02)']

            self.assertEqual(len(t1_calls), 1)
            self.assertEqual(len(t2_calls), 1)

            # Verify UI update (table_list_box children)
            # It should have 3 children: Select All + 2 tables
            self.assertEqual(len(self.app.table_list_box.children), 3)

    def test_drop_dry_run(self):
        # Setup: loaded tables and selected some
        self.app.tables = [
            {'name': 't1', 'created': '2023-01-01'},
            {'name': 't2', 'created': '2023-01-02'}
        ]

        cb1 = MagicMock()
        cb1.description = "t1 (2023-01-01)"
        cb1.value = True

        cb2 = MagicMock()
        cb2.description = "t2 (2023-01-02)"
        cb2.value = False # Not selected

        self.app.checkboxes = [cb1, cb2]
        self.app.dry_run_checkbox.value = True # Dry Run

        self.app.on_drop_click(None)

        # In Dry Run, spark.sql should NOT be called for DROP
        # (It might have been called earlier for SHOW TABLES, so we check calls starting with DROP)

        # We can check that no 'DROP' commands were sent to spark.sql
        for call in self.mock_spark.sql.call_args_list:
            args, _ = call
            self.assertFalse(args[0].startswith("DROP"), f"Should not drop in dry run: {args[0]}")

    def test_drop_execution(self):
        # Setup: loaded tables and selected some
        self.app.tables = [
            {'name': 't1', 'created': '2023-01-01'},
            {'name': 't2', 'created': '2023-01-02'}
        ]

        cb1 = MagicMock()
        cb1.description = "t1 (2023-01-01)"
        cb1.value = True

        cb2 = MagicMock()
        cb2.description = "t2 (2023-01-02)"
        cb2.value = True

        self.app.checkboxes = [cb1, cb2]
        self.app.dry_run_checkbox.value = False # Real execution

        self.app.on_drop_click(None)

        # Check that spark.sql was called with DROP statements
        expected_calls = [
            ("DROP TABLE IF EXISTS my_catalog.my_schema.t1",),
            ("DROP TABLE IF EXISTS my_catalog.my_schema.t2",)
        ]

        # Filter mostly for the DROP calls
        actual_calls = [
            call[0] for call in self.mock_spark.sql.call_args_list
            if call[0][0].startswith("DROP")
        ]

        self.assertEqual(len(actual_calls), 2)
        self.assertEqual(actual_calls[0][0], "DROP TABLE IF EXISTS my_catalog.my_schema.t1")
        self.assertEqual(actual_calls[1][0], "DROP TABLE IF EXISTS my_catalog.my_schema.t2")

if __name__ == '__main__':
    unittest.main()
