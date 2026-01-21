# Databricks notebook source
try:
    import ipywidgets as widgets
    from IPython.display import display
except ImportError:
    # Fallback for environments where ipywidgets is not installed (e.g., local testing without deps)
    # This allows the class definition to be loaded if we mock widgets later or just inspect code.
    pass

class TableDropper:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.tables = []
        self.checkboxes = []

        # UI Components
        self.catalog_schema_input = widgets.Text(
            description='Catalog.Schema:',
            placeholder='main.default',
            style={'description_width': 'initial'}
        )
        self.load_btn = widgets.Button(description="Load Tables")
        self.load_btn.on_click(self.on_load_click)

        self.dry_run_checkbox = widgets.Checkbox(
            value=True,
            description='Dry Run (Print only)',
            indent=False
        )

        self.drop_btn = widgets.Button(description="Drop Selected Tables", button_style='danger')
        self.drop_btn.on_click(self.on_drop_click)
        self.drop_btn.disabled = True

        self.output = widgets.Output()
        self.table_list_box = widgets.VBox([])

    def display_ui(self):
        display(widgets.VBox([
            widgets.HBox([self.catalog_schema_input, self.load_btn]),
            self.table_list_box,
            widgets.HBox([self.dry_run_checkbox, self.drop_btn]),
            self.output
        ]))

    def get_tables(self, catalog_schema):
        try:
            # Unity Catalog expects catalog.schema
            # We explicitly check for rows to be safe against different return structures
            df = self.spark.sql(f"SHOW TABLES IN {catalog_schema}")
            return [row.tableName for row in df.collect()]
        except Exception as e:
            with self.output:
                print(f"Error listing tables: {e}")
            return []

    def on_load_click(self, b):
        self.output.clear_output()
        self.table_list_box.children = []
        self.drop_btn.disabled = True

        catalog_schema = self.catalog_schema_input.value.strip()
        if not catalog_schema:
            with self.output:
                print("Please enter a valid catalog.schema")
            return

        with self.output:
            print(f"Loading tables from {catalog_schema}...")

        tables = self.get_tables(catalog_schema)
        self.tables = tables

        if not tables:
            with self.output:
                print("No tables found or error occurred.")
            return

        self.checkboxes = [
            widgets.Checkbox(value=False, description=t, indent=False)
            for t in tables
        ]

        # Create a "Select All" checkbox
        self.select_all_cb = widgets.Checkbox(value=False, description="Select All", indent=False, style={'font_weight': 'bold'})
        self.select_all_cb.observe(self.on_select_all_change, names='value')

        self.table_list_box.children = (self.select_all_cb,) + tuple(self.checkboxes)
        self.drop_btn.disabled = False

        with self.output:
            print(f"Found {len(tables)} tables.")

    def on_select_all_change(self, change):
        for cb in self.checkboxes:
            cb.value = change['new']

    def on_drop_click(self, b):
        self.output.clear_output()
        catalog_schema = self.catalog_schema_input.value.strip()
        selected_tables = [cb.description for cb in self.checkboxes if cb.value]

        if not selected_tables:
            with self.output:
                print("No tables selected.")
            return

        is_dry_run = self.dry_run_checkbox.value

        with self.output:
            if is_dry_run:
                print("--- DRY RUN MODE ---")
                print(f"The following {len(selected_tables)} tables would be DROPPED:")
            else:
                print("--- EXECUTING DROP ---")

            for table in selected_tables:
                full_table_name = f"{catalog_schema}.{table}"
                if is_dry_run:
                    print(f"[Dry Run] DROP TABLE IF EXISTS {full_table_name};")
                else:
                    try:
                        self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                        print(f"Dropped: {full_table_name}")
                    except Exception as e:
                        print(f"Failed to drop {full_table_name}: {e}")

            print("Done.")

# Entry point for Databricks execution
if 'spark' in globals():
    app = TableDropper(spark)
    app.display_ui()
