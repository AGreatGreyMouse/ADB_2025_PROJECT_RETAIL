import pandas as pd
import numpy as np
import os
import sys
import itertools

project_path = os.path.abspath(os.path.join('..'))

if project_path not in sys.path:
    sys.path.append(project_path)

class DQ:
    """Main class for running data quality checks on input tables"""
    
    def __init__(self, check_id,
                 check_name, client,
                 input_tables, th_values,
                 lvl_data, data_path
                ):
        # Store config parameters
        self.check_id = check_id
        self.check_name = check_name
        self.client = client
        self.input_tables = input_tables
        self.th_values = th_values
        self.lvl_data = lvl_data
        self.data_path = data_path
        # This will hold all data quality issues found
        self.data_quality_output = pd.DataFrame()
        

    def check_val_range(self, tables, th=0):
        """Check for values outside acceptable ranges (e.g. negative prices)"""
        
        for table_name, target_col in tables:
            try:
                # Load the table from CSV
                table = pd.read_csv(self.data_path + table_name + '.csv')
                
                # Skip if the column doesn't exist
                if target_col not in table.columns:
                    print(f"Warning: Column {target_col} not found in {table_name}")
                    continue
                
                # Filter rows where value is below threshold
                bad_rows = table[table[target_col] < th].copy()
                
                if len(bad_rows) > 0:
                    # Tag each row with check metadata
                    bad_rows['INPUT_COLUMN'] = target_col
                    bad_rows['INPUT_TABLE'] = table_name
                    bad_rows['INPUT_VALUE'] = th
                    bad_rows['WARNING_TYPE'] = 'val_range'
                    bad_rows['WARNING'] = f'Value {target_col}={bad_rows[target_col].iloc[0]:.2f} is below threshold {th} in {table_name}'
                    
                    # Append to output
                    self.data_quality_output = pd.concat([self.data_quality_output, bad_rows], ignore_index=True)
                    
            except Exception as e:
                print(f"Error checking {table_name}: {str(e)}")
                continue
        
        return self.data_quality_output
    
    
    def check_cross_consistency(self, tables):
        """Make sure IDs in one table exist in related tables"""
        
        # Generate all unique table pairs (A->B and B->A separately)
        table_pairs = list(itertools.permutations(tables, 2))
        
        for table1_name, table2_name in table_pairs:
            try:
                # Load both tables
                table1 = pd.read_csv(self.data_path + table1_name + '.csv')
                table2 = pd.read_csv(self.data_path + table2_name + '.csv')
                
                # Find ID columns that exist in both tables
                all_cols_table1 = set(table1.columns)
                all_cols_table2 = set(table2.columns)
                common_id_cols = [col for col in all_cols_table1 & all_cols_table2 
                                  if 'ID' in col.upper()]
                
                # Skip this pair if no common ID columns
                if not common_id_cols:
                    continue
                
                # Get unique ID combinations from each table
                unique_ids_table1 = table1[common_id_cols].drop_duplicates()
                unique_ids_table2 = table2[common_id_cols].drop_duplicates()
                
                # Find IDs in table1 that don't exist in table2
                merged = unique_ids_table1.merge(
                    unique_ids_table2,
                    on=common_id_cols,
                    how='left',
                    indicator=True
                )
                
                # Get the orphaned records
                orphaned = merged[merged['_merge'] == 'left_only'].copy()
                
                if len(orphaned) > 0:
                    # Drop the merge indicator column
                    orphaned = orphaned.drop('_merge', axis=1)
                    
                    # Add metadata for each orphaned record
                    orphaned['INPUT_TABLE'] = f'{table1_name} && {table2_name}'
                    orphaned['WARNING_TYPE'] = 'cross_consistency'
                    orphaned['WARNING'] = f'IDs from {table1_name} not found in {table2_name}'
                    
                    # Append to output
                    self.data_quality_output = pd.concat([self.data_quality_output, orphaned], ignore_index=True)
                    
            except Exception as e:
                print(f"Error checking {table1_name} vs {table2_name}: {str(e)}")
                continue
        
        return self.data_quality_output
    
    def check_time_cross_consistency(self, tables, th):
        """Check if product-location pairs exist across time periods in related tables"""
        
        for table1_name, table2_name in tables:
            try:
                # Load both tables
                table1 = pd.read_csv(self.data_path + table1_name + '.csv')
                table2 = pd.read_csv(self.data_path + table2_name + '.csv')
                
                # Find common columns between tables
                common_columns = set(table1.columns) & set(table2.columns)
                
                # Extract ID columns (PRODUCT_ID, LOCATION_ID)
                id_columns = [col for col in common_columns 
                             if 'ID' in col.upper() and 
                             ('PRODUCT' in col.upper() or 'LOCATION' in col.upper())]
                
                # Extract date columns (any column ending with _DT)
                date_columns = [col for col in common_columns if col.endswith('_DT')]
                
                # Need both IDs and dates for this check
                if not id_columns or not date_columns:
                    continue
                
                # Combine for composite key
                key_columns = id_columns + date_columns
                
                # Get unique combinations from each table
                unique_keys_table1 = table1[key_columns].drop_duplicates()
                unique_keys_table2 = table2[key_columns].drop_duplicates()
                
                # Find combinations in table1 missing from table2
                merged = unique_keys_table1.merge(
                    unique_keys_table2,
                    on=key_columns,
                    how='left',
                    indicator=True
                )
                
                missing_records = merged[merged['_merge'] == 'left_only'].copy()
                
                if len(missing_records) > 0:
                    # Drop merge indicator
                    missing_records = missing_records.drop('_merge', axis=1)
                    
                    # Calculate what percentage of records are missing
                    total_records = len(unique_keys_table1)
                    missing_count = len(missing_records)
                    missing_pct = (missing_count / total_records * 100) if total_records > 0 else 0
                    
                    # Add metadata
                    missing_records['INPUT_TABLE'] = f'{table1_name} && {table2_name}'
                    missing_records['INPUT_VALUE'] = th
                    missing_records['WARNING_TYPE'] = 'time_cross_consistency'
                    missing_records['WARNING'] = f'{missing_count} records ({missing_pct:.1f}%) from {table1_name} missing in {table2_name}'
                    
                    # Only add if missing percentage exceeds threshold
                    if missing_pct > th:
                        self.data_quality_output = pd.concat([self.data_quality_output, missing_records], ignore_index=True)
                
                # Also check for infrequent occurrences
                # Group by ID columns only (without date) to see frequency over time
                if len(id_columns) > 0:
                    # Count how many date periods each ID combo appears in table1
                    id_date_counts = table1.groupby(id_columns)[date_columns[0]].nunique().reset_index()
                    id_date_counts.columns = id_columns + ['period_count']
                    
                    # Find IDs that appear in very few periods (below threshold)
                    infrequent = id_date_counts[id_date_counts['period_count'] <= th].copy()
                    
                    if len(infrequent) > 0:
                        infrequent = infrequent.drop('period_count', axis=1)
                        infrequent['INPUT_TABLE'] = f'{table1_name}'
                        infrequent['INPUT_VALUE'] = th
                        infrequent['WARNING_TYPE'] = 'time_cross_consistency'
                        infrequent['WARNING'] = f'ID appears in {th} or fewer time periods in {table1_name}'
                        
                        self.data_quality_output = pd.concat([self.data_quality_output, infrequent], ignore_index=True)
                        
            except Exception as e:
                print(f"Error in time consistency check for {table1_name} vs {table2_name}: {str(e)}")
                continue
        
        return self.data_quality_output
    
    
    def format_output(self, lvl_data):
        """Transform IDs to proper hierarchy format for output"""
        
        if self.data_quality_output.empty:
            return
        
        # Process each dimension (LOCATION, PRODUCT, CUSTOMER, DISTR_CHANNEL)
        for dimension_name, hierarchy_table_name in lvl_data.items():
            id_col_name = f'{dimension_name}_ID'
            
            # Skip if this dimension doesn't exist in output
            if id_col_name not in self.data_quality_output.columns:
                continue
            
            try:
                # Load the hierarchy table to understand structure
                hierarchy_table = pd.read_csv(self.data_path + hierarchy_table_name + '.csv')
                
                # Count how many levels exist (e.g., PRODUCT_LVL_ID1, PRODUCT_LVL_ID2, etc.)
                level_cols = [col for col in hierarchy_table.columns if f'{dimension_name}_LVL_ID' in col]
                num_levels = len(level_cols)
                
                # The ID column represents the lowest (most detailed) level
                # So if we have 5 levels, this ID is level 6
                target_level = num_levels + 1
                
                # Create the properly named level column
                self.data_quality_output[f'{dimension_name}_LVL_ID{target_level}'] = \
                    self.data_quality_output[id_col_name].astype('Int64')
                
                # Store which level this represents
                self.data_quality_output[f'{dimension_name}_LVL'] = target_level
                
                # Drop the original ID column (we've renamed it)
                self.data_quality_output = self.data_quality_output.drop(id_col_name, axis=1)
                
            except Exception as e:
                print(f"Warning: Could not format {dimension_name}: {str(e)}")
                continue
            
    
    def check(self):
        """Run all data quality checks in sequence"""
        
        print(f"Starting data quality checks for: {self.check_name}")
        print("=" * 60)
        
        # Reset output to start fresh
        self.data_quality_output = pd.DataFrame()
        
        # STEP 1: Check for invalid values (negative prices, quantities, etc.)
        print("\n[1/3] Checking value ranges...")
        if 'val_range' in self.input_tables and 'val_range' in self.th_values:
            initial_count = len(self.data_quality_output)
            self.check_val_range(
                self.input_tables['val_range'],
                self.th_values['val_range']
            )
            found = len(self.data_quality_output) - initial_count
            print(f"      Found {found} value range issues")
        
        # STEP 2: Check that IDs exist across related tables
        print("\n[2/3] Checking cross-table consistency...")
        if 'cross_consistency' in self.input_tables:
            initial_count = len(self.data_quality_output)
            self.check_cross_consistency(self.input_tables['cross_consistency'])
            found = len(self.data_quality_output) - initial_count
            print(f"      Found {found} cross-consistency issues")
        
        # STEP 3: Check temporal consistency (same IDs across time periods)
        print("\n[3/3] Checking time-based consistency...")
        if 'time_cross_consistency' in self.input_tables and 'time_cross_consistency' in self.th_values:
            initial_count = len(self.data_quality_output)
            self.check_time_cross_consistency(
                self.input_tables['time_cross_consistency'],
                self.th_values['time_cross_consistency']
            )
            found = len(self.data_quality_output) - initial_count
            print(f"      Found {found} time-consistency issues")
        
        # STEP 4: Format the output to match expected structure
        print("\n[4/4] Formatting output...")
        self.format_output(self.lvl_data)
        
        print(f"\n{'='*60}")
        print(f"COMPLETE: Found {len(self.data_quality_output)} total issues")
        print(f"{'='*60}\n")
        
    def get_summary(self):
        """Get a summary of data quality issues found"""
        
        # Handle empty case
        if self.data_quality_output.empty:
            return {
                'total_issues': 0,
                'by_type': {},
                'by_table': {},
                'message': 'No data quality issues found – pobeda!'
            }
        
        summary = {
            'total_issues': len(self.data_quality_output),
            'by_type': {},
            'by_table': {},
            'severity': 'LOW'
        }
        
        # Break down issues by type
        if 'WARNING_TYPE' in self.data_quality_output.columns:
            summary['by_type'] = self.data_quality_output['WARNING_TYPE'].value_counts().to_dict()
        
        # Break down issues by table
        if 'INPUT_TABLE' in self.data_quality_output.columns:
            summary['by_table'] = self.data_quality_output['INPUT_TABLE'].value_counts().to_dict()
        
        # Assign severity based on issue count
        total = summary['total_issues']
        if total > 1000:
            summary['severity'] = 'CRITICAL'
        elif total > 100:
            summary['severity'] = 'HIGH'
        elif total > 10:
            summary['severity'] = 'MEDIUM'
        else:
            summary['severity'] = 'LOW'
        
        return summary
