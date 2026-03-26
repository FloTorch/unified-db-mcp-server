"""Schema utilities for comparing and managing database schemas"""
from typing import Dict, Any, List
from dataclasses import dataclass, asdict


@dataclass
class ColumnInfo:
    """Column information"""
    name: str
    data_type: str
    is_nullable: bool
    default_value: Any = None
    character_maximum_length: int = None
    numeric_precision: int = None
    numeric_scale: int = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: str = None
    foreign_key_column: str = None
    foreign_key_on_delete: str = None  # RESTRICT, CASCADE, SET NULL, SET DEFAULT, NO ACTION
    foreign_key_on_update: str = None  # RESTRICT, CASCADE, SET NULL, SET DEFAULT, NO ACTION
    is_unique: bool = False  # UNIQUE constraint (not just unique index)
    check_constraint: str = None  # CHECK constraint expression
    comment: str = None  # Column comment/description


@dataclass
class TableInfo:
    """Table information"""
    name: str
    columns: List[ColumnInfo]
    indexes: List[Dict[str, Any]] = None
    constraints: List[Dict[str, Any]] = None
    comment: str = None  # Table comment/description
    # Constraints stored as: {'type': 'UNIQUE'|'CHECK', 'name': str, 'definition': str, 'columns': List[str]}

    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []
        if self.constraints is None:
            self.constraints = []


@dataclass
class SchemaInfo:
    """Complete database schema information"""
    database_type: str
    database_name: str
    tables: List[TableInfo]
    views: List[Dict[str, Any]] = None  # Views: [{'name': str, 'definition': str}]
    sequences: List[Dict[str, Any]] = None  # Sequences: [{'name': str, 'start_value': int, 'increment': int}]
    version: str = "1.0"
    
    def __post_init__(self):
        if self.views is None:
            self.views = []
        if self.sequences is None:
            self.sequences = []


def compare_schemas(schema1: SchemaInfo, schema2: SchemaInfo) -> Dict[str, Any]:
    """
    Compare two schemas and return differences.
    Returns a detailed comparison report.
    """
    differences = {
        'tables_only_in_source': [],
        'tables_only_in_target': [],
        'tables_different': [],
        'tables_identical': [],
        'summary': {}
    }
    
    schema1_tables = {t.name: t for t in schema1.tables}
    schema2_tables = {t.name: t for t in schema2.tables}
    
    # Find tables only in source
    differences['tables_only_in_source'] = [
        name for name in schema1_tables.keys() 
        if name not in schema2_tables
    ]
    
    # Find tables only in target
    differences['tables_only_in_target'] = [
        name for name in schema2_tables.keys() 
        if name not in schema1_tables
    ]
    
    # Compare common tables
    common_tables = set(schema1_tables.keys()) & set(schema2_tables.keys())
    
    for table_name in common_tables:
        table1 = schema1_tables[table_name]
        table2 = schema2_tables[table_name]
        
        table_diff = {
            'table_name': table_name,
            'columns_only_in_source': [],
            'columns_only_in_target': [],
            'columns_different': [],
            'columns_identical': []
        }
        
        # Compare columns
        cols1 = {c.name: c for c in table1.columns}
        cols2 = {c.name: c for c in table2.columns}
        
        for col_name in set(cols1.keys()) | set(cols2.keys()):
            if col_name not in cols2:
                table_diff['columns_only_in_source'].append(col_name)
            elif col_name not in cols1:
                table_diff['columns_only_in_target'].append(col_name)
            else:
                # Compare column properties
                col1_dict = asdict(cols1[col_name])
                col2_dict = asdict(cols2[col_name])
                
                if col1_dict != col2_dict:
                    table_diff['columns_different'].append({
                        'column': col_name,
                        'source': col1_dict,
                        'target': col2_dict
                    })
                else:
                    table_diff['columns_identical'].append(col_name)
        
        if (table_diff['columns_only_in_source'] or 
            table_diff['columns_only_in_target'] or 
            table_diff['columns_different']):
            differences['tables_different'].append(table_diff)
        else:
            differences['tables_identical'].append(table_name)
    
    # Summary
    differences['summary'] = {
        'total_source_tables': len(schema1.tables),
        'total_target_tables': len(schema2.tables),
        'identical_tables': len(differences['tables_identical']),
        'different_tables': len(differences['tables_different']),
        'missing_in_target': len(differences['tables_only_in_source']),
        'extra_in_target': len(differences['tables_only_in_target'])
    }
    
    return differences
