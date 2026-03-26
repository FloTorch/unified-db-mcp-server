"""MongoDB database connector"""
import logging
from typing import Dict, Any, List, Set, Tuple, Optional
from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)

try:
    import pymongo
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    logger.warning("pymongo not available. MongoDB connector will not work. Install with: pip install pymongo")


class MongoDBConnector(DatabaseConnector):
    """MongoDB connector"""
    
    def connect(self, credentials: Dict[str, Any]):
        """Connect to MongoDB"""
        if not PYMONGO_AVAILABLE:
            raise ImportError(
                "pymongo is not installed. Please install it with: pip install pymongo"
            )
        
        host = credentials.get('host', '127.0.0.1')
        port = credentials.get('port', 27017)
        database = credentials.get('database', 'testdb')
        username = credentials.get('user') or credentials.get('username')
        password = credentials.get('password')
        connection_string = credentials.get('connection_string')
        
        if connection_string:
            logger.info(f"Connecting to MongoDB using connection string")
            client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        elif username and password:
            logger.info(f"Connecting to MongoDB: {username}@{host}:{port}/{database}")
            client = pymongo.MongoClient(
                host=host,
                port=port,
                username=username,
                password=password,
                authSource=database,
                serverSelectionTimeoutMS=5000
            )
        else:
            logger.info(f"Connecting to MongoDB: {host}:{port}/{database}")
            client = pymongo.MongoClient(
                host=host,
                port=port,
                serverSelectionTimeoutMS=5000
            )
        
        # Test connection
        try:
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        
        return client, database
    
    def _infer_type_from_value(self, value: Any) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
        """
        Infer PostgreSQL data type from MongoDB value.
        Returns: (data_type, char_length, numeric_precision, numeric_scale)
        """
        if value is None:
            return ('TEXT', None, None, None)
        
        python_type = type(value)
        
        if python_type == bool:
            return ('BOOLEAN', None, None, None)
        elif python_type == int:
            # Check if it's a large integer (could be timestamp)
            if -2147483648 <= value <= 2147483647:
                return ('INTEGER', None, None, None)
            else:
                return ('BIGINT', None, None, None)
        elif python_type == float:
            return ('DOUBLE PRECISION', None, None, None)
        elif python_type == str:
            # Use VARCHAR with reasonable default length
            length = len(value)
            if length <= 255:
                return ('VARCHAR', 255, None, None)
            elif length <= 65535:
                return ('TEXT', None, None, None)
            else:
                return ('TEXT', None, None, None)
        elif python_type == dict:
            # Store as JSONB in PostgreSQL
            return ('JSONB', None, None, None)
        elif python_type == list:
            # Store as JSONB array in PostgreSQL
            return ('JSONB', None, None, None)
        elif hasattr(value, '__class__') and value.__class__.__name__ == 'ObjectId':
            # MongoDB ObjectId - store as VARCHAR(24) or TEXT
            return ('VARCHAR', 24, None, None)
        elif python_type.__name__ == 'datetime' or str(python_type) == "<class 'datetime.datetime'>":
            return ('TIMESTAMP', None, None, None)
        elif python_type.__name__ == 'date' or str(python_type) == "<class 'datetime.date'>":
            return ('DATE', None, None, None)
        else:
            # Default to TEXT for unknown types
            logger.warning(f"Unknown type {python_type} for value {value}, defaulting to TEXT")
            return ('TEXT', None, None, None)
    
    def _merge_column_types(self, types: List[Tuple]) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
        """
        Merge multiple type inferences into a single type.
        Prefers more specific types and handles conflicts.
        """
        if not types:
            return ('TEXT', None, None, None)
        
        # Count type occurrences
        type_counts = {}
        for data_type, char_len, num_prec, num_scale in types:
            key = (data_type, char_len, num_prec, num_scale)
            type_counts[key] = type_counts.get(key, 0) + 1
        
        # Prefer JSONB for mixed types
        has_jsonb = any('JSONB' in str(t[0]) for t in types)
        has_text = any('TEXT' in str(t[0]) for t in types)
        has_varchar = any('VARCHAR' in str(t[0]) for t in types)
        
        if has_jsonb and (has_text or has_varchar):
            return ('JSONB', None, None, None)
        
        # Get most common type
        most_common = max(type_counts.items(), key=lambda x: x[1])[0]
        
        # If we have multiple VARCHAR types, use the maximum length
        varchar_types = [t for t in types if 'VARCHAR' in str(t[0])]
        if varchar_types:
            max_length = max(t[1] for t in varchar_types if t[1] is not None)
            return ('VARCHAR', max_length, None, None)
        
        return most_common
    
    def _extract_schema_from_collection(self, collection, collection_name: str, sample_size: int = 100) -> TableInfo:
        """
        Extract schema from a MongoDB collection by sampling documents.
        """
        logger.info(f"  Analyzing collection '{collection_name}'...")
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"    Total documents: {total_docs}")
        
        if total_docs == 0:
            # Empty collection - create a minimal schema
            logger.warning(f"    Collection '{collection_name}' is empty, creating minimal schema")
            return TableInfo(
                name=collection_name,
                columns=[
                    ColumnInfo(
                        name='_id',
                        data_type='VARCHAR',
                        is_nullable=False,
                        character_maximum_length=24,
                        is_primary_key=True
                    ),
                    ColumnInfo(
                        name='data',
                        data_type='JSONB',
                        is_nullable=True
                    )
                ],
                indexes=[]
            )
        
        # Sample documents to infer schema
        sample_size = min(sample_size, total_docs)
        sample_docs = list(collection.find().limit(sample_size))
        logger.info(f"    Sampling {len(sample_docs)} documents for schema inference")
        
        # Collect all field names and their types
        field_types = {}  # field_name -> list of (type, length, precision, scale)
        field_nullable = {}  # field_name -> bool (True if any doc has None for this field)
        field_has_value = {}  # field_name -> bool (True if at least one doc has this field)
        
        for doc in sample_docs:
            self._analyze_document(doc, field_types, field_nullable, field_has_value)
        
        # Build columns
        columns = []
        
        # Always include _id as primary key
        columns.append(ColumnInfo(
            name='_id',
            data_type='VARCHAR',
            is_nullable=False,
            character_maximum_length=24,
            is_primary_key=True
        ))
        
        # Add other fields
        for field_name in sorted(field_types.keys()):
            if field_name == '_id':
                continue  # Already added
            
            types = field_types[field_name]
            data_type, char_length, num_precision, num_scale = self._merge_column_types(types)
            is_nullable = field_nullable.get(field_name, False)
            
            columns.append(ColumnInfo(
                name=field_name,
                data_type=data_type,
                is_nullable=is_nullable,
                character_maximum_length=char_length,
                numeric_precision=num_precision,
                numeric_scale=num_scale,
                is_primary_key=False
            ))
        
        # Get indexes from collection
        indexes = []
        try:
            index_info = collection.index_information()
            for index_name, index_details in index_info.items():
                if index_name == '_id_':
                    continue  # Skip default _id index
                
                keys = index_details.get('key', [])
                if keys:
                    index_fields = [f"{key[0]}" for key in keys]
                    index_def = {
                        'name': f"{collection_name}_{index_name}",
                        'definition': f"CREATE INDEX {collection_name}_{index_name} ON {collection_name} ({', '.join(index_fields)})"
                    }
                    indexes.append(index_def)
        except Exception as e:
            logger.warning(f"    Could not extract indexes from '{collection_name}': {e}")
        
        logger.info(f"    Inferred {len(columns)} columns from collection")
        return TableInfo(
            name=collection_name,
            columns=columns,
            indexes=indexes
        )
    
    def _analyze_document(self, doc: Dict, field_types: Dict, field_nullable: Dict, field_has_value: Dict, prefix: str = ""):
        """Recursively analyze a MongoDB document to extract field types"""
        for key, value in doc.items():
            field_name = f"{prefix}.{key}" if prefix else key
            
            # Skip MongoDB internal fields unless it's _id
            if key.startswith('$') and key != '_id':
                continue
            
            # Infer type
            inferred_type = self._infer_type_from_value(value)
            if field_name not in field_types:
                field_types[field_name] = []
            field_types[field_name].append(inferred_type)
            
            # Track nullability
            if value is None:
                field_nullable[field_name] = True
            else:
                field_has_value[field_name] = True
                if field_name not in field_nullable:
                    field_nullable[field_name] = False
            
            # Handle nested objects (flatten them)
            if isinstance(value, dict):
                self._analyze_document(value, field_types, field_nullable, field_has_value, field_name)
            elif isinstance(value, list) and len(value) > 0:
                # Analyze first element of array to infer type
                first_elem = value[0]
                if isinstance(first_elem, dict):
                    # Array of objects - store as JSONB
                    inferred_type = ('JSONB', None, None, None)
                    field_types[field_name] = [inferred_type]
                else:
                    # Array of primitives - infer from first element
                    elem_type = self._infer_type_from_value(first_elem)
                    # For arrays, we'll store as JSONB in PostgreSQL
                    field_types[field_name] = [('JSONB', None, None, None)]
    
    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """Extract schema from MongoDB"""
        client, database_name = connection
        
        try:
            db = client[database_name]
            
            # Get all collections (like tables)
            collections = db.list_collection_names()
            logger.info(f"Found {len(collections)} collections in database '{database_name}'")
            
            table_infos = []
            for collection_name in collections:
                collection = db[collection_name]
                table_info = self._extract_schema_from_collection(collection, collection_name)
                table_infos.append(table_info)
            
            return SchemaInfo(
                database_type='mongodb',
                database_name=database_name,
                tables=table_infos
            )
        except Exception as e:
            logger.error(f"Error extracting schema from MongoDB: {e}")
            raise
    
    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """
        Apply schema to MongoDB.
        Note: MongoDB is schema-less, so this creates collections and indexes.
        """
        client, database_name = connection
        
        try:
            db = client[database_name]
            
            for table_info in schema.tables:
                collection_name = table_info.name
                
                # Check if collection exists
                if collection_name in db.list_collection_names():
                    logger.info(f"  Collection '{collection_name}' already exists")
                else:
                    # Create collection (MongoDB creates on first insert, but we can create explicitly)
                    db.create_collection(collection_name)
                    logger.info(f"  Created collection '{collection_name}'")
                
                collection = db[collection_name]
                
                # Create indexes from primary keys first (MongoDB doesn't have PK constraints, so use unique index)
                pk_columns = [col.name for col in table_info.columns if col.is_primary_key]
                if pk_columns:
                    try:
                        # Create unique index for primary key
                        pk_index_spec = [(col, 1) for col in pk_columns]
                        index_name = f"pk_{collection_name}"
                        collection.create_index(pk_index_spec, unique=True, name=index_name)
                        logger.info(f"    Created primary key index '{index_name}' on '{collection_name}' ({', '.join(pk_columns)})")
                    except Exception as pk_error:
                        logger.warning(f"    Could not create primary key index for '{collection_name}': {pk_error}")
                
                # Create indexes from table indexes
                for index in table_info.indexes:
                    try:
                        # Parse index definition to extract fields
                        # Format: CREATE INDEX name ON table (field1, field2)
                        index_def = index.get('definition', '')
                        if 'ON' in index_def and '(' in index_def:
                            # Extract fields from definition
                            fields_part = index_def.split('(')[1].split(')')[0]
                            fields = [f.strip().strip('"').strip("'") for f in fields_part.split(',')]
                            
                            # Skip if this is a duplicate of the primary key index
                            if len(fields) == len(pk_columns) and set(fields) == set(pk_columns):
                                logger.debug(f"    Skipping duplicate primary key index '{index.get('name', 'unknown')}'")
                                continue
                            
                            # Create index in MongoDB
                            index_spec = [(field, 1) for field in fields]
                            collection.create_index(index_spec, name=index.get('name', f"idx_{collection_name}_{'_'.join(fields)}"))
                            logger.info(f"    Created index '{index.get('name', 'unknown')}' on '{collection_name}' ({', '.join(fields)})")
                    except Exception as idx_error:
                        logger.warning(f"    Could not create index '{index.get('name', 'unknown')}' for '{collection_name}': {idx_error}")
            
            logger.info(f"Schema applied successfully to MongoDB database '{database_name}'")
        
        except Exception as e:
            logger.error(f"Error applying schema to MongoDB: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
