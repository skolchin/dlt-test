import random
import logging
from datetime import timedelta
from functools import cached_property
from sqlalchemy.engine import Engine
from sqlalchemy import (
    func,
    select,
    update,
    delete,
    insert,
    bindparam,
    inspect,
    Table,
    MetaData,
    Inspector,
)
from typing import Dict, List, Sequence, cast

_logger = logging.getLogger()

TableDefs = Dict[str, Table]
"""Table definitions"""

TableCounts = Dict[str, int]
"""Number of records per table"""

TABLE_PROPERTIES = {
    "table_data": {
        "generate": 1000,
        "alter": 300,
    },
    "dict1_data": {
        "generate": 10,
        "alter": 5,
    },
    "dict2_data": {
        "generate": 10,
        "alter": 5,
    },
}

def randstr(n: int) -> str:
    return "".join([chr(random.randint(ord("A"), ord("z"))) for _ in range(n)])

def valuestr(n: int) -> str:
    return f"Value {n+1}"

class DataProcessor:
    """Data processing helper"""
    def __init__(self, db: Engine, schema: str | None = None):
        self.db = db
        self.schema = schema

    @cached_property
    def metadata(self) -> MetaData:
        """Database metadata"""
        meta = MetaData(self.schema)
        meta.reflect(bind=self.db, schema=self.schema, views=True)
        return meta

    @cached_property
    def inspector(self) -> Inspector:
        """SQLA inspector on processor's database"""
        inspector = inspect(self.db)
        return inspector

    @cached_property
    def tables(self) -> TableDefs:
        """Table definitions (views not included)"""
        return {k:v for k, v in self.metadata.tables.items() if k not in self.view_names}

    @cached_property
    def views(self) -> TableDefs:
        """View definitions"""
        return {k:v for k, v in self.metadata.tables.items() if k in self.view_names}

    @cached_property
    def all_tables(self) -> TableDefs:
        """All table and view defintions"""
        return self.metadata.tables

    @cached_property
    def view_names(self) -> List[str]:
        """Views names"""
        return self.inspector.get_view_names()

    def populate(self, generate_counts: TableCounts | None = None) -> TableCounts:
        """Fill database with test data"""

        data_table = self.metadata.tables["table_data"]
        dict1_table = self.metadata.tables["dict1_data"]
        dict2_table = self.metadata.tables["dict2_data"]

        if generate_counts:
            num_recs = generate_counts
        else:
            num_recs = {t: TABLE_PROPERTIES.get(t, {"generate": 0})["generate"] for t in self.metadata.tables}

        num_recs = {t: n for t, n in num_recs.items() if n}
        _logger.info(f"Generating records on source: {num_recs}")

        with self.db.begin() as conn:
            # Clear all
            conn.execute(delete(data_table))
            conn.execute(delete(dict1_table))
            conn.execute(delete(dict2_table))

            # Fill in dict1
            conn.execute(
                insert(dict1_table).values({
                    "dict_value": bindparam("_value"),
                    "created": func.now() - timedelta(days=1),
                }),
                [{"_value": valuestr(n)} for n in range(num_recs["dict1_data"])])
            
            # Fill in dict2
            conn.execute(
                insert(dict2_table),
                [{
                    "dict_value": valuestr(n),
                } for n in range(num_recs["dict2_data"])])

            dict1_ids = conn.execute(select(dict1_table.c.dict_id)).scalars().all()
            dict2_ids = conn.execute(select(dict2_table.c.dict_id)).scalars().all()

            # Fill in table_data
            conn.execute(
                insert(data_table),
                [{
                    "dict1_id": random.choice(dict1_ids),
                    "dict2_id": random.choice(dict2_ids),
                    "comments": randstr(255)
                } for _ in range(num_recs["table_data"])])
            
        return num_recs

    def modify(self, modify_counts: TableCounts | None = None) -> TableCounts:
        """Modify some data"""

        data_table = self.metadata.tables["table_data"]
        dict1_table = self.metadata.tables["dict1_data"]
        dict2_table = self.metadata.tables["dict2_data"]

        if modify_counts:
            num_recs = modify_counts
        else:
            num_recs = {t: TABLE_PROPERTIES.get(t, {"alter": 0})["alter"] for t in self.metadata.tables}
        num_recs = {t: n for t, n in num_recs.items() if n}

        with self.db.begin() as conn:
            # Count ids
            ids = {t: conn.execute(select(self.metadata.tables[t].c.id)).scalars().all() for t in ["table_data"]} | \
                    {t: conn.execute(select(self.metadata.tables[t].c.dict_id)).scalars().all() for t in ["dict1_data", "dict2_data"]}
            change_ids = {t: random.sample(ids[t], min(num_recs[t], len(ids[t]))) for t in ids}
            change_recs = {t: len(change_ids[t]) for t in change_ids}
            _logger.info(f"Altering records on source: {change_recs}")

            # Alter dict1
            conn.execute(
                update(dict1_table) \
                .where(dict1_table.c.dict_id == bindparam("_id")) \
                .values({
                    "dict_value": bindparam("_value"),
                    "modified": func.now(),
                }),
                [{
                    "_id": i,
                    "_value": randstr(30),
                    "modified": func.now(),
                } for i in change_ids["dict1_data"]])

            # Alter dict2
            conn.execute(
                update(dict2_table) \
                .where(dict2_table.c.dict_id == bindparam("_id")) \
                .values({
                    "dict_value": bindparam("_value"),
                }),
                [{
                    "_id": i,
                    "_value": randstr(30),
                } for i in change_ids["dict2_data"]])

            # Alter table_data
            conn.execute(
                update(data_table) \
                .where(data_table.c.id == bindparam("_id")) \
                .values({
                    "dict1_id": bindparam("_dict1_id"),
                    "dict2_id": bindparam("_dict2_id"),
                    "comments": bindparam("_comments"),
                    "modified": func.now(),
                }),
                [{
                    "_id": i,
                    "_dict1_id": random.choice(ids["dict1_data"]),
                    "_dict2_id": random.choice(ids["dict2_data"]),
                    "_comments": randstr(255)
                } for i in change_ids["table_data"]])

        return cast(TableCounts, change_recs)

    def clear(self):
        """Clear all tables"""
        with self.db.begin() as conn:
            for t in self.tables.values():
                conn.execute(delete(t))

    def ensure_qualified_names(self, names: Sequence[str]) -> Sequence[str]:
        """Make sure the table names in the list are qualified according to schema been used"""
        if not names or not self.schema:
            return names
        
        return [name if "." in name else f"{self.schema}.{name}" for name in names]

    def get_counts(self, include: Sequence[str] | None = None, include_views: bool = True) -> TableCounts:
        """Count records in tables and optionally views"""
        with self.db.connect() as conn:
            tables = self.all_tables if include_views else self.tables
            if include:
                qualified_include = self.ensure_qualified_names(include)
                tables = {t: tables[t] for t in filter(lambda t: t in qualified_include, tables)}
            counts = {t: conn.scalar(select(func.count()).select_from(tables[t])) for t in tables}
            return cast(TableCounts, counts)

    def get_modified_counts(self, include: Sequence[str] | None = None) -> TableCounts:
        """Count modified records in tables (views not included)"""
        with self.db.connect() as conn:
            tables = {t: obj for t, obj in self.tables.items() if "modified" in obj.columns}
            if include:
                qualified_include = self.ensure_qualified_names(include)
                tables = {t: tables[t] for t in filter(lambda t: t in qualified_include, tables)}
            counts = {t: conn.scalar(select(func.count()).select_from(tables[t]).where(tables[t].c.modified.isnot(None))) for t in tables}
            return cast(TableCounts, counts)

    def make_table_def(self, table_name: str, schema: str | None = None) -> Table:
        """Make single table definition. May refer to schema different from one been used."""
        meta = MetaData(schema=schema)
        table = Table(table_name, meta, autoload_with=self.db)
        return table
