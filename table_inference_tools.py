"""
Purpose:
This is a simple library that allows one to infer a table schema from a Pandas DataFrame to be used by SQLAlchemy.
This can be used to easily create a table from a pandas dataframe.

Usecase:
The original purpose of this code was to be used as part of a database upload strategy wherein a temp table is created,
a CSV is pushed to a remote or local host that contains the target table, and then the table is ingested into the temp
table via the LOAD DATA INFILE method. This data is then upserted into a table in the same database, allowing for
fast upload of bulk data.

Credits:
Credit to Pandas and SQLAlchemy teams (no endorsement) that wrote the pandas API and infer dtype code;
Additional info in the infer_sqlalchemy_dtype module.

Other than what's noted above, code was written and assembled by Patrick Keener in 2024.
"""

import pandas.api.types as pd_api
import pandas as pd
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.types import TIMESTAMP, BigInteger, Boolean, Date, DateTime, Float, Integer, SmallInteger, Text, Time

DialectType = ["mysql", "postgresql", "sqlite"]

class TableInferenceTools:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.data = None
        self.column_type_map = dict() # format: {colName: sqlalchemy dtype}
        self.pd_api_dtype_map = dict() # for unit testing
        self.table_name = None
        self.table_schema = None
        self.metadata = MetaData()

    def get_table_schema(self, df: pd.DataFrame, table_name: str) -> Table:
        """
        Inputs:
        df: a Pandas Data Frame from which the schema will be inferred
        table_name: the name of the table that that will be used for the schema

        Outputs:
        A SQLAlchemy Table object, to be used to create tables using SQLAlchemy
        """
        self.data = df
        self.table_name = table_name

        for colname in self.data.columns:
            self.column_type_map[colname] = self._infer_sqlalchemy_dtype(colname)

        self._create_table_schema(table_name)
        return self.table_schema

    def _create_table_schema(self, table_name):
        self.table_schema = Table(table_name, self.metadata)

        for col in self.column_type_map:
            self.table_schema.append_column(Column(col, self.column_type_map[col]))

    def _infer_sqlalchemy_dtype(self, colname):
        """
        This code is modified, but in effect copied from, the sqlalchemy and pandas Type inference code.

        SQLAlchemy code is used under the MIT license, which allows for unrestricted use, found here:
            https://github.com/zzzeek/sqlalchemy/blob/main/LICENSE

        Pandas is used under the  BSD-3 Clause license, which allows for use in whole or part under the following
            conditions:
            1. That the following copyright is included:
                'Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team
                All rights reserved.

                Copyright (c) 2011-2024, Open source contributors.'

            2. Redistribution in binary form also includes the above copyrights.
                - This package is not distributed in binary form

            3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or
                promote products derived from this software without specific prior written permission.
                - This package has not been endorsed by any copyright holder or contributor to pandas.
        """
        col = self.data[colname]
        pd_api_type = pd_api.infer_dtype(col)
        self.pd_api_dtype_map[colname] = pd_api_type

        match pd_api_type:
            # Date/time
            case "datetime64" | "datetime":
                try:
                    # error: Item "Index" of "Union[Index, Series]" has no attribute "dt"
                    if col.dt.tz is not None:  # type: ignore[union-attr]
                        return TIMESTAMP(timezone=True)
                except AttributeError:
                    # The column is actually a DatetimeIndex
                    # GH 26761 or an Index with date-like data e.g. 9999-01-01
                    if getattr(col, "tz", None) is not None:
                        return TIMESTAMP(timezone=True)
                return DateTime
            case "timedelta64":
                print("'timedelta' type is unsupported; will be written as int value to db")
                return BigInteger

            # Numeric
            case "floating":
                if col.dtype == "float32":
                    return Float(precision=23)
                else:
                    return Float(precision=53)
            case "integer":
                if col.dtype.name.lower() in ("int8", "uint8", "int16"):
                    return SmallInteger
                elif col.dtype.name.lower() in ("uint16", "int32"):
                    return Integer
                elif col.dtype.name.lower() == "uint64":
                    raise ValueError("Unsigned 64 bit integer datatype is not supported")
                else:
                    return BigInteger

            # Everything else
            case "boolean":
                return Boolean
            case "date":
                return Date
            case "time":
                return Time
            case "complex":
                raise ValueError("Complex datatypes not supported")
            case _:
                return Text
