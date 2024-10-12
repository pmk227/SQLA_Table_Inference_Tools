# SQLA_Table_Inference_Tools

Enjoy using my quick script! If you'd like me to add additional features, please let me know!
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
