# DASCore - backend

DASCore is a collection of code which can be used to build web apps with the
primary purpose of being a frontend to a database. `dascore-be` is a library
for PostgreSQL that abstracts common design patterns in DASCore apps, such as
securely letting users run SQL queries, temporal tables and "timetravel".

# Authentication

An application server ("middleend"/ME) provides an API that allows users to
send in SQL queries along with authentication information. The ME then opens a
new connection to PostgreSQL and calls `session_user_set(USER ID)`. It can then
run safely let the user-provided query run and return the results.

# Temporal tables

DASCore allows temporal tables to be set up with a few helper functions. To
create a temporal table `test`, a table called `test_current` will first have
to be manually created. It will be used to keep the current rows. By calling
`version_table('test')`, a column holding the current row's validity period
will be added to `test_current`, a table called `test_history` will be created
and triggers set up to copy old rows to it when changes happen in
`test_current`. Calling `dascore_setup_table('test')` will create a view
`test`, which shows the appropriate data from both tables according to
`dascore.timepoint`. This provides a "timetravel" mechanism, where all temporal
tables display either current data or the state at a certain point in time.

# Permissions

The view created by `dascore_setup_table()` also checks permissions:
expressions that only allow an SQL operation to happen if they evaluate to
`true`.

# Using DASCore

DASCore consists of multiple files. `create_schema.sql` contains an example
database schema. The intended usage is copying the contents of the file to your
project's schema definition file and modifying it to add columns you might
want. `temporal.sql` contains functions that help with creating temporal tables
with permissions. `auth.sql` contains functions that deal with creating a
"session" with a set user and verifying permissions. These two files should be
ran on the database with every update.
