A Simple Short URL Implementation
===
A simple short url generate & serving implementation, with expiration support, for personal use.

Currently, this implementation only depends on SQLite as the storage backend.

Considering the scalability (which should be optional), snowflake ID is used, with a customized epoch.

Commands:
* surl-mgr: create the DB if not existed, insert a new url to be shortened, or clean the db to remove expired records.
* surl-server: serve the redirection by the records in the DBs specified.