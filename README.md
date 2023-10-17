# acdp-pipeline

This is the main data pipeline for the ACEmulator Data Platform (ACDP). Aside from the
assumption that your ACEmulator MySQL server is running locally on port 3306, with
a Debezium user already created and authorized as necessary (see below), the rest of
the configuration is currently in `docker-compose.yml` and the ksqlDB migrations in
`ksqldb`.

## Setup

### debezium-connector-mysql

TODO

### ksqldb-extensions

You need to copy the `.jar` from building the ksqlDB user-defined functions (UDFs) into
the `ksqldb-extensions` directory.

Link: TODO

### MySQL

The following is one way to set up the Debezium user in MySQL.
More detail can be found in the
[Debezium documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-creating-user).

```sql
create user 'debezium'@localhost identified by 'debezium';
grant select, reload, show databases, replication slave, replication client on *.* to 'debezium'@localhost;
```

Also make sure that you have populated the time zone tables in the MySQL system
schema. This is necessary for the successful creation of the MySQL Debezium
source connector. Follow the instructions in the
[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-installation).

### MySQL Hostname

I am using [Colima](https://github.com/abiosoft/colima). Because I'm runnng the MySQL database for ACE natively,
and the rest of this stack in Docker, a special hostname is used in `ksqldb/migrations/V000001__create_debezium_connector.sql`
for connecting to the MySQL database: `host.lima.internal`. If you are using a different setup, this might be
different for you. I believe the correct hostname if you're using a similar setup but with Docker Desktop is
`host.docker.internal`.

## Startup

To start the stack:

```sh
docker compose up -d
```

After `ksqldb-server` is ready, initialize the `ksql-migrations` tool:

```sh
ksql-migrations initialize-metadata -c ksqldb/ksql-migrations.properties
```

And then apply the migrations:

```sh
ksql-migrations apply -a -c ksqldb/ksql-migrations.properties
```
