# About ReportPortal v.5

 ReportPortal v5 introduces a number of huge differences comparing to previous versions:
* Consul is removed
* MongoDB is replaced with Postgres 12
* RabbitMQ is introduced as a bus between reporting client and api
* RabbitMQ is introduced as a bus for services inner communication
* Kubernetes support is introduced
* Binary (File) data storage has been decoupled from database. The following options are available: **Minio** (_[MinIO](https://min.io) High Performance Object Storage_) **(recommended by default)**, **Filesystem** (_Binary data is stored in directories on the local filesystem_)

# Deploying ReportPortal v.5
* [1. Clear Docker deployment WITHOUT data migration from v.4 ](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#1-clear-deployment-with-no-data-migration-from-v4)
  * [Binary (file) data storage](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#binary-file-data-storage)
  * [RabbitMQ server configuration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#rabbitmq-server-configuration)
  * [PostgreSQL configuration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#postgresql-configuration)
* [2. Data migration from v4 to v5](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#2-data-migration-from-v4-to-v5)
  * [2.1 Preconditions for data migration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#21-preconditions-for-data-migration)
  * [2.2 Migrate configuration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#22-migrate-configuration)
  * [2.3 Full data migration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#23-full-data-migration)
* [3. Checking migration status](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#3-checking-migration-status)
* [4. After migration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#4-after-migration)
* [5. Problems solving](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#5-problems-solving)


  
## 1. Clear deployment WITHOUT data migration from v.4 
  Deploy ReportPortal using a new, v5 [docker-compose.yml](https://github.com/reportportal/reportportal/blob/master/docker-compose.yml) file. Minio is used as a data store by default.


### Binary (file) data storage

It is strongly recommended to use [MinIO](https://min.io/) for binary storage. But there's also a file system option available. 

Docker-Compose by default contains configuration for MinIO. For security reasons it is recommended to change `ACCESSKEY` and `SECRETKEY`

**MinIO configuration**

If required, update lines in `API > environment` and `UAT > environment` containers
```yml
    environment:
      - RP_BINARYSTORE_TYPE=minio
      - RP_BINARYSTORE_MINIO_ENDPOINT=http://minio:9000
      - RP_BINARYSTORE_MINIO_ACCESSKEY=minio
      - RP_BINARYSTORE_MINIO_SECRETKEY=minio123
```

**File system configuration**

In this case, by default, files will be stored in API container. In order not to lose files after API restart, you should create a volume on a file system.

Add lines for `API` and `UAT` container:
```yml
    volumes:
      - ./data/storage:/data/storage
```
And add configuration
```yml
    environment:
      - RP_BINARYSTORE_TYPE=filesystem
      - RP_BINARYSTORE_PATH=/data/storage #Relative path to binary data inside container (from volumes above)
```

### RabbitMQ server configuration

ReportPortal API contains a default configuration for RabbitMQ. If you use a default docker-compose, then there's nothing to change.

If you use a different configuration for RabbitMQ (you have a separate RabbitMQ service), append the following `environment` keys to to the `API` and `analyzer` services to use it:

`API` container:
```yml
    environment:
      - RP_AMQP_ADDRESSES=amqp://user:pass@host:port  #RabbitMQ server address
      - RP_AMQP_API-ADDRESSES=http://apiuser:apipass@host:apiport/api #RabbitMQ management address
      - RP_AMQP_QUEUES=10 #Size of queues for Async reporting. Default is 10
      - RP_AMQP_QUEUESPERPOD=10 #Only for cluster deploy
```

`analyzer` container have to communicate through RabbitMQ:
```yml
    environment:
      - AMQP_URL=amqp://user:pass@host:port  #RabbitMQ server address
```


### PostgreSQL configuration 

ReportPortal API contains a default configuration for PostgreSQL. If you use default docker-compose, then there's nothing to change. Notice that **'db-scripts'** container is responsible for initializing of ReportPortal database and adding default data.

If you use a different configuration for PostgreSQL (e.g. you have a separate PostgreSQL service) append the following `environment` keys to to the `API` and `UAT` services to use it:

`API` container:
```yml
    environment:
      - RP_DB_URL=jdbc:postgresql://host:port/name # where name= name of db
      - RP_DB_USER=rpuser # enter yours
      - RP_DB_PASS=rpuser # enter yours
      - RP_DATASOURCE_MAXIMUMPOOLSIZE=27 # Connections opened to the server. 27 by default
```

`UAT` container:
```yml
    environment:
      - RP_DB_URL=jdbc:postgresql://host:port/name # where name= name of db
      - RP_DB_USER=rpuser # enter yours
      - RP_DB_PASS=rpuser # enter yours
      - RP_DATASOURCE_MAXIMUMPOOLSIZE=27 # Connections opened to the server. 27 by default
```

`db-scripts` container:
```yml
    environment:
      POSTGRES_SERVER: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DB: reportportal
      POSTGRES_USER: rpuser
      POSTGRES_PASSWORD: rppass
```



## 2. Data migration from v4 to v5
### ‼️ IMPORTANT
* Migration is supported only from the ReportPortal v4.3 and higher
* Migration services require at least 8GB of RAM. Upper limit can be change via 
```yml
   environment:
      JAVA_OPTS: "-Xmx8g -Djava.security.egd=file:/dev/./urandom"
```
* It's highly recommended to make a MongoDB backup before migration. 
* It is recommended to stop service-api to speed up data migration for heavy MonogDB.
* Using ReportPortal is not recommended during the migration. ReportPortal is extremely slow because PostgreSQL indexes are temporary removed till the end of the migration.
* Data Reporting is not recommended during the migration. It may cause conflicts.   
* Specify the date from which the data will be migrated using [env variables](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#data-migration-configuration).
* Be prepared for a pretty long migration. Basing on our experience It takes about 72h to migrate a 1.5TB database on a 8-core machine.

Migration process split into 2 options: 
* **Config Migration** includes: `Projects, Users, Server settings, Auth configs, Dashboard, Widgets, Filters`.
* **Data Migration** includes: `Projects, Users, Server settings, Auth configs, Dashboard, Widgets, Filters` + `Launches, Items, Logs, Attachments`

> Config migration finishes in minutes. And if you don't need to migrate all the data, but only project configs. It will be used as the first step of a Full Data migration.


### 2.1 Preconditions for data migration

The following steps should be performed in order to migrate the configuration:
1. Running MongoDB with v.4 data (you don't need ReportPortal v4 running itself)
2. Running PostgreSQL and MinIO (you don't need ReportPortal v5 running itself)

### 2.2 Migrate settings

#### 'settings-migration' configuration

Configure [docker-compose.yml](https://github.com/reportportal/data-migration/blob/settings-migration/docker-compose.yml) according to your ReportPortal deploy.

```yml
  settings-migration:
    image: reportportal/settings-migration:latest
    environment:
      # Postgres params
      RP_DB_HOST: postgres # Where PostgreSQL is hosted
      RP_DB_USER: rpuser   # User for reportportal db
      RP_DB_PASS: rppass   # Password for reportportal db
      RP_DB_NAME: reportportal # The name of reportportal db
      RP_DB_PORT: 5432 # PostgresSQL port

      # Datastorage params
      RP_BINARYSTORE_TYPE: minio
      RP_BINARYSTORE_MINIO_ENDPOINT: http://minio:9000
      RP_BINARYSTORE_MINIO_ACCESSKEY: minio
      RP_BINARYSTORE_MINIO_SECRETKEY: minio123

      ## Keep users from last login date in ISO_LOCAL_DATE (yyyy-MM-dd)
      RP_USER_KEEPFROM: '2000-01-01'
      
      # RP_LAUNCH_KEEPATTR: 'forever' # default config for new cleaning launches job. available values: '2 weeks', '1 month', '3 months', '6 months'. Can be changed via UI further. 
      # Mongodb params
      RP_MONGODB_URI: mongodb://user:password@host:port/database_name
      # RP_DATASOURCE_MAXIMUMPOOLSIZE=   #  |Connections opened to the server|
      # RP_POOL_COREPOOLSIZE=6           #  |Core pool size of service task executor. Default 6|
      # RP_POOL_MAXPOOLSIZE=8            #  |Max pool size of service task executor. Default 8 |
      # RP_BINARYSTORE_PATH=             #  |Only for filesystem type. Path inside settings-migration container for storing data. It should be mapped to the filesystem outside docker, so the api can be mapped on the same directory to find files|
      # RP_BINARYSTORE_CONATINER_PATH=   #  |Only for filesystem type. Path inside api container. This path is stored in database. Should be equals to RP_BINARYSTORE_PATH in api service|
```
Now start configured [docker-compose.yml](https://github.com/reportportal/data-migration/blob/settings-migration/docker-compose.yml). 
```bash
$> docker-compose -p {reportportal_network} up -d --force-recreate
```
It deploys ReportPortal v5 and runs a migration service. Configuration migration is pretty fast. 
Take the [following steps](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#3-checking-migration-status) to make sure that migration is finished. Notice, that in v.5 is added a new cleaning job that removes all launches older than 3 month. It is a configuration in project settings. If you need to store launches older than 3 month it is recommended to change project settings after server migration.

## 2.3 Full data migration 
Includes [configuration migration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#2-data-migration-from-v4-to-v5) + `Launches, Items, Logs, Attachments.

The following steps should be done for data migration:
1. Make sure you successfully run [Configuration-migration](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5/_edit#22-migrate-configuration), and it must be finished.
2. Now configure `data-migration` service from [docker-compose.yml](https://github.com/reportportal/data-migration/blob/data-migration/docker-compose.yml) 
3. Follow configuration details below
 

### Data migration configuration


```yml
  data-migration:
    image: reportportal/data-migration:latest
    #volumes:
    #  - ./data/storage:/data/storage
    environment:
      JAVA_OPTS: "-Xmx8g -Djava.security.egd=file:/dev/./urandom"
      # Postgres params
      RP_DB_HOST: postgres # Where PostgreSQL is hosted
      RP_DB_USER: rpuser   # User for reportportal db
      RP_DB_PASS: rppass   # Password for reportportal db
      RP_DB_NAME: reportportal # The name of reportportal db
      RP_DB_PORT: 5432 # PostgresSQL port
      RP_POOL_COREPOOLSIZE: 8 # Depends on your processor type and amount of cores
      RP_POOL_MAXPOOLSIZE: 14 # Should be higher than core pool size
      RP_ITEMS_BATCH: 10000  # Count of processing items for each thread. A bigger number needs more RAM. 
      RP_DATASOURCE_MAXIMUMPOOLSIZE: 24 # Size of connections to Postgres. Should be higher that core pool size

      # Api container binarystore path
      # RP_BINARYSTORE_CONTAINER_PATH: /data/storage
      # RP_BINARYSTORE_PATH: /data/storage

      RP_BINARYSTORE_TYPE: minio
      RP_BINARYSTORE_MINIO_ENDPOINT: http://minio:9000
      RP_BINARYSTORE_MINIO_ACCESSKEY: minio
      RP_BINARYSTORE_MINIO_SECRETKEY: minio123

      # Data older than dates below will not migrate. ISO_LOCAL_DATE (yyyy-MM-dd)
      RP_LAUNCH_KEEPFROM: '2000-01-01'     # Keep launches from date in ISO_LOCAL_DATE (yyyy-MM-dd)
      RP_TEST_KEEPFROM: '2000-01-01'       # Keep test from date in ISO_LOCAL_DATE (yyyy-MM-dd). Example: '2000-01-01'
      RP_LOG_KEEPFROM: '2000-01-01'        # Keep logs from date in ISO_LOCAL_DATE (yyyy-MM-dd). Example: '2000-01-01'
      RP_ATTACH_KEEPFROM: '2000-01-01'     # Keep attachments from date in ISO_LOCAL_DATE (yyyy-MM-dd). Example: '2000-01-01'

      # Mongo params
      RP_MONGODB_URI: mongodb://user:password@host:port/database_name
```

Now start configured [docker-compose.yml](https://github.com/reportportal/data-migration/blob/data-migration/docker-compose.yml). 
```bash
$> docker-compose -p {reportportal_network} up -d --force-recreate
```
It runs a `Data-Migration` service. It can take a while, so now you have some time for vacation.
Take the [following steps](https://github.com/reportportal/reportportal/wiki/Migration-to-ReportPortal-v.5#3-checking-migration-status) to make sure that migration is finished.


## 3. Checking migration status
1. Service stops after migration finished.
2. Check migration status in **batch_job_execution** - table in PostgreSQL 
3. Check migration status for each step (e.g. Launch) in **batch_step_execution** - table in PostgreSQL 
More information about tables can be found [here](https://docs.spring.io/spring-batch/docs/current/reference/html/schema-appendix.html).

## 4. After migration

When the migration is finished:

* Generate analyzer indexes on the settings page for each project. The reason is updated scheme of indexes in Elasticsearch.
* Access tokens for each user is changed. Update access tokens in your tests property file. The reason is changes in Auth module of newer Spring versions and inconsistency with older.

## 5. Problems solving
If something went wrong the problem can be investigated via logs or contacting <support@reportportal.io>. Before migration restart the next steps should be done:

0.  Drop the following mongo collections: 'filterMapping', 'widgetMapping', 'optimizeTest'
1.  Remove all containers. (Be careful, this command will delete all containers running on the machine. If there are other containers that must not be deleted, don't use this command. Delete needed containers one by one)
```bash
docker rm -f $(docker ps -q)
```
2. Remove volumes
```bash
docker volume prune
```
3. Remove networks and configs
```bash
docker system prune
```
4. Delete mapped volumes from the local filesystem
5. Go through the installing steps again
