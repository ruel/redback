#### Why this tool?

Since the function **MIGRATE** is not available for redis < 2.6.x, it's almost impossible to migrate data from EC2 redis to ElastiCache redis without downtime (or data loss).

#### Dependencies

* redis
* go

#### Installation

```
$ go get github.com/ruel/redback
```

#### Usage

```
redback [-src <source-host>] [-sp <source-port>] -dest <destination-host> [-dp <destination-port>]
  -dest="none": Destination redis server hostname
  -dp=6379: Destination redis server port
  -sp=6379: Source redis server port
  -src="127.0.0.1": Source redis server hostname
  -tc=10: Connection timeout in seconds
  -tr=5: Read timeout in seconds
  -tw=5: Write timeout in seconds
```