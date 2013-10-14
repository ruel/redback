package main

import (
    "os"
    "fmt"
    "flag"
    "time"
    "github.com/garyburd/redigo/redis"
)

func main() {
    
    // Vars
    var src, dest string
    var sp, dp, tc, tr, tw int
    
    // Flag declaration
    // For source
    flag.StringVar(&src, "src", "127.0.0.1", "Source redis server hostname")
    flag.IntVar(&sp, "sp", 6379, "Source redis server port")
    
    // For destination
    flag.StringVar(&dest, "dest", "none", "Destination redis server hostname")
    flag.IntVar(&dp, "dp", 6379, "Destination redis server port")
    
    // Timeouts
    flag.IntVar(&tc, "tc", 10, "Connection timeout in seconds")
    flag.IntVar(&tr, "tr", 5, "Read timeout in seconds")
    flag.IntVar(&tw, "tw", 5, "Write timeout in seconds")
    
    // Do the flag parsing
    flag.Usage = usage
    flag.Parse()
    
    // Check if there's a -dest flag
    if dest == "none" {
        usage()
    }
    
    // Format addresses
    srcaddr := fmt.Sprintf("%s:%d", src, sp)
    destaddr := fmt.Sprintf("%s:%d", dest, dp)
    
    // Set timeouts
    connt := time.Duration(tc) * time.Second
    readt := time.Duration(tr) * time.Second
    writet := time.Duration(tw) * time.Second
    
    // Connect to Redis Source
    srcc, err := redis.DialTimeout("tcp", srcaddr, connt, readt, writet)
    
    // Exit if connection failed (source)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot connect to source redis server: %v\n", err)
        os.Exit(-1)
    }
    
    // Connect to Redis Destination
    destc, err := redis.DialTimeout("tcp", destaddr, connt, readt, writet)
    
    // Exit if connection failed (destination)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Cannot connect to destination redis server: %v\n", err)
        os.Exit(-1)
    }
    
    // Go through each keys in source
    srckeys, _ := redis.Strings(srcc.Do("KEYS", "*"))
    
    for _, key := range srckeys {
        
        // Get the object type of each key
        rtype, _ := redis.String(srcc.Do("TYPE", key))
        
        fmt.Println("Migrating", key, "of type", rtype)
        
        // Map with the correct object data type
        switch rtype {
        case "string":
            
            // Raw key-value
            val, _ := redis.String(srcc.Do("GET", key))
            destc.Do("SET", key, val)
            break
        case "list":
            
            // Lists
            val, _ := redis.Strings(srcc.Do("LRANGE", key, 0, -1))
            
            for _, v := range val {
                destc.Do("RPUSH", key, v)
            }
            break
        case "set":
            
            // Set
            val, _ := redis.Strings(srcc.Do("SMEMBERS", key))
            
            for _, v := range val {
                destc.Do("SADD", key, v)
            }
            break
        case "zset":
            
            // Sorted Set
            val, _ := redis.Strings(srcc.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
            
            // Since this zset will include scores we need to adapt
            zval := ""
            for i, v := range val {
                if (i + 1) % 2 == 1 {
                    
                    // This is a value
                    zval = v
                } else {
                    
                    // This is a score
                    // And here we do our thing
                    destc.Do("ZADD", key, v, zval)
                }
            }
            break
        case "hash":
            
            // Hashes, finally
            val, _ := redis.Strings(srcc.Do("HGETALL", key))
            
            // Do the same like in zset
            hfield := ""
            for i, v := range val {
                if (i + 1) % 2 == 1 {
                    
                    // Value
                    hfield = v
                } else {
                    
                    // And field
                    destc.Do("HSET", key, hfield, v)
                }
            }
            break
        default:
            
            // What?
            fmt.Println("Unknown type for", key)
        }
        
        // Check TTL
        checkttl(key, srcc, destc);
    }
    
    // Done
    fmt.Println("Done migrating keys from", srcaddr, "to", destaddr)
    
    // Cleanup
    srcc.Close()
    destc.Close()
}

// Check for key expiration and set expiry. This is a problematic one though, and may be inaccurate for a second at most
// (but I'm pretty confident it'll hardly reach that, since one command will just take a couple of ms)
func checkttl(key string, src, dst redis.Conn) {
    ttl, _ := redis.Int(src.Do("TTL", key))
    
    // Expire the key if it's greater than 0
    if ttl > 0 {
        fmt.Println("Setting expiration of", key)
        dst.Do("EXPIRE", key, ttl)
    }
}

// For usage
func usage() {
    fmt.Fprintf(os.Stderr, "Usage: redback [-src <source-host>] [-sp <source-port>] -dest <destination-host> [-dp <destination-port>]\n")
    flag.PrintDefaults()
    os.Exit(2)
}