package mini_jedis.v4;

public enum Command {
	SET, GET, // string
	RPUSH, LRANGE // list
	//HGET, HSET, HMSET, HGETALL, // hash
	//SADD, SMEMBERS, SREM, // set
	//ZADD, ZRANGE, ZRANGEBYLEX // zset
}
