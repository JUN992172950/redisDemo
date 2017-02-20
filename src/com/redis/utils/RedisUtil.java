package com.redis.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.Transaction;

public class RedisUtil {
	// 数据�?
	private ShardedJedisPool shardedJedisPool;

	/**
	 * 执行器，{@link com.futurefleet.framework.base.redis.RedisUtil}的辅助类�?
	 * 它保证在执行操作之后释放数据源returnResource(jedis)
	 * 
	 * @version V1.0
	 * @author fengjc
	 * @param <T>
	 */
	abstract class Executor<T> {

		ShardedJedis jedis;
		ShardedJedisPool shardedJedisPool;

		public Executor(ShardedJedisPool shardedJedisPool) {
			this.shardedJedisPool = shardedJedisPool;
			jedis = this.shardedJedisPool.getResource();
		}

		/**
		 * 回调
		 * 
		 * @return 执行结果
		 */
		abstract T execute();

		/**
		 * 调用{@link #execute()}并返回执行结�?它保证在执行{@link #execute()}
		 * 之后释放数据源returnResource(jedis)
		 * 
		 * @return 执行结果
		 */
		@SuppressWarnings("deprecation")
		public T getResult() {
			T result = null;
			try {
				result = execute();
			} catch (Throwable e) {
				throw new RuntimeException("Redis execute exception", e);
			} finally {
				if (jedis != null) {
					shardedJedisPool.returnResource(jedis);
				}
			}
			return result;
		}
	}

	/**
	 * 删除模糊匹配的key
	 * 
	 * @param likeKey
	 *            模糊匹配的key
	 * @return 删除成功的条�?
	 */
	public long delKeysLike(final String likeKey) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				Collection<Jedis> jedisC = jedis.getAllShards();
				Iterator<Jedis> iter = jedisC.iterator();
				long count = 0;
				while (iter.hasNext()) {
					Jedis _jedis = iter.next();
					Set<String> keys = _jedis.keys(likeKey + "*");
					count += _jedis.del(keys.toArray(new String[keys.size()]));
				}
				return count;
			}
		}.getResult();
	}

	/**
	 * 删除
	 * 
	 * @param key
	 *            匹配的key
	 * @return 删除成功的条�?
	 */
	public Long delKey(final String key) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.del(key);
			}
		}.getResult();
	}

	/**
	 * 删除
	 * 
	 * @param keys
	 *            匹配的key的集�?
	 * @return 删除成功的条�?
	 */
	public Long delKeys(final String[] keys) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				Collection<Jedis> jedisC = jedis.getAllShards();
				Iterator<Jedis> iter = jedisC.iterator();
				long count = 0;
				while (iter.hasNext()) {
					Jedis _jedis = iter.next();
					count += _jedis.del(keys);
				}
				return count;
			}
		}.getResult();
	}

	/**
	 * 为给�?key 设置生存时间，当 key 过期�?生存时间�?0 )，它会被自动删除�?�?Redis 中，带有生存时间�?key
	 * 被称为�?可挥发�?(volatile)的�?
	 * 
	 * @param key
	 *            key
	 * @param expire
	 *            生命周期，单位为�?
	 * @return 1: 设置成功 0: 已经超时或key不存�?
	 */
	public Long expire(final String key, final int expire) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.expire(key, expire);
			}
		}.getResult();
	}

	/**
	 * �?��跨jvm的id生成器，利用了redis原子性操作的特点
	 * 
	 * @param key
	 *            id的key
	 * @return 返回生成的Id
	 */
	public long makeId(final String key) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				long id = jedis.incr(key);
				if ((id + 75807) >= Long.MAX_VALUE) {
					// 避免溢出，重置，getSet命令之前允许incr插队�?5807就是预留的插队空�?
					jedis.getSet(key, "0");
				}
				return id;
			}
		}.getResult();
	}

	/*
	 * ======================================Strings==============================
	 * ========
	 */

	/**
	 * 将字符串�?value 关联�?key �?如果 key 已经持有其他值， setString 就覆写旧值，无视类型�?
	 * 对于某个原本带有生存时间（TTL）的键来说， �?setString 成功在这个键上执行时�?这个键原有的 TTL 将被清除�?
	 * 时间复杂度：O(1)
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            string value
	 * @return 在设置操作成功完成时，才返回 OK �?
	 */
	public String setString(final String key, final String value) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				return jedis.set(key, value);
			}
		}.getResult();
	}

	/**
	 * 将�? value 关联�?key ，并�?key 的生存时间设�?expire (以秒为单�?�?如果 key 已经存在�?将覆写旧值�?
	 * 类似于以下两个命�? SET key value EXPIRE key expire # 设置生存时间
	 * 不同之处是这个方法是�?��原子�?atomic)操作，关联�?和设置生存时间两个动作会在同�?��间内完成，在 Redis 用作缓存时，非常实用�?
	 * 时间复杂度：O(1)
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            string value
	 * @param expire
	 *            生命周期
	 * @return 设置成功时返�?OK 。当 expire 参数不合法时，返回一个错误�?
	 */
	public String setString(final String key, final String value, final int expire) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				return jedis.setex(key, expire, value);
			}
		}.getResult();
	}

	/**
	 * �?key 的�?设为 value ，当且仅�?key 不存在�?若给定的 key 已经存在，则 setStringIfNotExists
	 * 不做任何动作�?时间复杂度：O(1)
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            string value
	 * @return 设置成功，返�?1 。设置失败，返回 0 �?
	 */
	public Long setStringIfNotExists(final String key, final String value) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.setnx(key, value);
			}
		}.getResult();
	}

	/**
	 * 返回 key �?��联的字符串�?。如�?key 不存在那么返回特殊�? nil �?假如 key 储存的�?不是字符串类型，返回�?��错误，因�?
	 * getString 只能用于处理字符串�?�?时间复杂�? O(1)
	 * 
	 * @param key
	 *            key
	 * @return �?key 不存在时，返�?nil ，否则，返回 key 的�?。如�?key 不是字符串类型，那么返回�?��错误�?
	 */
	public String getString(final String key) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				return jedis.get(key);
			}
		}.getResult();
	}

	/**
	 * 批量�?{@link #setString(String, String)}
	 * 
	 * @param pairs
	 *            键�?对数组{数组第一个元素为key，第二个元素为value}
	 * @return 操作状�?的集�?
	 */
	public List<Object> batchSetString(final List<Pair<String, String>> pairs) {
		return new Executor<List<Object>>(shardedJedisPool) {

			@Override
			List<Object> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				for (Pair<String, String> pair : pairs) {
					pipeline.set(pair.getKey(), pair.getValue());
				}
				return pipeline.syncAndReturnAll();
			}
		}.getResult();
	}

	/**
	 * 批量�?{@link #getString(String)}
	 * 
	 * @param keys
	 *            key数组
	 * @return value的集�?
	 */
	public List<String> batchGetString(final String[] keys) {
		return new Executor<List<String>>(shardedJedisPool) {

			@Override
			List<String> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				List<String> result = new ArrayList<String>(keys.length);
				List<Response<String>> responses = new ArrayList<Response<String>>(keys.length);
				for (String key : keys) {
					responses.add(pipeline.get(key));
				}
				pipeline.sync();
				for (Response<String> resp : responses) {
					result.add(resp.get());
				}
				return result;
			}
		}.getResult();
	}

	/*
	 * ======================================Hashes==============================
	 * ========
	 */

	/**
	 * 将哈希表 key 中的�?field 的�?设为 value �?如果 key 不存在，�?��新的哈希表被创建并进�?hashSet 操作�?如果�?
	 * field 已经存在于哈希表中，旧�?将被覆盖�?时间复杂�? O(1)
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            �?
	 * @param value
	 *            string value
	 * @return 如果 field 是哈希表中的�?��新建域，并且值设置成功，返回 1 。如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回
	 *         0 �?
	 */
	public Long hashSet(final String key, final String field, final String value) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.hset(key, field, value);
			}
		}.getResult();
	}
	
	public String ObjectSet(final String key, final Object object) {
		return new Executor<String>(shardedJedisPool) {
			@Override
			String execute() {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = null;
				byte[] byteArray = null;
				try {
					oos = new ObjectOutputStream(bos);
					oos.writeObject(object);
					byteArray = bos.toByteArray();
					oos.close();
					bos.close();
				} catch (IOException e) {
					return "nil";
				}
				return jedis.set(key.getBytes(), byteArray);
			}
		}.getResult();
	}

	public Object ObjectGet(final String key) {
		return new Executor<Object>(shardedJedisPool) {
			@Override
			Object execute() {
				byte[] bs = jedis.get(key.getBytes());
				ByteArrayInputStream bis = new ByteArrayInputStream(bs);
				ObjectInputStream inputStream;
				Object object = null;
				try {
					inputStream = new ObjectInputStream(bis);
					object = inputStream.readObject();
				} catch (IOException e) {
					return object;
				} catch (ClassNotFoundException e) {
					return object;
				}
				return object;
			}
		}.getResult();
	}

	/**
	 * 将哈希表 key 中的�?field 的�?设为 value �?如果 key 不存在，�?��新的哈希表被创建并进�?hashSet 操作�?如果�?
	 * field 已经存在于哈希表中，旧�?将被覆盖�?
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            �?
	 * @param value
	 *            string value
	 * @param expire
	 *            生命周期，单位为�?
	 * @return 如果 field 是哈希表中的�?��新建域，并且值设置成功，返回 1 。如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回
	 *         0 �?
	 */
	public Long hashSet(final String key, final String field, final String value, final int expire) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<Long> result = pipeline.hset(key, field, value);
				pipeline.expire(key, expire);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 返回哈希�?key 中给定域 field 的�?�?时间复杂�?O(1)
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            �?
	 * @return 给定域的值�?当给定域不存在或是给�?key 不存在时，返�?nil �?
	 */
	public String hashGet(final String key, final String field) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				return jedis.hget(key, field);
			}
		}.getResult();
	}

	/**
	 * 返回哈希�?key 中给定域 field 的�?�?如果哈希�?key 存在，同时设置这�?key 的生存时�?
	 * 
	 * @param key
	 *            key
	 * @param field
	 *            �?
	 * @param expire
	 *            生命周期，单位为�?
	 * @return 给定域的值�?当给定域不存在或是给�?key 不存在时，返�?nil �?
	 */
	public String hashGet(final String key, final String field, final int expire) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<String> result = pipeline.hget(key, field);
				pipeline.expire(key, expire);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 同时将多�?field-value (�?�?对设置到哈希�?key 中�? 时间复杂�? O(N) (N为fields的数�?
	 * 
	 * @param key
	 *            key
	 * @param hash
	 *            field-value的map
	 * @return 如果命令执行成功，返�?OK 。当 key 不是哈希�?hash)类型时，返回�?��错误�?
	 */
	public String hashMultipleSet(final String key, final Map<String, String> hash) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				return jedis.hmset(key, hash);
			}
		}.getResult();
	}

	/**
	 * 同时将多�?field-value (�?�?对设置到哈希�?key 中�?同时设置这个 key 的生存时�?
	 * 
	 * @param key
	 *            key
	 * @param hash
	 *            field-value的map
	 * @param expire
	 *            生命周期，单位为�?
	 * @return 如果命令执行成功，返�?OK 。当 key 不是哈希�?hash)类型时，返回�?��错误�?
	 */
	public String hashMultipleSet(final String key, final Map<String, String> hash, final int expire) {
		return new Executor<String>(shardedJedisPool) {

			@Override
			String execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<String> result = pipeline.hmset(key, hash);
				pipeline.expire(key, expire);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 返回哈希�?key 中，�?��或多个给定域的�?。如果给定的域不存在于哈希表，那么返回一�?nil 值�? 时间复杂�? O(N)
	 * (N为fields的数�?
	 * 
	 * @param key
	 *            key
	 * @param fields
	 *            field的数�?
	 * @return �?��包含多个给定域的关联值的表，表�?的排列顺序和给定域参数的请求顺序�?���?
	 */
	public List<String> hashMultipleGet(final String key, final String... fields) {
		return new Executor<List<String>>(shardedJedisPool) {

			@Override
			List<String> execute() {
				return jedis.hmget(key, fields);
			}
		}.getResult();
	}

	/**
	 * 返回哈希�?key 中，�?��或多个给定域的�?。如果给定的域不存在于哈希表，那么返回一�?nil 值�? 同时设置这个 key 的生存时�?
	 * 
	 * @param key
	 *            key
	 * @param fields
	 *            field的数�?
	 * @param expire
	 *            生命周期，单位为�?
	 * @return �?��包含多个给定域的关联值的表，表�?的排列顺序和给定域参数的请求顺序�?���?
	 */
	public List<String> hashMultipleGet(final String key, final int expire, final String... fields) {
		return new Executor<List<String>>(shardedJedisPool) {

			@Override
			List<String> execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<List<String>> result = pipeline.hmget(key, fields);
				pipeline.expire(key, expire);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #hashMultipleSet(String, Map)}，在管道中执�?
	 * 
	 * @param pairs
	 *            多个hash的多个field
	 * @return 操作状�?的集�?
	 */
	public List<Object> batchHashMultipleSet(final List<Pair<String, Map<String, String>>> pairs) {
		return new Executor<List<Object>>(shardedJedisPool) {

			@Override
			List<Object> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				for (Pair<String, Map<String, String>> pair : pairs) {
					pipeline.hmset(pair.getKey(), pair.getValue());
				}
				return pipeline.syncAndReturnAll();
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #hashMultipleSet(String, Map)}，在管道中执�?
	 * 
	 * @param data
	 *            Map<String, Map<String, String>>格式的数�?
	 * @return 操作状�?的集�?
	 */
	public List<Object> batchHashMultipleSet(final Map<String, Map<String, String>> data) {
		return new Executor<List<Object>>(shardedJedisPool) {

			@Override
			List<Object> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				for (Map.Entry<String, Map<String, String>> iter : data.entrySet()) {
					pipeline.hmset(iter.getKey(), iter.getValue());
				}
				return pipeline.syncAndReturnAll();
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #hashMultipleGet(String, String...)}，在管道中执�?
	 * 
	 * @param pairs
	 *            多个hash的多个field
	 * @return 执行结果的集�?
	 */
	public List<List<String>> batchHashMultipleGet(final List<Pair<String, String[]>> pairs) {
		return new Executor<List<List<String>>>(shardedJedisPool) {

			@Override
			List<List<String>> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				List<List<String>> result = new ArrayList<List<String>>(pairs.size());
				List<Response<List<String>>> responses = new ArrayList<Response<List<String>>>(pairs.size());
				for (Pair<String, String[]> pair : pairs) {
					responses.add(pipeline.hmget(pair.getKey(), pair.getValue()));
				}
				pipeline.sync();
				for (Response<List<String>> resp : responses) {
					result.add(resp.get());
				}
				return result;
			}
		}.getResult();

	}

	/**
	 * 返回哈希�?key 中，�?��的域和�?。在返回值里，紧跟每个域�?field
	 * name)之后是域的�?(value)，所以返回�?的长度是哈希表大小的两�?�?时间复杂�? O(N)
	 * 
	 * @param key
	 *            key
	 * @return 以列表形式返回哈希表的域和域的�?。若 key 不存在，返回空列表�?
	 */
	public Map<String, String> hashGetAll(final String key) {
		return new Executor<Map<String, String>>(shardedJedisPool) {

			@Override
			Map<String, String> execute() {
				return jedis.hgetAll(key);
			}
		}.getResult();
	}

	/**
	 * 返回哈希�?key 中，�?��的域和�?。在返回值里，紧跟每个域�?field
	 * name)之后是域的�?(value)，所以返回�?的长度是哈希表大小的两�?�?同时设置这个 key 的生存时�?
	 * 
	 * @param key
	 *            key
	 * @param expire
	 *            生命周期，单位为�?
	 * @return 以列表形式返回哈希表的域和域的�?。若 key 不存在，返回空列表�?
	 */
	public Map<String, String> hashGetAll(final String key, final int expire) {
		return new Executor<Map<String, String>>(shardedJedisPool) {

			@Override
			Map<String, String> execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<Map<String, String>> result = pipeline.hgetAll(key);
				pipeline.expire(key, expire);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #hashGetAll(String)}
	 * 
	 * @param keys
	 *            key的数�?
	 * @return 执行结果的集�?
	 */
	public List<Map<String, String>> batchHashGetAll(final String... keys) {
		return new Executor<List<Map<String, String>>>(shardedJedisPool) {

			@Override
			List<Map<String, String>> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				List<Map<String, String>> result = new ArrayList<Map<String, String>>(keys.length);
				List<Response<Map<String, String>>> responses = new ArrayList<Response<Map<String, String>>>(keys.length);
				for (String key : keys) {
					responses.add(pipeline.hgetAll(key));
				}
				pipeline.sync();
				for (Response<Map<String, String>> resp : responses) {
					result.add(resp.get());
				}
				return result;
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #hashMultipleGet(String, String...)}，与
	 * {@link #batchHashGetAll(String...)}不同的是，返回�?为Map类型
	 * 
	 * @param keys
	 *            key的数�?
	 * @return 多个hash的所有filed和value
	 */
	public Map<String, Map<String, String>> batchHashGetAllForMap(final String... keys) {
		return new Executor<Map<String, Map<String, String>>>(shardedJedisPool) {

			@Override
			Map<String, Map<String, String>> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();

				// 设置map容量防止rehash
				int capacity = 1;
				while ((int) (capacity * 0.75) <= keys.length) {
					capacity <<= 1;
				}
				Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>(capacity);
				List<Response<Map<String, String>>> responses = new ArrayList<Response<Map<String, String>>>(keys.length);
				for (String key : keys) {
					responses.add(pipeline.hgetAll(key));
				}
				pipeline.sync();
				for (int i = 0; i < keys.length; ++i) {
					result.put(keys[i], responses.get(i).get());
				}
				return result;
			}
		}.getResult();
	}

	/*
	 * ======================================List================================
	 * ======
	 */

	/**
	 * 将一个或多个�?value 插入到列�?key 的表�?�?���?�?
	 * 
	 * @param key
	 *            key
	 * @param values
	 *            value的数�?
	 * @return 执行 listPushTail 操作后，表的长度
	 */
	public Long listPushTail(final String key, final String... values) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.rpush(key, values);
			}
		}.getResult();
	}

	/**
	 * 将一个或多个�?value 插入到列�?key 的表�?
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            string value
	 * @return 执行 listPushHead 命令后，列表的长度�?
	 */
	public Long listPushHead(final String key, final String value) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.lpush(key, value);
			}
		}.getResult();
	}

	/**
	 * 将一个或多个�?value 插入到列�?key 的表�? 当列表大于指定长度是就对列表进行修剪(trim)
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            string value
	 * @param size
	 *            链表超过这个长度就修剪元�?
	 * @return 执行 listPushHeadAndTrim 命令后，列表的长度�?
	 */
	public Long listPushHeadAndTrim(final String key, final String value, final long size) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				Pipeline pipeline = jedis.getShard(key).pipelined();
				Response<Long> result = pipeline.lpush(key, value);
				// 修剪列表元素, 如果 size - 1 �?end 下标还要大，Redis�?size 的�?设置�?end �?
				pipeline.ltrim(key, 0, size - 1);
				pipeline.sync();
				return result.get();
			}
		}.getResult();
	}

	/**
	 * 批量的{@link #listPushTail(String, String...)}，以锁的方式实现
	 * 
	 * @param key
	 *            key
	 * @param values
	 *            value的数�?
	 * @param delOld
	 *            如果key存在，是否删除它。true 删除；false: 不删除，只是在行尾追�?
	 */
	public void batchListPushTail(final String key, final String[] values, final boolean delOld) {
		new Executor<Object>(shardedJedisPool) {

			@Override
			Object execute() {
				if (delOld) {
					RedisLock lock = new RedisLock(key, shardedJedisPool);
					lock.lock();
					try {
						Pipeline pipeline = jedis.getShard(key).pipelined();
						pipeline.del(key);
						for (String value : values) {
							pipeline.rpush(key, value);
						}
						pipeline.sync();
					} finally {
						lock.unlock();
					}
				} else {
					jedis.rpush(key, values);
				}
				return null;
			}
		}.getResult();
	}

	/**
	 * 同{@link #batchListPushTail(String, String[], boolean)}
	 * ,不同的是利用redis的事务特性来实现
	 * 
	 * @param key
	 *            key
	 * @param values
	 *            value的数�?
	 * @return null
	 */
	public Object updateListInTransaction(final String key, final List<String> values) {
		return new Executor<Object>(shardedJedisPool) {

			@Override
			Object execute() {
				Transaction transaction = jedis.getShard(key).multi();
				transaction.del(key);
				for (String value : values) {
					transaction.rpush(key, value);
				}
				transaction.exec();
				return null;
			}
		}.getResult();
	}

	/**
	 * 在key对应list的尾部部添加字符串元�?如果key存在，什么也不做
	 * 
	 * @param key
	 *            key
	 * @param values
	 *            value的数�?
	 * @return 执行insertListIfNotExists后，表的长度
	 */
	public Long insertListIfNotExists(final String key, final String[] values) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				RedisLock lock = new RedisLock(key, shardedJedisPool);
				lock.lock();
				try {
					if (!jedis.exists(key)) {
						return jedis.rpush(key, values);
					}
				} finally {
					lock.unlock();
				}
				return 0L;
			}
		}.getResult();
	}

	/**
	 * 返回list�?��元素，下标从0�?��，负值表示从后面计算�?1表示倒数第一个元素，key不存在返回空列表
	 * 
	 * @param key
	 *            key
	 * @return list�?��元素
	 */
	public List<String> listGetAll(final String key) {
		return new Executor<List<String>>(shardedJedisPool) {

			@Override
			List<String> execute() {
				return jedis.lrange(key, 0, -1);
			}
		}.getResult();
	}

	/**
	 * 返回指定区间内的元素，下标从0�?��，负值表示从后面计算�?1表示倒数第一个元素，key不存在返回空列表
	 * 
	 * @param key
	 *            key
	 * @param beginIndex
	 *            下标�?��索引（包含）
	 * @param endIndex
	 *            下标结束索引（不包含�?
	 * @return 指定区间内的元素
	 */
	public List<String> listRange(final String key, final long beginIndex, final long endIndex) {
		return new Executor<List<String>>(shardedJedisPool) {

			@Override
			List<String> execute() {
				return jedis.lrange(key, beginIndex, endIndex - 1);
			}
		}.getResult();
	}

	/**
	 * �?��获得多个链表的数�?
	 * 
	 * @param keys
	 *            key的数�?
	 * @return 执行结果
	 */
	public Map<String, List<String>> batchGetAllList(final List<String> keys) {
		return new Executor<Map<String, List<String>>>(shardedJedisPool) {

			@Override
			Map<String, List<String>> execute() {
				ShardedJedisPipeline pipeline = jedis.pipelined();
				Map<String, List<String>> result = new HashMap<String, List<String>>();
				List<Response<List<String>>> responses = new ArrayList<Response<List<String>>>(keys.size());
				for (String key : keys) {
					responses.add(pipeline.lrange(key, 0, -1));
				}
				pipeline.sync();
				for (int i = 0; i < keys.size(); ++i) {
					result.put(keys.get(i), responses.get(i).get());
				}
				return result;
			}
		}.getResult();
	}

	/*
	 * ======================================Pub/Sub==============================
	 * ========
	 */

	/**
	 * 将信�?message 发�?到指定的频道 channel�?时间复杂度：O(N+M)，其�?N 是频�?channel 的订阅�?数量，�? M
	 * 则是使用模式订阅(subscribed patterns)的客户端的数量�?
	 * 
	 * @param channel
	 *            频道
	 * @param message
	 *            信息
	 * @return 接收到信�?message 的订阅�?数量�?
	 */
	public Long publish(final String channel, final String message) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				Jedis _jedis = jedis.getShard(channel);
				return _jedis.publish(channel, message);
			}

		}.getResult();
	}

	/**
	 * 订阅给定的一个频道的信息�?
	 * 
	 * @param jedisPubSub
	 *            监听�?
	 * @param channel
	 *            频道
	 */
	public void subscribe(final JedisPubSub jedisPubSub, final String channel) {
		new Executor<Object>(shardedJedisPool) {

			@Override
			Object execute() {
				Jedis _jedis = jedis.getShard(channel);
				// 注意subscribe是一个阻塞操作，因为当前线程要轮询Redis的响应然后调用subscribe
				_jedis.subscribe(jedisPubSub, channel);
				return null;
			}
		}.getResult();
	}

	/**
	 * 取消订阅
	 * 
	 * @param jedisPubSub
	 *            监听�?
	 */
	public void unSubscribe(final JedisPubSub jedisPubSub) {
		jedisPubSub.unsubscribe();
	}

	/*
	 * ======================================Sorted
	 * set=================================
	 */

	/**
	 * 将一�?member 元素及其 score 值加入到有序�?key 当中�?
	 * 
	 * @param key
	 *            key
	 * @param score
	 *            score 值可以是整数值或双精度浮点数�?
	 * @param member
	 *            有序集的成员
	 * @return 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员�?
	 */
	public Long addWithSortedSet(final String key, final double score, final String member) {
		return new Executor<Long>(shardedJedisPool) {

			@Override
			Long execute() {
				return jedis.zadd(key, score, member);
			}
		}.getResult();
	}

	/**
	 * 返回有序�?key 中， score 值介�?max �?min 之间(默认包括等于 max �?min )的所有的成员�?有序集成员按
	 * score 值�?�?从大到小)的次序排列�?
	 * 
	 * @param key
	 *            key
	 * @param max
	 *            score�?���?
	 * @param min
	 *            score�?���?
	 * @return 指定区间内，带有 score �?可�?)的有序集成员的列�?
	 */
	public Set<String> revrangeByScoreWithSortedSet(final String key, final double max, final double min) {
		return new Executor<Set<String>>(shardedJedisPool) {

			@Override
			Set<String> execute() {
				return jedis.zrevrangeByScore(key, max, min);
			}
		}.getResult();
	}
	
	public Boolean existKey(final String key) {
		return new Executor<Boolean>(shardedJedisPool) {
			Boolean execute() {
				return jedis.exists(key);
			};
		}.getResult();
	}
	
	public Boolean existKeyField(final String key, final String field) {
		return new Executor<Boolean>(shardedJedisPool) {
			Boolean execute() {
				return jedis.hexists(key, field);
			};
		}.getResult();
	}

	/*
	 * ======================================Other================================
	 * ======
	 */

	/**
	 * 设置数据�?
	 * 
	 * @param shardedJedisPool
	 *            数据�?
	 */
	public void setShardedJedisPool(ShardedJedisPool shardedJedisPool) {
		this.shardedJedisPool = shardedJedisPool;
	}

	/**
	 * 构�?Pair键�?�?
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return 键�?�?
	 */
	public <K, V> Pair<K, V> makePair(K key, V value) {
		return new Pair<K, V>(key, value);
	}

	/**
	 * 键�?�?
	 * 
	 * @version V1.0
	 * @author fengjc
	 * @param <K>
	 *            key
	 * @param <V>
	 *            value
	 */
	public class Pair<K, V> {

		private K key;
		private V value;

		public Pair(K key, V value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}
	}
}
