package com.hunt.common.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 使用redis来实现分布式锁
 */
@Component
public class RedisUtil {
    @Autowired
    private StringRedisTemplate template;

    public boolean lock(String key, String value) {
        if (template.opsForValue().setIfAbsent(key, value, 180, TimeUnit.SECONDS))
            return true;
        //-1:没有设置过期时间 -2:没有指定的key
        //为了防止获取锁时,客户端设置了value,但还没有来得及设置过期时间,客户端就挂掉的情况
        if (template.boundValueOps(key).getExpire() <= 0)
            template.opsForValue().setIfAbsent(key, value, 180, TimeUnit.SECONDS);
        return false;
    }

    /**
     * 要确保redis开启了事务,否则multi exec命令要放在SessionCallback中执行,
     * 否则multi与exec命令是在不同的session中执行的,起不到事务的作用,
     * 也会导致exec执行报错
     */
    public boolean unlock(String key, String v) {
        SessionCallback<Boolean> callback = new SessionCallback<Boolean>() {
            @Override
            public <K, V> Boolean execute(RedisOperations<K, V> operations) throws DataAccessException {
                while (true) {
                    template.watch(key);
                    if (v.equals(template.opsForValue().get(key))) {
                        template.multi();
                        template.opsForValue().getOperations().delete(key);
                        List<Object> list = template.exec();
                        if (list.size() == 0) {//说明已被修改过,即使修改的值和原来的一样
                            continue;
                        }
                        return true;
                    }
                    template.unwatch();
                    return false;
                }
            }
        };
        return template.execute(callback);
    }
}