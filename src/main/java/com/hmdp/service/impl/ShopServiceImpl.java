package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jodd.util.StringUtil;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.Metrics;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
//                this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
        Shop shop = cacheClient.queryWithMutex(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
                this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //逻辑过期解决缓存击穿
//        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
//                this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //7.返回
        return Result.ok(shop);
    }


//
//    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        //1.查询店铺数据
//        Shop shop = getById(id);
//        Thread.sleep(200);//模拟缓存重建耗时
//        //2.封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusMinutes(expireSeconds));
//        //3.写入redis
//        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }

//    @Override
//    public Result queryShopType(Integer typeId, Integer current, Double x, Double y) {
//        //1.判断 是否需要根据坐标查询
//        if (x == null || y == null) {
//            //不需要坐标查询 按照数据库查询
//
//            // 根据类型分页查询
//            Page<Shop> page = query()
//                    .eq("type_id", typeId)
//                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
//            // 返回数据
//            return Result.ok(page.getRecords());
//        }
//        //2.计算分页参数
//        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
//        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
//        //3.查询redis 按照距离排序 分页 结果 shopId distance
//        String key = RedisConstants.SHOP_GEO_KEY + typeId;
////        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() //GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
////                .search(key,
////                        GeoReference.fromCoordinate(x, y),
////                        new Distance(5000),
////                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
////                );
//        //4.解析出id
//        if (results == null) {
//            return Result.ok(Collections.emptyList());
//        }
//        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
//        //4.1 截取 from - end 的 部分
//        List<Long> ids = new ArrayList<>(list.size());
//        Map<String, Distance> distanceMap = new HashMap<>(list.size());
//        list.stream().skip(from).forEach(result -> {
//            //4.2.获取店铺id
//            String shopIdStr = result.getContent().getName();
//            //4.3.获取距离
//            Distance distance = result.getDistance();
//            distanceMap.put(shopIdStr, distance);
//        });
//        //5.根据id 查询出shop
//        String idStr = StrUtil.join("," , ids);
//        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
//        for (Shop shop: shops) {
//            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
//        }
//
//        //6.返回
//        return Result.ok(shops);
//    }

    @Override
    public Result queryShopType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            //不需要坐标查询 按照数据库查询

            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询redis 按照距离排序分页（改用 GEORADIUS 命令）
        //GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE → 改用 GEORADIUS 实现相同效果
        String key = RedisConstants.SHOP_GEO_KEY + typeId;

        //3.1 构建圆形范围查询参数（中心点坐标 + 半径）
        Circle circle = new Circle(new Point(x, y), new Distance(5000, Metrics.METERS));

        //3.2 设置查询参数：包含距离、分页终点限制（Redis 3.2 需要内存分页）
        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs
                .newGeoRadiusArgs()
                .includeDistance()          // 包含距离信息
                .sortAscending()            // 按距离升序排序
                .limit(end);                // 限制总查询数量

        //3.3 执行 GEO 查询
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .radius(key, circle, args); // 使用 radius() 代替 search()

        //4.解析结果
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        //4.1 内存分页处理（Redis 3.2 不支持服务端分页）
        List<Long> ids = new ArrayList<>(SystemConstants.DEFAULT_PAGE_SIZE);
        Map<String, Distance> distanceMap = new HashMap<>(SystemConstants.DEFAULT_PAGE_SIZE);

        // 截取 from ~ end 范围结果
        list.stream()
                .skip(from)  // 跳过前面 N 条
                .limit(SystemConstants.DEFAULT_PAGE_SIZE)  // 取当前页数量
                .forEach(result -> {
                    String shopIdStr = result.getContent().getName();
                    ids.add(Long.valueOf(shopIdStr));
                    distanceMap.put(shopIdStr, result.getDistance());
                });

        //5.根据id查询店铺并保持顺序
        if (ids.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")")  // 保持 ID 顺序
                .list();

        //5.1 填充距离信息
        shops.forEach(shop ->
                shop.setDistance(distanceMap.get(shop.getId().toString()).getValue())
        );

        //6.返回结果
        return Result.ok(shops);
    }
}
