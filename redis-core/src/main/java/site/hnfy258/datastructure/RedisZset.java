package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.internal.Dict;
import site.hnfy258.internal.SkipList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Redis有序集合数据结构实现类
 * 
 * <p>实现了Redis的Sorted Set数据类型，提供高效的有序集合操作功能。
 * 使用双重数据结构实现O(1)的成员查找和O(log N)的有序范围查询：
 * <ul>
 *     <li>memberDict: 成员到分数的映射，支持O(1)查找</li>
 *     <li>skipList: 分数有序的跳表结构，支持范围查询</li>
 * </ul>
 * 
 * <p>主要功能包括：
 * <ul>
 *     <li>按分数排序的成员管理（ZADD/ZREM）</li>
 *     <li>按排名和分数范围的查询（ZRANGE/ZRANGEBYSCORE）</li>
 *     <li>成员分数的获取和更新</li>
 *     <li>支持Redis协议的序列化转换</li>
 * </ul>
 * 
 * @author hnfy258
 * @since 1.0.0
 */
@Setter
@Getter
public class RedisZset implements RedisData {
    
    /**
     * Zset节点，包含分数和成员信息，实现Comparable接口以支持跳表排序
     */
    public static class ZsetNode implements Comparable<ZsetNode> {
        private final double score;
        private final String member;
        
        public ZsetNode(final double score, final String member) {
            this.score = score;
            this.member = member;
        }
        
        public double getScore() {
            return score;
        }
        
        public String getMember() {
            return member;
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ZsetNode zsetNode = (ZsetNode) obj;
            return Double.compare(zsetNode.score, score) == 0 && 
                   Objects.equals(member, zsetNode.member);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(score, member);
        }

        @Override
        public int compareTo(ZsetNode other) {
            // 首先按分数排序
            int scoreCompare = Double.compare(this.score, other.score);
            if (scoreCompare != 0) {
                return scoreCompare;
            }
            // 分数相同时按成员字典序排序
            return this.member.compareTo(other.member);
        }
    }
    
    /** 数据过期时间，-1表示永不过期 */
    private volatile long timeout = -1;
    
    /** 成员到分数的映射，支持O(1)查找成员是否存在及其分数 */
    private Dict<String, Double> memberDict;
    
    /** 分数有序的跳表结构，支持范围查询和排序 */
    private SkipList<ZsetNode> skipList;
    
    /** 关联的Redis键名 */
    private RedisBytes key;
    
    public RedisZset() {
        memberDict = new Dict<>();
        skipList = new SkipList<>();
    }
    
    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(final long timeout) {
        this.timeout = timeout;
    }

    @Override
    public List<Resp> convertToResp() {
        final List<Resp> result = new ArrayList<>();
        if (memberDict.size() == 0) {
            return Collections.emptyList();
        }
        
        // 遍历所有成员，生成ZADD命令
        for (final Map.Entry<Object, Object> entry : memberDict.entrySet()) {
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof Double)) {
                continue;
            }
            
            final String member = (String) entry.getKey();
            final Double score = (Double) entry.getValue();
            
            final List<Resp> zaddCommand = new ArrayList<>();
            zaddCommand.add(new BulkString(RedisBytes.fromString("ZADD")));
            zaddCommand.add(new BulkString(key.getBytesUnsafe()));
            zaddCommand.add(new BulkString(RedisBytes.fromString(score.toString())));
            zaddCommand.add(new BulkString(RedisBytes.fromString(member)));
            
            result.add(new RespArray(zaddCommand.toArray(new Resp[0])));
        }
        return result;
    }

    
    /**
     * 向有序集合添加成员
     * 
     * @param score 分数
     * @param member 成员
     * @return 如果是新成员返回true，否则返回false
     */
    public boolean add(final double score, final Object member) {
        // 将成员转换为String
        final String memberStr = convertMemberToString(member);
        
        // 检查成员是否已存在
        final Double existingScore = (Double) memberDict.get(memberStr);
        if (existingScore != null) {
            // 如果分数相同，不需要更新
            if (Double.compare(existingScore, score) == 0) {
                return false;
            }
            // 移除旧的分数映射
            skipList.delete(existingScore, new ZsetNode(existingScore, memberStr));
        }
        
        // 添加新的映射
        memberDict.put(memberStr, score);
        skipList.insert(score, new ZsetNode(score, memberStr));
        
        return existingScore == null;
    }
    
    /**
     * 将成员对象转换为字符串
     */
    private String convertMemberToString(final Object member) {
        if (member instanceof RedisBytes) {
            return ((RedisBytes) member).getString();
        }
        return member.toString();
    }
    
    /**
     * 根据排名范围获取元素
     * 
     * @param start 开始位置（从0开始）
     * @param stop 结束位置
     * @return 节点列表
     */
    public List<ZsetNode> getRange(final int start, final int stop) {
        return skipList.getElementByRankRange(start, stop)
                      .stream()
                      .map(node -> (ZsetNode)node.member)
                      .collect(Collectors.toList());
    }
    
    /**
     * 根据分数范围获取元素
     * 
     * @param min 最小分数
     * @param max 最大分数
     * @return 节点列表
     */
    public List<ZsetNode> getRangeByScore(final double min, final double max) {
        return skipList.getElementByScoreRange(min, max)
                      .stream()
                      .map(node -> (ZsetNode)node.member)
                      .collect(Collectors.toList());
    }
    
    /**
     * 获取有序集合的大小
     * 
     * @return 元素数量
     */
    public long size() {
        return memberDict.size();
    }
    
    /**
     * 获取所有元素
     * 
     * @return 所有元素的迭代器
     */
    public Iterable<? extends Map.Entry<String, Double>> getAll() {
        final Map<String, Double> typedMap = new HashMap<>();
        for (final Map.Entry<Object, Object> entry : memberDict.entrySet()) {
            if (entry.getKey() instanceof String && entry.getValue() instanceof Double) {
                typedMap.put((String) entry.getKey(), (Double) entry.getValue());
            }
        }
        return typedMap.entrySet();
    }
    
    /**
     * 检查成员是否存在
     * 
     * @param member 成员
     * @return 如果存在返回true
     */
    public boolean contains(final Object member) {
        final String memberStr = convertMemberToString(member);
        return memberDict.containsKey(memberStr);
    }
    
    /**
     * 获取成员的分数
     * 
     * @param member 成员
     * @return 分数，如果不存在返回null
     */
    public Double getScore(final Object member) {
        final String memberStr = convertMemberToString(member);
        return (Double) memberDict.get(memberStr);
    }
    
    /**
     * 移除成员
     * 
     * @param member 成员
     * @return 如果移除成功返回true
     */
    public boolean remove(final Object member) {
        final String memberStr = convertMemberToString(member);
        final Double score = (Double) memberDict.remove(memberStr);
        if (score != null) {
            skipList.delete(score, new ZsetNode(score, memberStr));
            return true;
        }
        return false;
    }
}
