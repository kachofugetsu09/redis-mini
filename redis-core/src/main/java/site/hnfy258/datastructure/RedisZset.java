package site.hnfy258.datastructure;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.internal.Dict;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis有序集合实现类
 * 使用双重数据结构实现高效的有序集合操作：
 * 1. memberToScore: 成员到分数的映射，支持O(1)查找
 * 2. scoreToMembers: 分数到成员集合的有序映射，支持范围查询
 * 
 * @author hnfy258
 */
@Setter
@Getter
public class RedisZset implements RedisData {
    
    /**
     * Zset节点，包含分数和成员信息
     */
    public static class ZsetNode {
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
    }
    
    private volatile long timeout = -1;
    
    // 成员到分数的映射，支持O(1)查找成员是否存在及其分数
    private final Dict<String, Double> memberToScore;
    
    // 分数到成员集合的有序映射，支持范围查询和排序
    // 使用Set是因为相同分数可能有多个成员
    private final ConcurrentSkipListMap<Double, Set<String>> scoreToMembers;
    
    // 用于生成唯一序列号，处理相同分数时的字典序排序
    private final AtomicLong sequenceGenerator;    private RedisBytes key;
    
    public RedisZset() {
        memberToScore = new Dict<>();
        scoreToMembers = new ConcurrentSkipListMap<>();
        sequenceGenerator = new AtomicLong(0);
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
        if (memberToScore.size() == 0) {
            return Collections.emptyList();
        }
        
        // 1. 遍历所有成员，生成ZADD命令
        for (final Map.Entry<Object, Object> entry : memberToScore.entrySet()) {
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
    }    /**
     * 向有序集合添加成员
     * 
     * @param score 分数
     * @param member 成员
     * @return 如果是新成员返回true，否则返回false
     */
    public boolean add(final double score, final Object member) {
        // 1. 将成员转换为String
        final String memberStr = convertMemberToString(member);
        
        // 2. 检查成员是否已存在
        final Double existingScore = (Double) memberToScore.get(memberStr);
        if (existingScore != null) {
            // 3. 如果分数相同，不需要更新
            if (Double.compare(existingScore, score) == 0) {
                return false;
            }
            // 4. 移除旧的分数映射
            removeFromScoreToMembers(existingScore, memberStr);
        }
        
        // 5. 添加新的映射
        memberToScore.put(memberStr, score);
        addToScoreToMembers(score, memberStr);
        
        return existingScore == null; // 如果之前不存在则返回true
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
     * 添加成员到分数映射中
     */
    private void addToScoreToMembers(final double score, final String member) {
        scoreToMembers.computeIfAbsent(score, k -> new TreeSet<>()).add(member);
    }
    
    /**
     * 从分数映射中移除成员
     */
    private void removeFromScoreToMembers(final double score, final String member) {
        final Set<String> members = scoreToMembers.get(score);
        if (members != null) {
            members.remove(member);
            if (members.isEmpty()) {
                scoreToMembers.remove(score);
            }
        }
    }    /**
     * 根据排名范围获取元素
     * 
     * @param start 开始位置（从0开始）
     * @param stop 结束位置
     * @return 节点列表
     */
    public List<ZsetNode> getRange(final int start, final int stop) {
        final List<ZsetNode> result = new ArrayList<>();
        final List<ZsetNode> allNodes = getAllNodesSorted();
        
        if (allNodes.isEmpty()) {
            return result;
        }
        
        // 1. 处理负数索引
        final int size = allNodes.size();
        int actualStart = start < 0 ? Math.max(0, size + start) : start;
        int actualStop = stop < 0 ? Math.max(-1, size + stop) : stop;
        
        // 2. 确保索引在有效范围内
        actualStart = Math.max(0, actualStart);
        actualStop = Math.min(size - 1, actualStop);
        
        // 3. 提取指定范围的元素
        for (int i = actualStart; i <= actualStop && i < size; i++) {
            result.add(allNodes.get(i));
        }
        
        return result;
    }
    
    /**
     * 获取所有节点的排序列表
     */
    private List<ZsetNode> getAllNodesSorted() {
        final List<ZsetNode> result = new ArrayList<>();
        
        // 1. 按分数顺序遍历
        for (final Map.Entry<Double, Set<String>> entry : scoreToMembers.entrySet()) {
            final double score = entry.getKey();
            final Set<String> members = entry.getValue();
            
            // 2. 对相同分数的成员按字典序排序
            for (final String member : members) {
                result.add(new ZsetNode(score, member));
            }
        }
        
        return result;
    }    /**
     * 根据分数范围获取元素
     * 
     * @param min 最小分数
     * @param max 最大分数
     * @return 节点列表
     */
    public List<ZsetNode> getRangeByScore(final double min, final double max) {
        final List<ZsetNode> result = new ArrayList<>();
        
        // 1. 使用ConcurrentSkipListMap的subMap方法获取指定分数范围
        final NavigableMap<Double, Set<String>> subMap = 
            scoreToMembers.subMap(min, true, max, true);
        
        // 2. 将结果转换为ZsetNode列表
        for (final Map.Entry<Double, Set<String>> entry : subMap.entrySet()) {
            final double score = entry.getKey();
            final Set<String> members = entry.getValue();
            
            // 3. 对相同分数的成员按字典序排序
            for (final String member : members) {
                result.add(new ZsetNode(score, member));
            }
        }
        
        return result;
    }    /**
     * 获取有序集合的大小
     * 
     * @return 元素数量
     */
    public int size() {
        return memberToScore.size();
    }
    
    /**
     * 获取所有元素
     * 
     * @return 所有元素的迭代器
     */
    public Iterable<? extends Map.Entry<String, Double>> getAll() {
        // 1. 创建一个新的包含正确类型的集合
        final Map<String, Double> typedMap = new HashMap<>();
        for (final Map.Entry<Object, Object> entry : memberToScore.entrySet()) {
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
        return memberToScore.containsKey(memberStr);
    }
    
    /**
     * 获取成员的分数
     * 
     * @param member 成员
     * @return 分数，如果不存在返回null
     */
    public Double getScore(final Object member) {
        final String memberStr = convertMemberToString(member);
        return (Double) memberToScore.get(memberStr);
    }
    
    /**
     * 移除成员
     * 
     * @param member 成员
     * @return 如果移除成功返回true
     */
    public boolean remove(final Object member) {
        final String memberStr = convertMemberToString(member);
        final Double score = (Double) memberToScore.remove(memberStr);
        if (score != null) {
            removeFromScoreToMembers(score, memberStr);
            return true;
        }
        return false;
    }
}
