package site.hnfy258.internal;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 改进版跳表(SkipList) - Redis风格的有序集合核心数据结构实现
 *
 * <p>相比传统跳表实现的主要改进：</p>
 * <ul>
 *   <li><strong>后向指针优化</strong>: 支持从任意节点反向遍历，提升范围查询性能</li>
 *   <li><strong>跨度计数</strong>: 每个前向指针记录跨越的节点数，支持O(log N)的排名计算</li>
 *   <li><strong>分数和成员的双重排序</strong>: 当分数相同时按成员字典序排序，保证唯一性</li>
 *   <li><strong>负数索引支持</strong>: 支持类似Python的负数下标访问</li>
 * </ul>
 *
 * <p>主要特性：</p>
 * <ul>
 *   <li>时间复杂度：
 *     <ul>
 *       <li>插入/删除/查找：平均O(log N)</li>
 *       <li>按排名/分数范围查找：O(log N)</li>
 *       <li>获取排名：O(log N)</li>
 *     </ul>
 *   </li>
 *   <li>空间复杂度：平均O(N)，每个节点平均1.33个指针</li>
 *   <li>线程安全：设计为在Redis的单线程命令执行模型下使用，不需要同步</li>
 * </ul>
 *
 * @author hnfy258
 * @since 1.0
 */
@Slf4j
public class SkipList<T extends Comparable<T>> implements Serializable {
    /** 最大层数，Redis默认值 */
    private static final int MAX_LEVEL = 32;
    
    /** 层数增长概率，Redis默认值 */
    private static final double P = 0.25;
    
    /** 头节点，哨兵节点，分数为负无穷 */
    private final SkipListNode<T> head;
    
    /** 当前最大层数 */
    private int level;
    
    /** 节点数量 */
    private int size;
    
    /** 用于生成随机层数 */
    private final Random random;

    /**
     * 跳表节点实现
     * 
     * <p>每个节点包含：</p>
     * <ul>
     *   <li>分数：用于主要排序</li>
     *   <li>成员：当分数相同时用于次要排序</li>
     *   <li>后向指针：支持反向遍历</li>
     *   <li>层：包含前向指针和跨度</li>
     * </ul>
     */
    public static class SkipListNode<T> {
        /** 节点分数，主排序键 */
        public final double score;
        
        /** 节点成员，次排序键 */
        public final T member;
        
        /** 后向指针，用于反向遍历 */
        SkipListNode<T> backward;
        
        /** 层数组，包含前向指针和跨度 */
        final SkipListLevel[] level;

        /**
         * 创建指定层数的跳表节点
         * 
         * @param level 节点层数
         * @param score 节点分数
         * @param member 节点成员
         */
        public SkipListNode(int level, double score, T member) {
            this.level = new SkipListLevel[level];
            for (int i = 0; i < level; i++) {
                this.level[i] = new SkipListLevel();
            }
            this.score = score;
            this.member = member;
            this.backward = null;
        }

        /**
         * 跳表层实现，包含前向指针和跨度
         */
        static class SkipListLevel {
            /** 前向指针 */
            SkipListNode forward;
            
            /** 跨度，到下一个节点跨越的节点数量 */
            long span;

            public SkipListLevel() {
                this.forward = null;
                this.span = 0;
            }
        }
    }

    /**
     * 创建一个空的跳表
     * 
     * <p>初始化：</p>
     * <ul>
     *   <li>创建头节点，分数为负无穷</li>
     *   <li>初始层数为1</li>
     *   <li>初始化随机数生成器</li>
     * </ul>
     */
    public SkipList() {
        head = new SkipListNode<>(MAX_LEVEL, Double.NEGATIVE_INFINITY, null);
        level = 1;
        size = 0;
        random = new Random();
    }

    /**
     * 获取跳表中的节点数量
     * 
     * @return 节点数量
     */
    public int size() {
        return size;
    }

    /**
     * 检查跳表是否为空
     * 
     * @return true如果跳表为空
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 获取跳表中分数最小的节点
     * 
     * @return 第一个节点，如果跳表为空则返回null
     */
    public SkipListNode<T> getFirst() {
        return head.level[0].forward;
    }

    /**
     * 获取跳表中分数最大的节点
     * 
     * <p>从最高层开始，沿着前向指针遍历到每层的最后一个节点。
     * 
     * @return 最后一个节点，如果跳表为空则返回null
     */
    public SkipListNode<T> getLast() {
        SkipListNode<T> node = head;
        for (int i = level - 1; i >= 0; i--) {
            while (node.level[i].forward != null) {
                node = node.level[i].forward;
            }
        }
        return node != head ? node : null;
    }

    /**
     * 插入新节点到跳表
     */
    @SuppressWarnings("unchecked")
    public SkipListNode<T> insert(double score, T member) {
        SkipListNode<T>[] update = new SkipListNode[MAX_LEVEL];
        long[] rank = new long[MAX_LEVEL];
        
        // 从最高层开始查找插入位置
        SkipListNode<T> x = head;
        for (int i = level - 1; i >= 0; i--) {
            // 初始化当前层的排名
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            
            // 找到当前层的插入位置：
            // 1. 分数更小 或
            // 2. 分数相同但成员字典序更小
            while (x.level[i].forward != null &&
                   (x.level[i].forward.score < score ||
                    (x.level[i].forward.score == score &&
                     ((Comparable<T>)x.level[i].forward.member).compareTo(member) < 0))) {
                rank[i] += x.level[i].span;  // 累加跨度，用于计算新节点的排名
                x = x.level[i].forward;
            }
            update[i] = x;  // 记录每层的前驱节点
        }

        // 生成随机层数，如果高于当前最大层数则初始化新层
        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                rank[i] = 0;
                update[i] = head;
                update[i].level[i].span = size;  // 新层的跨度为整个跳表大小
            }
            level = newLevel;
        }

        // 创建新节点并插入到跳表中
        x = new SkipListNode<>(newLevel, score, member);

        // 逐层更新前向指针和跨度
        for (int i = 0; i < newLevel; i++) {
            x.level[i].forward = update[i].level[i].forward;  // 连接后继节点
            update[i].level[i].forward = x;  // 连接前驱节点
            
            // 计算新节点的跨度：前驱节点原跨度 - 新节点到前驱的距离
            x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);
            // 更新前驱节点的跨度：新节点到前驱的距离 + 1
            update[i].level[i].span = rank[0] - rank[i] + 1;
        }

        // 未接入新节点的更高层需要增加跨度
        for (int i = newLevel; i < level; i++) {
            update[i].level[i].span++;
        }

        // 设置后向指针，支持反向遍历
        x.backward = update[0] == head ? null : update[0];
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x;
        }

        size++;
        return x;
    }

    /**
     * 从跳表中删除指定节点
     */
    @SuppressWarnings("unchecked")
    public boolean delete(double score, T member) {
        SkipListNode<T>[] update = new SkipListNode[MAX_LEVEL];
        SkipListNode<T> x = head;

        // 从最高层开始查找要删除的节点
        for (int i = level - 1; i >= 0; i--) {
            // 找到每层中小于目标节点的最大节点
            while (x.level[i].forward != null &&
                   (x.level[i].forward.score < score ||
                    (x.level[i].forward.score == score &&
                     ((Comparable<T>)x.level[i].forward.member).compareTo(member) < 0))) {
                x = x.level[i].forward;
            }
            update[i] = x;  // 记录每层的前驱节点
        }

        // 获取可能要删除的节点
        x = x.level[0].forward;
        
        // 检查是否找到了匹配的节点
        if (x != null && x.score == score && x.member.equals(member)) {
            deleteNode(x, update);  // 执行实际的删除操作
            return true;
        }
        return false;
    }

    /**
     * 执行实际的节点删除操作
     */
    private void deleteNode(SkipListNode<T> x, SkipListNode<T>[] update) {
        // 逐层更新前向指针和跨度
        for (int i = 0; i < level; i++) {
            if (update[i].level[i].forward == x) {
                // 如果当前层指向要删除的节点，则跨度需要合并
                update[i].level[i].span += x.level[i].span - 1;
                update[i].level[i].forward = x.level[i].forward;
            } else {
                // 否则仅减少跨度
                update[i].level[i].span--;
            }
        }

        // 更新后向指针
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x.backward;
        }

        // 如果删除的是最高层的唯一节点，则降低跳表的层数
        while (level > 1 && head.level[level - 1].forward == null) {
            level--;
        }

        size--;
    }

    /**
     * 生成随机层数
     * 
     * <p>使用几何分布：每层以概率P继续向上。
     * 这确保了节点的期望层数为1/(1-P)，
     * 当P=0.25时，期望层数为1.33。</p>
     * 
     * @return 随机生成的层数，范围[1, MAX_LEVEL]
     */
    private int randomLevel() {
        int level = 1;
        while (random.nextDouble() < P && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }

    /**
     * 按排名范围获取节点
     */
    public List<SkipListNode<T>> getElementByRankRange(long start, long end) {
        List<SkipListNode<T>> result = new ArrayList<>();
        
        // 处理负数索引，转换为正数索引
        if (start < 0) start = size + start;
        if (end < 0) end = size + end;
        
        // 边界检查和调整
        if (start < 0) start = 0;
        if (end >= size) end = size - 1;
        if (start > end || start >= size) return result;

        // 快速定位到起始位置
        long traversed = 0;  // 记录已遍历的节点数
        SkipListNode<T> x = head;
        
        // 从最高层开始查找，利用跨度快速跳过不需要的节点
        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null && traversed + x.level[i].span <= start) {
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
        }

        // 收集范围内的节点
        x = x.level[0].forward;  // 移动到第一个要收集的节点
        if (x != null) {
            traversed++;  // 计入当前节点
            while (x != null && traversed <= end + 1) {
                result.add(x);
                if (x.level[0].forward != null) {
                    x = x.level[0].forward;
                    traversed++;
                } else {
                    break;
                }
            }
        }

        return result;
    }

    /**
     * 按分数范围获取节点
     */
    public List<SkipListNode<T>> getElementByScoreRange(double min, double max) {
        List<SkipListNode<T>> result = new ArrayList<>();
        SkipListNode<T> x = head;

        // 快速定位到最小分数位置
        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null && x.level[i].forward.score < min) {
                x = x.level[i].forward;
            }
        }

        // 收集范围内的所有节点
        x = x.level[0].forward;
        while (x != null && x.score <= max) {
            result.add(x);
            x = x.level[0].forward;
        }

        return result;
    }

    /**
     * 获取指定排名的节点
     */
    public SkipListNode<T> getElementByRank(long rank) {
        if (rank <= 0 || rank > size) {
            return null;
        }

        long traversed = 0;  // 记录已遍历的节点数
        SkipListNode<T> x = head;

        // 利用跨度快速定位到指定排名的节点
        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null && traversed + x.level[i].span <= rank) {
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
            if (traversed == rank) {
                return x;
            }
        }

        return null;
    }

    /**
     * 计算指定节点的排名
     */
    public long getRank(double score, T member) {
        long rank = 0;  // 累计排名
        SkipListNode<T> x = head;

        // 从最高层开始查找，累加路径上的跨度
        for (int i = level - 1; i >= 0; i--) {
            while (x.level[i].forward != null &&
                   (x.level[i].forward.score < score ||
                    (x.level[i].forward.score == score &&
                     ((Comparable<T>)x.level[i].forward.member).compareTo(member) <= 0))) {
                rank += x.level[i].span;  // 累加跨度
                x = x.level[i].forward;
            }
            if (x.member != null && x.member.equals(member)) {
                return rank;  // 找到目标节点，返回累计排名
            }
        }

        return 0;  // 节点不存在
    }
}
