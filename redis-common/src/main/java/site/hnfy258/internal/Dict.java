package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个使用版本号实现的单写多读的线程安全的哈希表，其实在正常情况下是单写单读，
 * 但是这个设计和redis的设计有一个不同，就是redis本身在执行rdb快照的时候
 * 你触发rewriteaof,会导致他并不立刻执行，而是等待当前的rdb快照操作完成后再执行。
 * 但是这个哈希表的设计就不同了，因为他支持单写多读，所以可以同时进行aof的重写和rdb的持久化而不阻塞
 * 这就是这个Dict的亮点所在。
 */

/**
 * 表示哈希表中的一个键值对条目（节点）。
 * 采用不可变设计，每次修改（更新值、移除）都返回一个新的DictEntry实例，
 * 从而在并发场景下提供更好的线程安全性和简化逻辑。
 * 它构成了哈希表桶中的链表。
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 */
class DictEntry<K, V> {
    /** 哈希值，用于快速比较和查找。 */
    final int hash;
    /** 键。 */
    final K key;
    /** 值。 */
    final V value;
    /** 指向链表中下一个条目的引用。 */
    final DictEntry<K, V> next;

    /**
     * 构造一个新的DictEntry。
     *
     * @param hash 条目的哈希值。
     * @param key 条目的键。
     * @param value 条目的值。
     * @param next 链表中下一个条目，如果当前是最后一个则为null。
     */
    DictEntry(final int hash, final K key, final V value, final DictEntry<K, V> next) {
        this.hash = hash;
        this.key = key;
        this.value = value;
        this.next = next;
    }

    /**
     * 创建当前DictEntry的副本，并指定新的next指针。
     * 主要用于链表的重构。
     *
     * @param newNext 新的next DictEntry。
     * @return 包含新next指针的DictEntry新实例。
     */
    DictEntry<K, V> copyWithNext(DictEntry<K, V> newNext) {
        return new DictEntry<>(this.hash, this.key, this.value, newNext);
    }

    /**
     * 在当前DictEntry或其后续链表中更新匹配键的值。
     * 如果找到匹配的键，则返回一个包含更新值的新链表（通过递归构建）。
     * 如果未找到，则返回原始链表的副本。
     *
     * @param targetHash 目标键的哈希值。
     * @param targetKey 目标键。
     * @param newValue 要设置的新值。
     * @return 更新后的DictEntry链表头。
     */
    DictEntry<K, V> updateValue(int targetHash, K targetKey, V newValue) {
        // 如果当前节点匹配，则更新其值并保留next不变
        if (this.hash == targetHash && targetKey.equals(this.key)) {
            return new DictEntry<>(this.hash, this.key, newValue, this.next);
        }
        // 递归更新后续节点
        DictEntry<K, V> updatedNext = (this.next != null) ? this.next.updateValue(targetHash, targetKey, newValue) : null;
        // 返回包含更新后的next的新节点
        return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
    }

    /**
     * 在当前DictEntry或其后续链表中移除匹配键的节点。
     * 如果当前节点是目标节点，则返回其next节点。
     * 如果目标节点在后续链表中，则返回一个包含移除节点后新链表的新实例。
     *
     * @param targetHash 目标键的哈希值。
     * @param targetKey 目标键。
     * @return 移除节点后的DictEntry链表头。
     */
    DictEntry<K, V> removeNode(int targetHash, K targetKey) {
        // 如果当前节点匹配，则返回其next节点（相当于移除了当前节点）
        if (this.hash == targetHash && targetKey.equals(this.key)) {
            return this.next;
        }
        // 递归地从后续节点中移除
        DictEntry<K, V> updatedNext = (this.next != null) ? this.next.removeNode(targetHash, targetKey) : null;
        // 返回包含更新后的next的新节点
        return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
    }
}

/**
 * 表示哈希表的一个物理结构（底层数组和相关元数据）。
 * 它包含一个DictEntry数组，每个元素是一个链表的头，用于处理哈希冲突。
 * Dict通过组合两个DictHashTable实例来实现渐进式Rehash。
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 */
class DictHashTable<K, V> {
    /** 存储DictEntry链表头的数组。 */
    final DictEntry<K, V>[] table;
    /** 哈希表的大小（桶的数量），必须是2的幂次。 */
    final int size;
    /** 用于计算索引的掩码，等于 size - 1。 */
    final int sizemask;
    /** 当前哈希表中已使用的条目数量。 */
    volatile long used;

    /**
     * 构造一个新的DictHashTable实例。
     *
     * @param size 哈希表的初始容量，必须是2的幂次。
     * @throws IllegalArgumentException 如果size不是2的幂次或小于等于0。
     */
    @SuppressWarnings("unchecked")
    DictHashTable(final int size) {
        if (size <= 0 || (size & (size - 1)) != 0) {
            throw new IllegalArgumentException("Hash table size must be a power of 2, got: " + size);
        }
        this.table = (DictEntry<K, V>[]) new DictEntry[size];
        this.size = size;
        this.sizemask = size - 1;
        this.used = 0;
    }

    /**
     * 根据哈希值计算键在当前哈希表中的桶索引。
     *
     * @param hashValue 键的哈希值。
     * @return 对应的桶索引。
     */
    int keyIndex(final int hashValue) {
        return hashValue & sizemask;
    }
}

/**
 * 存储字典状态和版本信息的不可变类。
 * 使用版本号来支持一致的快照读取。
 */
class VersionedSnapshot<K, V> {
    /** 快照的版本号 */
    final long version;
    /** 快照数据 */
    final Map<K, V> data;

    VersionedSnapshot(long version, Map<K, V> data) {
        this.version = version;
        this.data = data;
    }
}

/**
 * 实现了一个仿照Redis的哈希表，支持渐进式Rehash、扩容和缩容。
 * 针对单写多读场景优化，使用版本号机制确保多个线程同时创建快照时得到完全一致的结果。
 *
 * @param <K> 键的类型
 * @param <V> 值的类型
 */
public class Dict<K,V> {
    /** 哈希表初始大小，必须是2的幂次。 */
    static final int DICT_HT_INITIAL_SIZE = 4;
    /** 每次渐进式 Rehash 步骤中迁移的桶数量。 */
    static final int DICT_REHASH_BUCKETS_PER_STEP = 100;
    /** 渐进式 Rehash 时，连续访问空桶的最大次数。用于避免Rehash卡在稀疏的旧哈希表区域。 */
    private static final int DICT_REHASH_MAX_EMPTY_VISITS = 10;

    /**
     * 表示Dict的当前状态，包含两个哈希表（ht0和ht1）以及Rehash索引。
     * 这是一个不可变类，每次状态变更都返回一个新的DictState实例，以支持并发环境下的无锁读取。
     *
     * @param <K> 键的类型
     * @param <V> 值的类型
     */
    static final class DictState<K, V> {
        /** 主哈希表，正常操作时使用。 */
        final DictHashTable<K, V> ht0;
        /** Rehash 过程中使用的辅助哈希表（新表）。 */
        final DictHashTable<K, V> ht1;
        /**
         * Rehash 索引：
         * -1 表示不进行 Rehash。
         * >=0 表示正在 Rehash 的桶索引，ht0中索引小于rehashIndex的桶已迁移完成。
         */
        final int rehashIndex;
        /** 状态版本号，每次状态变更时递增 */
        final long version;

        /**
         * 构造一个新的DictState实例。
         *
         * @param ht0 主哈希表。
         * @param ht1 辅助哈希表（Rehash时使用），不Rehash时为null。
         * @param rehashIndex Rehash的当前索引。
         * @param version 状态版本号。
         */
        DictState(DictHashTable<K, V> ht0, DictHashTable<K, V> ht1, int rehashIndex, long version) {
            this.ht0 = ht0;
            this.ht1 = ht1;
            this.rehashIndex = rehashIndex;
            this.version = version;
        }

        /**
         * 创建一个DictState的新实例，仅更新Rehash索引。
         *
         * @param newRehashIndex 新的Rehash索引。
         * @param newVersion 新的版本号。
         * @return 更新Rehash索引后的新DictState实例。
         */
        DictState<K, V> withRehashIndex(int newRehashIndex, long newVersion) {
            return new DictState<>(this.ht0, this.ht1, newRehashIndex, newVersion);
        }

        /**
         * 创建一个DictState的新实例，更新两个哈希表和Rehash索引。
         * 主要用于启动或完成Rehash。
         *
         * @param newHt0 新的主哈希表。
         * @param newHt1 新的辅助哈希表。
         * @param newRehashIndex 新的Rehash索引。
         * @param newVersion 新的版本号。
         * @return 更新哈希表和Rehash索引后的新DictState实例。
         */
        DictState<K, V> withHt0AndHt1AndRehashIndex(DictHashTable<K, V> newHt0, DictHashTable<K, V> newHt1,
                                                    int newRehashIndex, long newVersion) {
            return new DictState<>(newHt0, newHt1, newRehashIndex, newVersion);
        }
    }

    /** 当前字典的状态。 */
    private  DictState<K, V> state;

    /** 版本号生成器，用于追踪状态变更 */
    private AtomicLong versionGenerator = new AtomicLong(0);


    /** 快照缓存，key是版本号，value是对应版本的快照 */
    private final ConcurrentHashMap<Long, VersionedSnapshot<K, V>> snapshotCache = new ConcurrentHashMap<>();

    /** 缓存清理阈值 - 当缓存大小超过此值时进行清理 */
    private static final int CACHE_CLEANUP_THRESHOLD = 100;

    /**
     * 构造一个新的空Dict实例。
     * 初始化时只包含一个主哈希表（ht0）。
     */
    public Dict() {
        long initialVersion = versionGenerator.get();
        this.state = new DictState<>(new DictHashTable<>(DICT_HT_INITIAL_SIZE), null, -1, initialVersion);
    }

    /**
     * 获取字典的EntrySet视图。
     * 该方法通过调用 {@link #createSafeSnapshot()} 来获取一个字典当前状态的
     * 线程安全快照，然后返回该快照的 {@code entrySet()}。
     *
     * @return 一个包含字典所有键值对的 {@code Map.Entry} 集合的只读迭代器。
     */
    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        final Map<K, V> snapshot = createSafeSnapshot();
        final Map<Object, Object> result = new HashMap<>();
        for (final Map.Entry<K, V> entry : snapshot.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result.entrySet();
    }

    /**
     * 计算给定键的哈希值。
     * 使用与Redis类似的一些位操作来进一步扰动hashCode，减少哈希冲突。
     *
     * @param key 要计算哈希值的键。
     * @return 键的哈希值。
     */
    private int hash(final Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        // 扰动函数，类似于HashMap的hash实现
        h = (h ^ 61) ^ (h >>> 16);
        h = h + (h << 3);
        h = h ^ (h >>> 4);
        h = h * 0x27d4eb2d;
        h = h ^ (h >>> 15);
        return h & 0x7FFFFFFF; // 确保为正数
    }

    /**
     * 检查字典中是否包含指定的键。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @param key 要检查的键。
     * @return 如果字典包含该键则返回true，否则返回false。
     */
    public boolean containsKey(K key) {
        if (key == null) return false;
        if (state.rehashIndex != -1) rehashStep(); // 渐进式Rehash
        return find(key) != null;
    }

    /**
     * 检查字典中是否包含指定的键值对。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @param key 要检查的键。
     * @param value 要检查的值。
     * @return 如果字典包含该键值对则返回true，否则返回false。
     */
    public boolean contains(K key, V value) {
        if (key == null) return false;
        if (state.rehashIndex != -1) rehashStep(); // 渐进式Rehash
        DictEntry<K, V> entry = find(key);
        // 遍历链表查找匹配的值
        while (entry != null) {
            if (entry.value.equals(value)) {
                return true;
            }
            entry = entry.next;
        }
        return false;
    }

    /**
     * 获取指定键对应的值。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @param key 要获取值的键。
     * @return 键对应的值，如果键不存在则返回null。
     */
    public V get(K key) {
        if (key == null) return null;
        if (state.rehashIndex != -1) rehashStep(); // 渐进式Rehash

        DictEntry<K, V> entry = find(key);
        return entry != null ? entry.value : null;
    }

    /**
     * 在当前哈希表中查找指定键的条目。
     * 查找顺序：先查ht0，如果Rehash中，再查ht1。
     *
     * @param key 要查找的键。
     * @return 匹配的DictEntry，如果未找到则返回null。
     */
    private DictEntry<K, V> find(final K key) {
        if (key == null) return null;
        final int keyHash = hash(key);
        final DictState<K, V> current = state; // 获取当前状态快照
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 先查ht0
        if (localHt0 != null) {
            final int idx0 = localHt0.keyIndex(keyHash);
            DictEntry<K, V> entry = localHt0.table[idx0];
            while (entry != null) {
                if (entry.hash == keyHash && key.equals(entry.key)) {
                    return entry;
                }
                entry = entry.next;
            }
        }

        // 如果在Rehash，再查ht1
        if (localRehashIndex != -1 && localHt1 != null) {
            final int idx1 = localHt1.keyIndex(keyHash);
            DictEntry<K, V> entry = localHt1.table[idx1];
            while (entry != null) {
                if (entry.hash == keyHash && key.equals(entry.key)) {
                    return entry;
                }
                entry = entry.next;
            }
        }
        return null;
    }

    /**
     * 将指定的键值对添加到字典中。
     * 如果键已存在，则更新其值。
     * 在操作前会检查是否需要触发扩容，并在Rehash过程中执行RehashStep。
     *
     * @param key 要添加或更新的键。
     * @param value 要关联的值。
     * @return 如果键已存在则返回旧值，否则返回null。
     * @throws IllegalArgumentException 如果键为null。
     * @throws IllegalStateException 如果哈希表未正确初始化。
     */
    public  V put(final K key, final V value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null.");

        final int keyHash = hash(key);

        // 检查是否需要扩容（仅当不Rehash时）
        if (state.rehashIndex == -1 && state.ht0 != null) {
            final long currentUsed = state.ht0.used;
            final double loadFactor = (double) (currentUsed + 1) / state.ht0.size; // 考虑当前插入的元素
            if (loadFactor >= 1.0) { // 负载因子达到阈值，开始扩容
                startRehash(state.ht0.size * 2);
            }
        }

        // 推进Rehash（如果正在进行）
        if (state.rehashIndex != -1) {
            rehashStep();
        }

        // 确定操作的目标哈希表：Rehash时写入ht1，否则写入ht0
        final DictHashTable<K, V> targetHt = (state.rehashIndex != -1 && state.ht1 != null) ? state.ht1 : state.ht0;
        if (targetHt == null) {
            throw new IllegalStateException("Hash table not properly initialized.");
        }

        final int idx = targetHt.keyIndex(keyHash);
        V result = putInBucket(targetHt, idx, key, value, keyHash);

        // 更新版本号并清理旧快照缓存
        updateVersionAndCleanupCache();

        // 检查是否需要缩容（仅当不Rehash时）
        if (state.rehashIndex == -1) {
            checkShrinkIfNeeded();
        }
        return result;
    }

    /**
     * 将键值对插入或更新到指定的哈希表桶中。
     *
     * @param table 目标哈希表（ht0或ht1）。
     * @param idx 键计算出的桶索引。
     * @param key 键。
     * @param value 值。
     * @param keyHash 键的哈希值。
     * @return 如果键已存在则返回旧值，否则返回null。
     */
    private V putInBucket(final DictHashTable<K, V> table, final int idx,
                          final K key, final V value, final int keyHash) {
        DictEntry<K, V> head = table.table[idx];
        V oldValue = null;
        boolean found = false;

        // 遍历链表检查键是否已存在
        DictEntry<K, V> node = head;
        while (node != null) {
            if (node.hash == keyHash && key.equals(node.key)) {
                oldValue = node.value;
                found = true;
                break;
            }
            node = node.next;
        }

        DictEntry<K, V> newHead;
        if (found) {
            // 如果找到，更新值并构建新链表
            newHead = head.updateValue(keyHash, key, value);
        } else {
            // 如果未找到，作为新节点插入链表头部
            newHead = new DictEntry<>(keyHash, key, value, head);
            table.used++; // 增加已使用条目数
        }

        // 更新桶的头指针
        table.table[idx] = newHead;
        return oldValue;
    }

    /**
     * 从字典中移除指定键的条目。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @param key 要移除的键。
     * @return 被移除键对应的值，如果键不存在则返回null。
     */
    public  V remove(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);

        if (state.rehashIndex != -1) {
            rehashStep(); // 渐进式Rehash
        }

        final DictHashTable<K, V> localHt0 = state.ht0;
        final DictHashTable<K, V> localHt1 = state.ht1;
        final int localRehashIndex = state.rehashIndex;

        // 尝试从ht0中移除
        V removedValue = removeFromTable(localHt0, key, keyHash);

        // 如果ht0中未找到且正在Rehash，则尝试从ht1中移除
        if (removedValue == null && localRehashIndex != -1 && localHt1 != null) {
            removedValue = removeFromTable(localHt1, key, keyHash);
        }

        // 如果成功移除，更新版本号并检查是否需要缩容
        if (removedValue != null) {
            updateVersionAndCleanupCache();
            checkShrinkIfNeeded();
        }
        return removedValue;
    }

    /**
     * 从指定的哈希表桶中移除匹配键的条目。
     *
     * @param table 目标哈希表（ht0或ht1）。
     * @param key 要移除的键。
     * @param keyHash 键的哈希值。
     * @return 被移除键的值，如果未找到则返回null。
     */
    private V removeFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null) return null;

        final int idx = table.keyIndex(keyHash);
        DictEntry<K, V> head = table.table[idx];
        if (head == null) return null; // 桶为空，直接返回

        V oldValue = null;
        DictEntry<K, V> newHead;
        boolean found = false;

        // 检查头节点是否匹配
        if (head.hash == keyHash && key.equals(head.key)) {
            oldValue = head.value;
            newHead = head.next; // 移除头节点，新头为原next
            found = true;
        } else {
            // 遍历链表查找并确认存在
            DictEntry<K, V> current = head;
            while (current != null) {
                if (current.hash == keyHash && key.equals(current.key)) {
                    oldValue = current.value;
                    found = true;
                    break;
                }
                current = current.next;
            }

            if (!found) return null; // 链表中未找到
            // 如果找到且不是头节点，通过递归调用removeNode构建新链表
            newHead = head.removeNode(keyHash, key);
        }

        // 更新桶的头指针
        table.table[idx] = newHead;
        table.used--; // 减少已使用条目数
        return oldValue;
    }

    /**
     * 执行一步渐进式Rehash操作，将ht0中的一部分桶迁移到ht1。
     * 该方法会在每次操作（put, get, remove等）时被调用，以分散Rehash的开销。
     */
    private void rehashStep() {
        if (state.rehashIndex == -1) return; // 不在Rehash状态

        final DictHashTable<K, V> localHt0 = state.ht0;
        final DictHashTable<K, V> localHt1 = state.ht1;
        int currentRehashIndex = state.rehashIndex;

        // Rehash完成条件判断：如果ht0或ht1为null，或ht0已迁移完毕且used为0
        if (localHt0 == null || localHt1 == null) {
            // 确保Rehash完全结束
            if (localHt0 != null && currentRehashIndex >= localHt0.size && localHt0.used == 0) {
                long newVersion = versionGenerator.incrementAndGet();
                state = new DictState<>(localHt1, null, -1, newVersion); // 切换到新表，清除旧表和Rehash状态
            }
            return;
        }

        int emptyVisits = 0; // 连续访问空桶的计数器
        int bucketsToMove = DICT_REHASH_BUCKETS_PER_STEP; // 本次要迁移的桶数量
        int tempRehashIndex = currentRehashIndex; // 临时的Rehash索引，用于本次操作

        // 循环迁移桶，直到达到步长限制或遍历完ht0
        while (bucketsToMove > 0 && tempRehashIndex < localHt0.size) {
            DictEntry<K, V> entryToMove = localHt0.table[tempRehashIndex];

            // 处理空桶：连续遇到空桶则提前退出，避免无效遍历
            if (entryToMove == null) {
                emptyVisits++;
                if (emptyVisits >= DICT_REHASH_MAX_EMPTY_VISITS) {
                    break;
                }
                tempRehashIndex++;
                bucketsToMove--;
                continue;
            }

            // 迁移整个桶中的所有条目
            while (entryToMove != null) {
                final DictEntry<K, V> currentEntry = entryToMove;
                final DictEntry<K, V> nextEntryInHt0 = currentEntry.next; // 保存ht0中的下一个节点

                final int targetIdx = localHt1.keyIndex(currentEntry.hash); // 计算在新表中的索引

                // 将条目插入到新表（ht1）的桶头部
                DictEntry<K, V> oldHeadInHt1 = localHt1.table[targetIdx];
                localHt1.table[targetIdx] = new DictEntry<>(
                        currentEntry.hash, currentEntry.key, currentEntry.value, oldHeadInHt1);

                localHt0.used--; // 旧表used减少
                localHt1.used++; // 新表used增加

                entryToMove = nextEntryInHt0; // 移动到ht0中的下一个节点
            }

            // 清空旧表中的当前桶
            localHt0.table[tempRehashIndex] = null;
            bucketsToMove--;
            tempRehashIndex++;
            emptyVisits = 0; // 重置空桶计数
        }

        // 更新Rehash状态：如果ht0已迁移完毕且used为0，则完成Rehash
        long newVersion = versionGenerator.incrementAndGet();
        if (tempRehashIndex >= localHt0.size && localHt0.used == 0) {
            state = new DictState<>(localHt1, null, -1, newVersion); // 切换到新表，清除旧表和Rehash状态
        } else {
            state = state.withRehashIndex(tempRehashIndex, newVersion); // 更新Rehash索引
        }
    }

    /**
     * 启动Rehash过程。
     * 创建一个新的辅助哈希表（ht1），并将Rehash索引设置为0。
     *
     * @param targetSize 新哈希表（ht1）的目标大小。
     */
    private void startRehash(final int targetSize) {
        if (state.rehashIndex != -1) return; // 已经在Rehash中，不重复启动

        DictHashTable<K, V> newHt1 = new DictHashTable<>(targetSize);
        long newVersion = versionGenerator.incrementAndGet();
        // 更新状态，设置ht1并启动Rehash（rehashIndex从0开始）
        state = state.withHt0AndHt1AndRehashIndex(state.ht0, newHt1, 0, newVersion);
        rehashStep(); // 立即执行一步Rehash
    }

    /**
     * 检查是否需要进行缩容。
     * 当负载因子过低且哈希表大小超过初始大小时，会触发缩容Rehash。
     */
    private void checkShrinkIfNeeded() {
        if (state.rehashIndex != -1) return; // 正在Rehash中，不检查缩容

        final DictHashTable<K, V> localHt0 = state.ht0;
        if (localHt0 == null) return;

        final double loadFactor = (double) localHt0.used / localHt0.size;

        // 负载因子低于阈值且大小大于初始大小时触发缩容
        if (loadFactor < 0.1 && localHt0.size > DICT_HT_INITIAL_SIZE) {
            int newSize = localHt0.size / 2; // 新大小为当前大小的一半
            // 确保新大小不小于初始大小
            if (newSize < DICT_HT_INITIAL_SIZE) {
                newSize = DICT_HT_INITIAL_SIZE;
            }
            startRehash(newSize); // 启动缩容Rehash
        }
    }

    /**
     * 更新版本号并清理过期的快照缓存
     */
    private void updateVersionAndCleanupCache() {
        long newVersion = versionGenerator.incrementAndGet();
        state = new DictState<>(state.ht0, state.ht1, state.rehashIndex, newVersion);

        // 定期清理缓存
        if (snapshotCache.size() > CACHE_CLEANUP_THRESHOLD) {
            cleanupOldSnapshots();
        }
    }

    /**
     * 清理过期的快照缓存，只保留最近的少量快照
     */
    private void cleanupOldSnapshots() {
        if (snapshotCache.size() <= 10) return; // 保留至少10个快照

        long currentVersion = state.version;
        snapshotCache.entrySet().removeIf(entry ->
                currentVersion - entry.getKey() > 50); // 清理50个版本之前的快照
    }

    /**
     * 清空字典中所有条目。
     * 将字典重置为初始状态，只包含一个空的ht0。
     */
    public  void clear() {
        long newVersion = versionGenerator.incrementAndGet();
        this.state = new DictState<>(new DictHashTable<>(DICT_HT_INITIAL_SIZE), null, -1, newVersion);
        snapshotCache.clear(); // 清空所有缓存的快照
    }

    /**
     * 返回字典中键值对的数量。
     * 如果正在Rehash，则返回ht0和ht1中条目总和。
     *
     * @return 字典中条目的总数。
     */
    public long size() {
        long totalSize = state.ht0.used;
        if (state.ht1 != null) {
            totalSize += state.ht1.used;
        }
        return totalSize;
    }

    /**
     * 返回字典中所有键的Set视图。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @return 包含所有键的Set。
     */
    public Set<K> keySet() {
        final Set<K> keys = new HashSet<>();
        if (state.rehashIndex != -1) rehashStep(); // 渐进式Rehash

        final DictHashTable<K, V> localHt0 = state.ht0;
        final DictHashTable<K, V> localHt1 = state.ht1;
        final int localRehashIndex = state.rehashIndex;

        // 遍历ht0中的键
        if (localHt0 != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table[i];
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }

        // 如果正在Rehash，遍历ht1中的键
        if (localRehashIndex != -1 && localHt1 != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table[i];
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }
        return keys;
    }

    /**
     * 返回字典中所有键值对的Map视图。
     * 如果正在Rehash，会先执行一步Rehash。
     *
     * @return 包含所有键值对的Map。
     */
    public Map<K, V> getAll() {
        final Map<K, V> map = new HashMap<>();
        if (state.rehashIndex != -1) rehashStep(); // 渐进式Rehash

        final DictHashTable<K, V> localHt0 = state.ht0;
        final DictHashTable<K, V> localHt1 = state.ht1;
        final int localRehashIndex = state.rehashIndex;

        // 遍历ht0并添加到Map
        if (localHt0 != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table[i];
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }

        // 如果正在Rehash，遍历ht1并添加到Map
        if (localRehashIndex != -1 && localHt1 != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table[i];
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }
        return map;
    }

    /**
     * 创建当前字典的线程安全快照。
     * 使用版本号机制确保多个线程同时调用此方法时得到完全一致的结果。
     *
     * 实现策略：
     * 1. 读取当前状态版本号
     * 2. 检查缓存中是否已有该版本的快照
     * 3. 如果没有，创建新快照并缓存
     * 4. 返回快照数据
     *
     * @return 字典内容的HashMap快照，保证一致性。
     */
    public Map<K, V> createSafeSnapshot() {
        final long currentVersion = state.version;

        // 检查缓存
        VersionedSnapshot<K, V> cachedSnapshot = snapshotCache.get(currentVersion);
        if (cachedSnapshot != null) {
            return new HashMap<>(cachedSnapshot.data);
        }

        // 创建新快照
        final Map<K, V> snapshot = new HashMap<>();
        final DictState<K, V> current = state;

        createSnapshotFromTable(current.ht0, snapshot);
        if (current.rehashIndex != -1 && current.ht1 != null) {
            createSnapshotFromTable(current.ht1, snapshot);
        }

        // 缓存快照
        VersionedSnapshot<K, V> newSnapshot = new VersionedSnapshot<>(currentVersion, snapshot);
        snapshotCache.put(currentVersion, newSnapshot);

        // 清理旧缓存
        if (snapshotCache.size() > CACHE_CLEANUP_THRESHOLD) {
            cleanupOldSnapshots();
        }

        return new HashMap<>(snapshot); // 返回副本保证安全
    }

    /**
     * 将指定哈希表中的所有键值对添加到给定的快照Map中。
     *
     * @param table 要遍历的哈希表。
     * @param snapshot 目标快照Map。
     */
    private void createSnapshotFromTable(final DictHashTable<K, V> table, final Map<K, V> snapshot) {
        if (table == null) return;

        for (int i = 0; i < table.size; i++) {
            DictEntry<K, V> current = table.table[i];
            while (current != null) {
                // 仅添加键值对，Map的put操作会自动处理重复键（在Rehash中可能存在）
                snapshot.put(current.key, current.value);
                current = current.next;
            }
        }
    }

    /**
     * 判断字典当前是否正在进行Rehash。
     *
     * @return 如果正在Rehash则返回true，否则返回false。
     */
    public boolean isRehashing() {
        return state.rehashIndex != -1;
    }

    /**
     * 获取主哈希表（ht0）的负载因子。
     * 负载因子 = 已使用条目数 / 哈希表大小。
     *
     * @return 主哈希表（ht0）的负载因子。
     */
    public double getLoadFactor() {
        DictHashTable<K, V> ht0 = state.ht0;
        if (ht0 == null) return 0.0;
        return (double) ht0.used / ht0.size;
    }

    /**
     * 获取当前状态的版本号
     *
     * @return 当前版本号
     */
    public long getCurrentVersion() {
        return state.version;
    }

    /**
     * 获取快照缓存的大小（用于监控和调试）
     *
     * @return 缓存中快照的数量
     */
    public int getSnapshotCacheSize() {
        return snapshotCache.size();
    }
}
