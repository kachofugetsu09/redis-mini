package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * 无锁并发字典实现，支持渐进式rehash。
 *
 * <p>本实现模仿Redis字典的核心设计理念：
 * <ul>
 *   <li><strong>双哈希表结构：</strong> 使用两个哈希表（ht0和ht1）实现渐进式rehash</li>
 *   <li><strong>无锁设计：</strong> 基于CAS操作和不可变数据结构，避免锁竞争</li>
 *   <li><strong>渐进式扩容：</strong> 扩容时不会阻塞操作，而是分批迁移数据</li>
 *   <li><strong>自动收缩：</strong> 负载因子过低时自动收缩，节约内存</li>
 * </ul>
 *
 * <p><strong>应用场景：</strong>
 * <ul>
 *   <li>MiniRedis服务端的核心数据存储</li>
 *   <li>单线程命令处理，多线程快照创建</li>
 *   <li>支持AOF重写和RDB持久化的无锁快照</li>
 * </ul>
 *
 * <p><strong>线程安全保证：</strong>
 * <ul>
 *   <li>读操作：完全无锁，使用快照语义</li>
 *   <li>写操作：基于CAS的乐观更新</li>
 *   <li>rehash：渐进式进行，不阻塞正常操作</li>
 *   <li>快照：提供一致性视图，适用于持久化</li>
 * </ul>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 * @author hnfy258
 * @version 1.0
 */
public class Dict<K, V> {

    // ========================= 常量定义 =========================

    /** 哈希表初始大小，必须是2的幂次 */
    static final int DICT_HT_INITIAL_SIZE = 4;

    /** 每次渐进式rehash步骤中迁移的桶数量 */
    static final int DICT_REHASH_BUCKETS_PER_STEP = 100;

    /**
     * 渐进式rehash时，连续访问空桶的最大次数。
     * 超过此次数，本次rehash步骤将停止，避免不必要的空循环。
     */
    private static final int DICT_REHASH_MAX_EMPTY_VISITS = 10;

    /** CAS重试时的最大退避时间（纳秒） */
    private static final long MAX_BACKOFF_NANOS = 1_000_000; // 1毫秒

    // ========================= 核心状态 =========================

    /** 字典状态的原子引用，支持无锁状态切换 */
    final AtomicReference<DictState<K, V>> state;

    // ========================= 内部类定义 =========================

    /**
     * 字典状态快照，不可变对象。
     *
     * <p>包含两个哈希表和rehash索引，通过原子引用确保状态一致性。
     *
     * @param <K> 键类型
     * @param <V> 值类型
     */
    static final class DictState<K, V> {
        /** 主哈希表，正常情况下保存所有数据 */
        final DictHashTable<K, V> ht0;

        /** 辅助哈希表，仅在rehash期间使用 */
        final DictHashTable<K, V> ht1;

        /** rehash索引：-1表示未进行rehash，>=0表示正在rehash的桶索引 */
        final int rehashIndex;

        DictState(DictHashTable<K, V> ht0, DictHashTable<K, V> ht1, int rehashIndex) {
            this.ht0 = ht0;
            this.ht1 = ht1;
            this.rehashIndex = rehashIndex;
        }

        /**
         * 创建新状态，仅更新rehash索引
         */
        DictState<K, V> withRehashIndex(int newRehashIndex) {
            return new DictState<>(this.ht0, this.ht1, newRehashIndex);
        }

        /**
         * 创建新状态，更新所有字段
         */
        DictState<K, V> withHt0AndHt1AndRehashIndex(DictHashTable<K, V> newHt0,
                                                    DictHashTable<K, V> newHt1,
                                                    int newRehashIndex) {
            return new DictState<>(newHt0, newHt1, newRehashIndex);
        }
    }

    /**
     * 哈希表节点，使用链地址法解决冲突。
     *
     * <p>节点为不可变对象，更新时创建新节点，保证无锁读取的安全性。
     *
     * @param <K> 键类型
     * @param <V> 值类型
     */
    static final class DictEntry<K, V> {
        /** 缓存的哈希值，避免重复计算 */
        final int hash;

        /** 键 */
        final K key;

        /** 值 */
        final V value;

        /** 指向链表中下一个节点的指针 */
        final DictEntry<K, V> next;

        DictEntry(final int hash, final K key, final V value, final DictEntry<K, V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        /**
         * 创建新节点，保持当前节点数据不变，仅更新next指针
         */
        DictEntry<K, V> copyWithNext(DictEntry<K, V> newNext) {
            return new DictEntry<>(this.hash, this.key, this.value, newNext);
        }

        /**
         * 递归更新链表中指定键的值
         *
         * @param targetHash 目标键的哈希值
         * @param targetKey  目标键
         * @param newValue   新值
         * @return 更新后的链表头节点
         */
        DictEntry<K, V> updateValue(int targetHash, K targetKey, V newValue) {
            if (this.hash == targetHash && targetKey.equals(this.key)) {
                return new DictEntry<>(this.hash, this.key, newValue, this.next);
            }
            DictEntry<K, V> updatedNext = (this.next != null) ?
                    this.next.updateValue(targetHash, targetKey, newValue) : null;
            return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
        }

        /**
         * 递归移除链表中指定键的节点
         *
         * @param targetHash 目标键的哈希值
         * @param targetKey  目标键
         * @return 移除指定节点后的链表头节点
         */
        DictEntry<K, V> removeNode(int targetHash, K targetKey) {
            if (this.hash == targetHash && targetKey.equals(this.key)) {
                return this.next;
            }
            DictEntry<K, V> updatedNext = (this.next != null) ?
                    this.next.removeNode(targetHash, targetKey) : null;
            return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
        }
    }

    /**
     * 哈希表实现，使用原子引用数组存储链表头节点。
     *
     * <p>设计要点：
     * <ul>
     *   <li>表大小必须是2的幂次，便于位运算求模</li>
     *   <li>使用原子引用数组支持CAS操作</li>
     *   <li>维护已用槽位数量，用于负载因子计算</li>
     * </ul>
     *
     * @param <K> 键类型
     * @param <V> 值类型
     */
    static class DictHashTable<K, V> {
        /** 哈希表数组，每个元素是链表头节点 */
        final AtomicReferenceArray<DictEntry<K, V>> table;

        /** 表大小，必须是2的幂次 */
        final int size;

        /** 表大小掩码，用于快速求模运算（size - 1） */
        final int sizemask;

        /** 已使用的槽位数量，用于负载因子计算 */
        final AtomicLong used;

        DictHashTable(final int size) {
            if (size <= 0 || (size & (size - 1)) != 0) {
                throw new IllegalArgumentException("哈希表大小必须是2的幂次，当前值: " + size);
            }
            this.table = new AtomicReferenceArray<>(size);
            this.size = size;
            this.sizemask = size - 1;
            this.used = new AtomicLong(0);
        }
    }

    // ========================= 构造函数 =========================

    /**
     * 创建新的字典实例
     */
    public Dict() {
        this.state = new AtomicReference<>(
                new DictState<>(new DictHashTable<>(DICT_HT_INITIAL_SIZE), null, -1)
        );
    }

    // ========================= 哈希函数 =========================

    /**
     * 计算对象的哈希值
     *
     * <p>使用改进的MurmurHash算法，提供良好的哈希分布特性。
     *
     * @param key 要计算哈希值的键
     * @return 非负哈希值
     */
    private int hash(final Object key) {
        if (key == null) return 0;

        int h = key.hashCode();
        // MurmurHash风格的哈希混合
        h = (h ^ 61) ^ (h >>> 16);
        h = h + (h << 3);
        h = h ^ (h >>> 4);
        h = h * 0x27d4eb2d;
        h = h ^ (h >>> 15);
        return h & 0x7FFFFFFF; // 确保非负
    }

    /**
     * 计算键在指定大小哈希表中的索引
     *
     * @param hashValue 哈希值
     * @param size      表大小
     * @return 表索引
     */
    private int keyIndex(final int hashValue, final int size) {
        return hashValue & (size - 1);
    }

    // ========================= 查询操作 =========================

    /**
     * 根据键获取值
     *
     * <p>查询策略：
     * <ol>
     *   <li>如果正在rehash，先推进一个rehash步骤</li>
     *   <li>优先从ht0查找</li>
     *   <li>如果ht0未找到且正在rehash，再从ht1查找</li>
     * </ol>
     *
     * @param key 键
     * @return 对应的值，不存在则返回null
     */
    public V get(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);

        // 推进rehash
        DictState<K, V> current = state.get();
        if (current.rehashIndex != -1) {
            rehashStep();
        }

        // 获取最新状态快照
        current = state.get();
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 无锁查询：先查ht0，再查ht1
        V result = getFromTable(localHt0, key, keyHash);
        if (result == null && localRehashIndex != -1 && localHt1 != null) {
            result = getFromTable(localHt1, key, keyHash);
        }
        return result;
    }

    /**
     * 从指定哈希表中查询键值
     *
     * @param table   目标哈希表
     * @param key     键
     * @param keyHash 键的哈希值
     * @return 值，不存在则返回null
     */
    private V getFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }

        final int idx = keyIndex(keyHash, table.size);
        if (idx >= table.table.length() || idx < 0) {
            throw new IllegalStateException("计算的索引 " + idx + " 超出表大小 " + table.table.length());
        }

        DictEntry<K, V> current = table.table.get(idx);
        while (current != null) {
            if (current.hash == keyHash && key.equals(current.key)) {
                return current.value;
            }
            current = current.next;
        }
        return null;
    }

    /**
     * 查找键对应的节点
     *
     * @param key 键
     * @return 节点，不存在则返回null
     */
    private DictEntry<K, V> find(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);
        final DictState<K, V> current = state.get();
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 先在ht0中查找
        if (localHt0 != null && localHt0.table != null) {
            final int idx0 = keyIndex(keyHash, localHt0.size);
            if (idx0 >= 0 && idx0 < localHt0.size) {
                DictEntry<K, V> entry = localHt0.table.get(idx0);
                while (entry != null) {
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next;
                }
            }
        }

        // 如果正在rehash，再在ht1中查找
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            final int idx1 = keyIndex(keyHash, localHt1.size);
            if (idx1 >= 0 && idx1 < localHt1.size) {
                DictEntry<K, V> entry = localHt1.table.get(idx1);
                while (entry != null) {
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next;
                }
            }
        }
        return null;
    }

    /**
     * 检查是否包含指定键
     *
     * @param key 键
     * @return 是否包含
     */
    public boolean containsKey(K key) {
        if (key == null) return false;

        DictState<K, V> current = state.get();
        if (current.rehashIndex != -1) {
            rehashStep();
        }
        return find(key) != null;
    }

    /**
     * 检查是否包含指定的键值对
     *
     * @param score  键
     * @param member 值
     * @return 是否包含该键值对
     */
    public boolean contains(K score, V member) {
        if (score == null) return false;

        DictState<K, V> current = state.get();
        if (current.rehashIndex != -1) {
            rehashStep();
        }

        DictEntry<K, V> entry = find(score);
        while (entry != null) {
            if (entry.value.equals(member)) {
                return true;
            }
            entry = entry.next;
        }
        return false;
    }

    // ========================= 修改操作 =========================

    /**
     * 插入或更新键值对
     *
     * <p>操作流程：
     * <ol>
     *   <li>检查负载因子，必要时启动rehash扩容</li>
     *   <li>推进rehash步骤（如果正在进行）</li>
     *   <li>选择目标哈希表（rehash期间选择ht1，否则选择ht0）</li>
     *   <li>使用CAS操作更新链表</li>
     *   <li>检查是否需要收缩</li>
     * </ol>
     *
     * @param key   键，不能为null
     * @param value 值
     * @return 旧值，不存在则返回null
     * @throws IllegalArgumentException 如果键为null
     */
    public V put(final K key, final V value) {
        if (key == null) {
            throw new IllegalArgumentException("键不能为null");
        }

        final int keyHash = hash(key);
        DictState<K, V> current = state.get();

        // 检查是否需要扩容
        if (current.rehashIndex == -1 && current.ht0 != null) {
            final long currentUsed = current.ht0.used.get();
            final double loadFactor = (double) (currentUsed + 1) / current.ht0.size;
            if (loadFactor >= 1.0) {
                startRehash(current.ht0.size * 2);
            }
        }

        // 推进rehash
        current = state.get();
        if (current.rehashIndex != -1) {
            rehashStep();
        }

        // 选择目标哈希表
        current = state.get();
        final DictHashTable<K, V> targetHt = (current.rehashIndex != -1 && current.ht1 != null)
                ? current.ht1 : current.ht0;

        if (targetHt == null || targetHt.table == null) {
            throw new IllegalStateException("哈希表未正确初始化");
        }

        final int idx = keyIndex(keyHash, targetHt.size);
        if (idx >= targetHt.table.length() || idx < 0) {
            throw new IllegalStateException("计算的索引 " + idx + " 超出表大小 " + targetHt.table.length());
        }

        V result = putInBucket(targetHt, idx, key, value, keyHash);

        // 检查收缩
        current = state.get();
        if (current.rehashIndex == -1) {
            checkShrinkIfNeeded();
        }
        return result;
    }

    /**
     * 在指定桶中插入或更新键值对
     *
     * <p>使用乐观CAS策略，失败时进行一次重试。
     * 在单线程命令执行环境中，CAS失败通常由rehash引起，重试一次即可成功。
     *
     * @param table   目标哈希表
     * @param idx     桶索引
     * @param key     键
     * @param value   值
     * @param keyHash 键的哈希值
     * @return 旧值，不存在则返回null
     */
    private V putInBucket(final DictHashTable<K, V> table, final int idx,
                          final K key, final V value, final int keyHash) {
        DictEntry<K, V> head = table.table.get(idx);
        DictEntry<K, V> node = head;
        V oldValue = null;
        boolean found = false;

        // 查找是否已存在
        while (node != null) {
            if (node.hash == keyHash && key.equals(node.key)) {
                oldValue = node.value;
                found = true;
                break;
            }
            node = node.next;
        }

        // 构建新链表头
        DictEntry<K, V> newHead;
        if (found) {
            newHead = head.updateValue(keyHash, key, value);
        } else {
            newHead = new DictEntry<>(keyHash, key, value, head);
        }

        // CAS更新
        if (table.table.compareAndSet(idx, head, newHead)) {
            if (!found) {
                table.used.incrementAndGet();
            }
            return oldValue;
        } else {
            // 重试一次
            Thread.yield();
            head = table.table.get(idx);

            // 重新计算
            node = head;
            oldValue = null;
            found = false;
            while (node != null) {
                if (node.hash == keyHash && key.equals(node.key)) {
                    oldValue = node.value;
                    found = true;
                    break;
                }
                node = node.next;
            }

            if (found) {
                newHead = head.updateValue(keyHash, key, value);
            } else {
                newHead = new DictEntry<>(keyHash, key, value, head);
            }

            if (table.table.compareAndSet(idx, head, newHead)) {
                if (!found) {
                    table.used.incrementAndGet();
                }
                return oldValue;
            } else {
                throw new IllegalStateException("重试后CAS仍失败，键: " + key);
            }
        }
    }

    /**
     * 移除指定键的键值对
     *
     * @param key 键
     * @return 被移除的值，不存在则返回null
     */
    public V remove(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);
        DictState<K, V> current = state.get();

        // 推进rehash
        if (current.rehashIndex != -1) {
            rehashStep();
        }

        current = state.get();
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 先从ht0移除
        V removedValue = removeFromTable(localHt0, key, keyHash);

        // 如果ht0未找到且正在rehash，再从ht1移除
        if (removedValue == null && localRehashIndex != -1 && localHt1 != null) {
            removedValue = removeFromTable(localHt1, key, keyHash);
        }

        if (removedValue != null) {
            checkShrinkIfNeeded();
        }
        return removedValue;
    }

    /**
     * 从指定哈希表中移除键值对
     *
     * @param table   目标哈希表
     * @param key     键
     * @param keyHash 键的哈希值
     * @return 被移除的值，不存在则返回null
     */
    private V removeFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }

        final int idx = keyIndex(keyHash, table.size);
        if (idx >= table.table.length() || idx < 0) {
            return null;
        }

        DictEntry<K, V> head = table.table.get(idx);
        if (head == null) {
            return null;
        }

        V oldValue = null;
        DictEntry<K, V> newHead = null;
        boolean found = false;

        // 检查头节点
        if (head.hash == keyHash && key.equals(head.key)) {
            oldValue = head.value;
            newHead = head.next;
            found = true;
        } else {
            // 检查链表其余节点
            DictEntry<K, V> current = head;
            while (current != null) {
                if (current.hash == keyHash && key.equals(current.key)) {
                    oldValue = current.value;
                    found = true;
                    break;
                }
                current = current.next;
            }

            if (!found) {
                return null;
            }

            newHead = head.removeNode(keyHash, key);
        }

        // CAS移除
        if (found) {
            if (table.table.compareAndSet(idx, head, newHead)) {
                table.used.decrementAndGet();
                return oldValue;
            } else {
                // 重试一次
                Thread.yield();
                head = table.table.get(idx);

                // 重新计算
                V retriedOldValue = null;
                DictEntry<K, V> retriedNewHead = null;
                boolean retriedFound = false;

                if (head != null && head.hash == keyHash && key.equals(head.key)) {
                    retriedOldValue = head.value;
                    retriedNewHead = head.next;
                    retriedFound = true;
                } else if (head != null) {
                    DictEntry<K, V> current = head;
                    while (current != null) {
                        if (current.hash == keyHash && key.equals(current.key)) {
                            retriedOldValue = current.value;
                            retriedFound = true;
                            break;
                        }
                        current = current.next;
                    }
                    if (retriedFound) {
                        retriedNewHead = head.removeNode(keyHash, key);
                    }
                }

                if (retriedFound) {
                    if (table.table.compareAndSet(idx, head, retriedNewHead)) {
                        table.used.decrementAndGet();
                        return retriedOldValue;
                    } else {
                        throw new IllegalStateException("重试后CAS仍失败，键: " + key);
                    }
                } else {
                    // 键已不存在（可能被rehash移动了）
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    /**
     * 清空字典，重置为初始状态
     */
    public void clear() {
        while (true) {
            DictState<K, V> current = state.get();
            DictState<K, V> newState = new DictState<>(
                    new DictHashTable<>(DICT_HT_INITIAL_SIZE), null, -1);
            if (state.compareAndSet(current, newState)) {
                return;
            }
        }
    }

    // ========================= 查看操作 =========================

    /**
     * 获取字典中键值对的数量
     *
     * @return 键值对数量
     */
    public int size() {
        DictState<K, V> current = state.get();
        long totalSize = current.ht0.used.get();
        if (current.ht1 != null) {
            totalSize += current.ht1.used.get();
        }
        return (int) totalSize;
    }

    /**
     * 获取所有键的集合
     *
     * @return 键的集合，包含字典当前所有键
     */
    public Set<K> keySet() {
        final Set<K> keys = new HashSet<>();
        DictState<K, V> current = state.get();

        if (current.rehashIndex != -1) {
            rehashStep();
        }

        current = state.get();
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 遍历ht0
        if (localHt0 != null && localHt0.table != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table.get(i);
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }

        // 如果正在rehash，遍历ht1
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table.get(i);
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }
        return keys;
    }

    /**
     * 获取所有键值对的映射
     *
     * @return 包含所有键值对的Map
     */
    public Map<K, V> getAll() {
        final Map<K, V> map = new HashMap<>();
        DictState<K, V> current = state.get();

        if (current.rehashIndex != -1) {
            rehashStep();
        }

        current = state.get();
        final DictHashTable<K, V> localHt0 = current.ht0;
        final DictHashTable<K, V> localHt1 = current.ht1;
        final int localRehashIndex = current.rehashIndex;

        // 遍历ht0
        if (localHt0 != null && localHt0.table != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table.get(i);
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }

        // 如果正在rehash，遍历ht1
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table.get(i);
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }
        return map;
    }

    /**
     * 获取字典的EntrySet视图
     *
     * @return Map.Entry的集合
     */
    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        final Map<K, V> snapshot = createSafeSnapshot();
        final Map<Object, Object> result = new HashMap<>();
        for (final Map.Entry<K, V> entry : snapshot.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result.entrySet();
    }

    // ========================= 渐进式Rehash =========================

    /**
     * 执行一个渐进式rehash步骤
     *
     * <p>渐进式rehash是Redis字典的核心特性，避免了一次性rehash造成的长时间阻塞。
     * 每次调用此方法会迁移少量数据桶，直到全部迁移完成。
     *
     * <p>工作原理：
     * <ol>
     *   <li>检查是否正在进行rehash（rehashIndex >= 0）</li>
     *   <li>从ht0的currentRehashIndex开始，迁移指定数量的桶到ht1</li>
     *   <li>使用CAS操作确保线程安全</li>
     *   <li>迁移完成后，将ht1提升为ht0，清空ht1</li>
     * </ol>
     *
     * <p>优化策略：
     * <ul>
     *   <li>跳过空桶，但限制连续空桶访问次数</li>
     *   <li>批量迁移多个桶，提高效率</li>
     *   <li>CAS失败时使用微秒级退避</li>
     * </ul>
     */
    void rehashStep() {
        while (true) {
            DictState<K, V> current = state.get();
            if (current.rehashIndex == -1) {
                return; // 未在进行rehash
            }

            final DictHashTable<K, V> localHt0 = current.ht0;
            final DictHashTable<K, V> localHt1 = current.ht1;
            int currentRehashIndex = current.rehashIndex;

            if (localHt0 == null || localHt1 == null) {
                // 处理边界情况：rehash完成但状态未更新
                if (localHt0 != null && currentRehashIndex >= localHt0.size && localHt0.used.get() == 0) {
                    DictState<K, V> newState = new DictState<>(localHt1, null, -1);
                    if (state.compareAndSet(current, newState)) {
                        return;
                    }
                    continue;
                }
                return;
            }

            int emptyVisits = 0;
            int maxEmptyVisits = DICT_REHASH_MAX_EMPTY_VISITS;
            int bucketsToMove = DICT_REHASH_BUCKETS_PER_STEP;
            boolean movedAny = false;
            int tempRehashIndex = currentRehashIndex;

            // 迁移指定数量的桶
            while (bucketsToMove > 0 && tempRehashIndex < localHt0.size) {
                DictEntry<K, V> entryToMove = localHt0.table.get(tempRehashIndex);

                // 跳过空桶，但限制连续空桶数量
                if (entryToMove == null) {
                    emptyVisits++;
                    if (emptyVisits >= maxEmptyVisits) {
                        break;
                    }
                    tempRehashIndex++;
                    bucketsToMove--;
                    continue;
                }

                // 迁移当前桶的所有节点
                while (entryToMove != null) {
                    final DictEntry<K, V> currentEntry = entryToMove;
                    final DictEntry<K, V> nextEntryInHt0 = currentEntry.next;

                    final int targetIdx = keyIndex(currentEntry.hash, localHt1.size);

                    // CAS插入到ht1
                    boolean casSuccess = false;
                    while (!casSuccess) {
                        DictEntry<K, V> oldHeadInHt1 = localHt1.table.get(targetIdx);
                        DictEntry<K, V> newHeadInHt1 = new DictEntry<>(
                                currentEntry.hash, currentEntry.key, currentEntry.value, oldHeadInHt1);
                        casSuccess = localHt1.table.compareAndSet(targetIdx, oldHeadInHt1, newHeadInHt1);
                        if (!casSuccess) {
                            LockSupport.parkNanos(1); // 微秒级退避
                        }
                    }

                    // 更新使用计数
                    localHt0.used.decrementAndGet();
                    localHt1.used.incrementAndGet();
                    movedAny = true;

                    entryToMove = nextEntryInHt0;
                }

                // CAS清空ht0的当前桶
                boolean clearSuccess = false;
                while (!clearSuccess) {
                    DictEntry<K, V> oldEntry = localHt0.table.get(tempRehashIndex);
                    if (oldEntry == null) {
                        clearSuccess = true;
                    } else {
                        clearSuccess = localHt0.table.compareAndSet(tempRehashIndex, oldEntry, null);
                        if (!clearSuccess) {
                            LockSupport.parkNanos(1);
                        }
                    }
                }

                bucketsToMove--;
                tempRehashIndex++;
                emptyVisits = 0; // 重置空桶计数
            }

            // 更新状态
            DictState<K, V> newState;
            if (tempRehashIndex >= localHt0.size && localHt0.used.get() == 0) {
                // rehash完成，提升ht1为ht0
                newState = new DictState<>(localHt1, null, -1);
            } else {
                // 更新rehash进度
                newState = current.withRehashIndex(tempRehashIndex);
            }

            if (state.compareAndSet(current, newState)) {
                return;
            }
            // CAS失败，重试
        }
    }

    /**
     * 启动渐进式rehash
     *
     * @param targetSize 目标哈希表大小，必须是2的幂次
     */
    private void startRehash(final int targetSize) {
        while (true) {
            DictState<K, V> current = state.get();
            if (current.rehashIndex != -1) {
                return; // 已在进行rehash
            }

            DictHashTable<K, V> newHt1 = new DictHashTable<>(targetSize);
            DictState<K, V> newState = current.withHt0AndHt1AndRehashIndex(current.ht0, newHt1, 0);

            if (state.compareAndSet(current, newState)) {
                rehashStep(); // 启动后立即执行一个步骤
                return;
            }
        }
    }

    /**
     * 检查是否需要收缩哈希表
     *
     * <p>当负载因子低于0.1且表大小大于初始大小时，启动收缩rehash。
     * 收缩可以节约内存，避免空间浪费。
     */
    private void checkShrinkIfNeeded() {
        DictState<K, V> current = state.get();

        if (current.rehashIndex != -1) {
            return; // 正在rehash，不进行收缩
        }

        final DictHashTable<K, V> localHt0 = current.ht0;
        if (localHt0 == null) {
            return;
        }

        final double loadFactor = (double) localHt0.used.get() / localHt0.size;

        if (loadFactor < 0.1 && localHt0.size > DICT_HT_INITIAL_SIZE) {
            int newSize = localHt0.size / 2;
            if (newSize < DICT_HT_INITIAL_SIZE) {
                newSize = DICT_HT_INITIAL_SIZE;
            }
            startRehash(newSize);
        }
    }

    // ========================= 快照功能 =========================

    /**
     * 创建字典的线程安全快照
     *
     * <p><strong>设计目标：</strong>
     * 为MiniRedis的RDB持久化和AOF重写提供数据一致性保证。
     *
     * <p><strong>核心特性：</strong>
     * <ul>
     *   <li><strong>一致性：</strong> 快照反映调用时刻的完整数据状态</li>
     *   <li><strong>隔离性：</strong> 快照创建期间不推进rehash，避免数据不一致</li>
     *   <li><strong>完整性：</strong> 包含所有有效键值对，无遗漏无重复</li>
     *   <li><strong>独立性：</strong> 返回的Map是完全独立的副本</li>
     * </ul>
     *
     * <p><strong>实现策略：</strong>
     * <ol>
     *   <li>获取当前状态的原子快照，避免中途状态变更</li>
     *   <li>遍历ht0，收集所有键值对</li>
     *   <li>如果正在rehash，同时遍历ht1</li>
     *   <li>利用HashMap的put语义处理重复键（ht1覆盖ht0）</li>
     * </ol>
     *
     * <p><strong>线程安全保证：</strong>
     * <ul>
     *   <li>基于快照语义，读取过程无锁</li>
     *   <li>利用AtomicReferenceArray的volatile语义</li>
     *   <li>不干扰正常的读写操作</li>
     * </ul>
     *
     * <p><strong>使用场景：</strong>
     * <ul>
     *   <li><strong>RDB持久化：</strong> 创建数据快照写入磁盘</li>
     *   <li><strong>AOF重写：</strong> 生成当前状态的完整命令序列</li>
     *   <li><strong>数据备份：</strong> 导出完整数据副本</li>
     *   <li><strong>调试诊断：</strong> 分析当前数据分布</li>
     * </ul>
     *
     * <p><strong>性能考量：</strong>
     * <ul>
     *   <li>时间复杂度：O(n)，n为键值对总数</li>
     *   <li>空间复杂度：O(n)，创建完整数据副本</li>
     *   <li>适合中等规模数据集的快照创建</li>
     * </ul>
     *
     * @return 当前字典数据的独立快照Map，保证线程安全和数据一致性
     * @throws RuntimeException 如果快照创建过程中发生异常
     */
    public Map<K, V> createSafeSnapshot() {
        final Map<K, V> snapshot = new HashMap<>();

        try {
            // 获取当前状态的原子快照，确保遍历过程中状态不变
            final DictState<K, V> current = state.get();
            final DictHashTable<K, V> localHt0 = current.ht0;
            final DictHashTable<K, V> localHt1 = current.ht1;
            final int localRehashIndex = current.rehashIndex;

            // 遍历主哈希表ht0，收集所有键值对
            createSnapshotFromTable(localHt0, snapshot);

            // 如果正在进行渐进式rehash，同时遍历辅助哈希表ht1
            // 对于重复的键，ht1中的值会覆盖ht0中的值（符合rehash语义）
            if (localRehashIndex != -1 && localHt1 != null) {
                createSnapshotFromTable(localHt1, snapshot);
            }
        } catch (Exception e) {
            throw new RuntimeException("创建快照时发生错误: " + e.getMessage(), e);
        }

        return snapshot;
    }

    /**
     * 从指定哈希表创建快照数据
     *
     * <p>遍历哈希表的每个桶，将所有键值对添加到快照Map中。
     *
     * @param table    源哈希表
     * @param snapshot 目标快照Map
     */
    private void createSnapshotFromTable(final DictHashTable<K, V> table, final Map<K, V> snapshot) {
        if (table == null || table.table == null) {
            return;
        }

        // 遍历哈希表的每个桶
        for (int i = 0; i < table.size; i++) {
            DictEntry<K, V> current = table.table.get(i);

            // 遍历桶中的链表
            while (current != null) {
                snapshot.put(current.key, current.value);
                current = current.next;
            }
        }
    }
}