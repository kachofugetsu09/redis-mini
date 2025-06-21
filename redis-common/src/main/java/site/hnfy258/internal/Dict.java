package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray; // 保持 AtomicReferenceArray 以便更明确地表示数组元素的可见性

/**
 * 基于Redis源码设计的Dict实现
 *
 * <p>主要特性：
 * <ul>
 * <li>**严格单线程写入**：所有写操作（put/remove/rehash）都在主线程（命令执行线程）中串行执行，
 * 由类级别的 `synchronized(this)` 保护。</li>
 * <li>**无锁读取**：主线程的 `get`/`find`/`containsKey` 操作是无锁的，通过 volatile 保证可见性。</li>
 * <li>**后台弱一致性读取（快照）**：后台线程的 `createSafeSnapshot()` 和迭代器，通过类级别锁 + 局部拷贝确保快照一致性，
 * 并避免 `ConcurrentModificationException`。</li>
 * <li>**渐进式rehash**：在主线程的写命令执行过程中渐进式完成，不阻塞读操作。</li>
 * </ul>
 *
 * <p>并发模型：
 * <ul>
 * <li>写操作（put, remove, rehash）：**由 `synchronized(this)` 保护**，确保串行执行和可见性。
 * 内部不再需要桶锁，因为主线程的写操作互斥。</li>
 * <li>读操作（get, containsKey, find）：**无锁读取**，直接访问 `volatile` 字段。
 * 它们不会触发或等待 rehashStep。</li>
 * <li>后台读操作（keySet, getAll, createSafeSnapshot）：这些方法现在也将在其内部获取 `synchronized(this)` 锁来创建快照，
 * 确保在拷贝数据时主线程的写操作被短暂阻塞，从而获得一个在某个时间点上一致的快照。</li>
 * <li>rehash：仅由写操作（`put`, `remove`）触发和推进，且在 `synchronized(this)` 保护下进行。</li>
 * </ul>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class Dict<K,V> {

    // 常量定义
    static final int DICT_HT_INITIAL_SIZE = 4;
    private static final int DICT_REHASH_BUCKETS_PER_STEP = 100; // 每步rehash处理的桶数量
    private static final int DICT_REHASH_MAX_EMPTY_VISITS = 10; // 连续空桶的最大访问次数

    // 哈希表和rehash状态
    volatile DictHashTable<K,V> ht0; // 主哈希表
    volatile DictHashTable<K,V> ht1; // rehash时的目标哈希表
    volatile int rehashIndex; // rehash索引，-1表示没有rehash，>=0表示rehash正在进行

    public Dict() {
        ht0 = new DictHashTable<>(DICT_HT_INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
    }

    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        // 使用 createSafeSnapshot 获取线程安全的快照
        final Map<K, V> snapshot = createSafeSnapshot();
        
        // 将类型化的 Map 转换为 Object 类型的 Map
        final Map<Object, Object> result = new HashMap<>();
        for (Map.Entry<K, V> entry : snapshot.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        
        // 返回 entrySet 视图
        return result.entrySet();
    }

    /**
     * 哈希表的内部实现
     * table使用AtomicReferenceArray来保证对桶头引用的原子性更新和可见性
     * used使用AtomicLong来保证计数的原子性
     */
    static class DictHashTable<K,V> {
        final AtomicReferenceArray<DictEntry<K,V>> table; // 存储桶的数组
        final int size; // 数组大小 (2的幂)
        final int sizemask; // 用于计算索引 (size - 1)
        final AtomicLong used; // 已使用的桶数量

        DictHashTable(final int size) {
            if (size <= 0 || (size & (size - 1)) != 0) {
                throw new IllegalArgumentException("Hash table size must be a power of 2, got: " + size);
            }
            this.table = new AtomicReferenceArray<>(size);
            this.size = size;
            this.sizemask = size - 1;
            this.used = new AtomicLong(0);
        }
    }

    /**
     * 哈希表节点
     * value和next是volatile，确保在主线程修改后，后台读线程能立即看到最新值。
     */
    static class DictEntry<K,V> {
        final K key;
        volatile V value; // volatile: 确保对值的修改对所有线程可见
        volatile DictEntry<K,V> next; // volatile: 确保链表结构的修改对所有线程可见
        final int hash;

        DictEntry(final K key, final V value, final int hash) {
            this.key = key;
            this.value = value;
            this.hash = hash;
            this.next = null;
        }

        DictEntry(final K key, final V value, final int hash, final DictEntry<K,V> next) {
            this.key = key;
            this.value = value;
            this.hash = hash;
            this.next = next;
        }
    }

    /**
     * 使用高质量哈希函数计算hash值
     * @param key 要计算哈希值的键
     * @return 32位哈希值（保证非负）
     */
    private int hash(final Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        h = (h ^ 61) ^ (h >>> 16);
        h = h + (h << 3);
        h = h ^ (h >>> 4);
        h = h * 0x27d4eb2d;
        h = h ^ (h >>> 15);
        return h & 0x7FFFFFFF;
    }

    /**
     * 使用位运算计算索引
     * @param hashValue 已计算的hash值
     * @param size 哈希表大小（必须是2的幂次）
     * @return 桶索引（0到size-1）
     */
    private int keyIndex(final int hashValue, final int size) {
        return hashValue & (size - 1);
    }

    /**
     * 查找键值对。无锁读取。
     * @param key 要查找的键
     * @return 找到的DictEntry或null
     */
    private DictEntry<K, V> find(final K key) {
        if (key == null) return null;
        final int keyHash = hash(key);

        // 获取哈希表的本地快照引用，防止rehash期间引用变化
        final DictHashTable<K, V> currentHt0 = ht0; // volatile read
        final DictHashTable<K, V> currentHt1 = ht1; // volatile read
        final int currentRehashIndex = rehashIndex; // volatile read

        // 先在ht0中查找
        if (currentHt0 != null && currentHt0.table != null) {
            final int idx0 = keyIndex(keyHash, currentHt0.size);
            if (idx0 >= 0 && idx0 < currentHt0.table.length()) { // 边界检查
                DictEntry<K, V> entry = currentHt0.table.get(idx0); // volatile read
                while (entry != null) {
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next; // volatile read
                }
            }
        }

        // 如果正在rehash且在ht0中未找到，则在ht1中查找
        if (currentRehashIndex != -1 && currentHt1 != null && currentHt1.table != null) {
            final int idx1 = keyIndex(keyHash, currentHt1.size);
            if (idx1 >= 0 && idx1 < currentHt1.table.length()) { // 边界检查
                DictEntry<K, V> entry = currentHt1.table.get(idx1); // volatile read
                while (entry != null) {
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next; // volatile read
                }
            }
        }
        return null;
    }

    public V get(final K key) {
        if (key == null) {
            return null;
        }

        // 调用内部的 find 方法，该方法是无锁的，并且已经处理了 rehash 期间的查找逻辑
        final DictEntry<K, V> entry = find(key);

        // 如果找到了条目，则返回其 volatile 的 value 字段，否则返回 null
        return (entry != null) ? entry.value : null;
    }

    /**
     * 检查键是否存在。无锁读取。
     * @param key 键
     * @return true如果键存在，否则返回false
     */
    public boolean containsKey(K key) {
        if(key == null) return false;
        // 读操作不推进rehashStep
        return find(key) != null;
    }

    /**
     * 检查键值对是否存在 (ZSET场景)。无锁读取。
     * @param score 分数
     * @param member 成员
     * @return true如果键值对存在，否则返回false
     */
    public boolean contains(K score, V member) {
        if(score == null) return false;
        // 读操作不推进rehashStep
        DictEntry<K, V> entry = find(score); // find是无锁的
        while (entry != null) {
            if(member.equals(entry.value)){ // 直接比较值
                return true;
            }
            entry = entry.next;
        }
        return false;
    }

    /**
     * 向哈希表插入或更新键值对。主线程串行写操作，由类级别锁保护。
     * @param key 要插入的键
     * @param value 要插入的值
     * @return 旧值或null
     */
    public V put(final K key, final V value) {
        if (key == null) throw new IllegalArgumentException("key can not be null");
        final int keyHash = hash(key);

        // 所有写操作都通过 synchronized(this) 保护，确保原子性、可见性和线程安全
        synchronized (this) {
            // 检查是否需要扩容或推进rehash
            if (rehashIndex == -1 && ht0 != null) {
                final long currentUsed = ht0.used.get();
                // Redis扩容策略：当负载因子 >= 1.0 (ht[0]->used >= ht[0]->size) 且 ht0 已达到初始大小才考虑扩容
                // 考虑到即将插入的新元素，所以是 currentUsed + 1
                if ((currentUsed + 1) >= ht0.size && ht0.size >= DICT_HT_INITIAL_SIZE) {
                    int newSize = Math.max(DICT_HT_INITIAL_SIZE, (int)((currentUsed + 1) * 2));
                    newSize = adjustToPowerOfTwo(newSize);
                    startRehash(newSize);
                }
            }

            // 总是尝试推进rehash一步
            if (rehashIndex != -1) {
                rehashStep();
            }

            // 选择要插入的哈希表（rehash时插入到ht1，否则插入到ht0）
            final DictHashTable<K, V> targetHt = (rehashIndex != -1 && ht1 != null) ? ht1 : ht0;
            if (targetHt == null || targetHt.table == null) {
                throw new IllegalStateException("Hash table not properly initialized");
            }

            final int idx = keyIndex(keyHash, targetHt.size);
            if (idx >= targetHt.table.length() || idx < 0) {
                throw new IllegalStateException("Calculated index " + idx + " is out of bounds for table size " + targetHt.table.length());
            }

            // 执行插入操作（在类级别锁保护下，无需桶锁）
            DictEntry<K, V> head = targetHt.table.get(idx);
            DictEntry<K, V> current = head;
            while (current != null) {
                if (current.hash == keyHash && key.equals(current.key)) {
                    V oldValue = current.value;
                    current.value = value; // volatile write
                    return oldValue;
                }
                current = current.next;
            }

            // 没找到相同key，在头部插入新节点
            DictEntry<K, V> newEntry = new DictEntry<>(key, value, keyHash, head);
            targetHt.table.set(idx, newEntry); // volatile write
            targetHt.used.incrementAndGet(); // Atomic update
            return null;
        }
    }

    /**
     * 从哈希表中删除键值对。主线程串行写操作，由类级别锁保护。
     * @param key 要删除的键
     * @return 删除的值，如果不存在则返回null
     */
    public V remove(final K key) {
        if (key == null) return null;
        final int keyHash = hash(key);

        synchronized (this) { // 所有写操作都通过 synchronized(this) 保护
            // 总是尝试推进rehash一步
            if (rehashIndex != -1) {
                rehashStep();
            }

            V removedValue = null;

            // 先尝试从ht0删除
            if (ht0 != null && ht0.table != null) {
                final int idx0 = keyIndex(keyHash, ht0.size);
                removedValue = removeFromTable(ht0, idx0, key, keyHash);
            }

            // 如果ht0中没找到且正在rehash，则从ht1删除
            if (removedValue == null && rehashIndex != -1 && ht1 != null && ht1.table != null) {
                final int idx1 = keyIndex(keyHash, ht1.size);
                removedValue = removeFromTable(ht1, idx1, key, keyHash);
            }

            // 删除成功后检查是否需要缩容
            if (removedValue != null && rehashIndex == -1) {
                checkShrinkIfNeeded();
            }
            return removedValue;
        }
    }

    /**
     * 从指定的哈希表中删除键值对。在类级别锁保护下，无需桶锁。
     * @param table 目标哈希表
     * @param idx 桶索引
     * @param key 要删除的键
     * @param keyHash 键的hash值
     * @return 删除的值，如果不存在则返回null
     */
    private V removeFromTable(final DictHashTable<K, V> table, final int idx,
                              final K key, final int keyHash) {
        // 此方法现在在 synchronized(this) 块内调用，确保主线程写操作的串行性
        // 因此不再需要桶锁。
        DictEntry<K, V> current = table.table.get(idx);
        DictEntry<K, V> prev = null;

        while (current != null) {
            if (current.hash == keyHash && key.equals(current.key)) {
                if (prev == null) {
                    table.table.set(idx, current.next); // volatile write
                } else {
                    prev.next = current.next; // volatile write
                }
                table.used.decrementAndGet(); // Atomic update
                return current.value;
            }
            prev = current;
            current = current.next;
        }
        return null;
    }


    /**
     * 启动rehash过程。主线程串行写操作，由类级别锁保护。
     * @param targetSize 目标大小（必须是2的幂次）
     */
    private synchronized void startRehash(final int targetSize) {
        if (rehashIndex != -1) { // 已经在rehash中
            return;
        }

        int newSize = adjustToPowerOfTwo(targetSize);
        ht1 = new DictHashTable<>(newSize);
        rehashIndex = 0;

        // 对于小表（如初始表），立即完成rehash，避免频繁小步长rehash
        if (ht0.size <= DICT_HT_INITIAL_SIZE) {
            while (rehashIndex != -1) { // 持续rehashStep直到完成
                rehashStep();
            }
        }
    }

    /**
     * 执行一步渐进式rehash。主线程串行写操作，由类级别锁保护。
     */
    synchronized void rehashStep() {
        // 1. 检查是否没有rehash在进行，或ht0/ht1为空，或rehashIndex已超出ht0范围。
        // 如果满足这些条件，且ht0已经清空（所有元素已迁移），则完成rehash。
        if (rehashIndex == -1 || ht0 == null || ht0.table == null || ht1 == null || ht1.table == null) {
            // 这通常不应该发生，但作为防御性检查。
            // 确保 rehashIndex 已经完成遍历且 ht0 已清空
            if (rehashIndex != -1 && ht0 != null && ht0.used.get() == 0) {
                ht0 = ht1; // 完成rehash
                ht1 = null;
                rehashIndex = -1;
            }
            return;
        }

        int emptyVisits = 0;
        int bucketsToProcessInThisStep = DICT_REHASH_BUCKETS_PER_STEP; // 每次rehash步骤尝试处理的桶总数

        // 核心的渐进式rehash循环：尝试处理固定数量的桶
        while (bucketsToProcessInThisStep > 0 && rehashIndex < ht0.size) {
            // 获取源桶（在类级别锁保护下，无需桶锁）
            DictEntry<K, V> entry = ht0.table.get(rehashIndex); // volatile read

            // 如果桶为空
            if (entry == null) {
                // 递增rehashIndex并检查空桶访问限制
                rehashIndex++;
                emptyVisits++;
                if (emptyVisits >= DICT_REHASH_MAX_EMPTY_VISITS) {
                    // 达到最大空桶访问数，停止本次rehash步骤
                    break;
                }
                bucketsToProcessInThisStep--; // 即使是空桶，也算处理了一个桶
                continue; // 继续下一个循环迭代，寻找下一个桶
            }

            // 如果桶不为空，处理迁移
            // 遍历并迁移链表中的每一个节点
            DictEntry<K, V> currentEntry = entry;
            while (currentEntry != null) {
                final DictEntry<K, V> nextEntry = currentEntry.next; // 保存下一个节点

                // 计算当前节点在ht1中的目标索引
                final int targetIdx = keyIndex(currentEntry.hash, ht1.size);

                // 在类级别锁保护下，对ht1的操作也是安全的
                // 头插法插入到ht1
                currentEntry.next = ht1.table.get(targetIdx); // volatile read
                ht1.table.set(targetIdx, currentEntry); // volatile write

                // 更新used计数
                ht1.used.incrementAndGet(); // Atomic update
                ht0.used.decrementAndGet(); // Atomic update

                currentEntry = nextEntry;
            }

            // 成功迁移整个桶后，清空ht0中的原桶
            ht0.table.set(rehashIndex, null); // volatile write

            // 成功处理一个非空桶，递增rehashIndex并减少待处理桶数
            rehashIndex++;
            bucketsToProcessInThisStep--;
            emptyVisits = 0; // 重置 emptyVisits，因为我们找到了一个非空桶
        }

        // 检查rehash是否完成：当 rehashIndex 遍历完 ht0 的所有桶，并且 ht0 已经清空。
        if (rehashIndex >= ht0.size && ht0.used.get() == 0) {
            ht0 = ht1; // 完成rehash
            ht1 = null;
            rehashIndex = -1;
        }
    }

    /**
     * 调整大小为2的幂次
     * @param size 目标大小
     * @return 不小于size的最小2的幂次
     */
    private int adjustToPowerOfTwo(int size) {
        int n = size - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= (1 << 30)) ? (1 << 30) : n + 1;
    }

    /**
     * 检查是否需要缩容。主线程串行写操作，由类级别锁保护。
     */
    private synchronized void checkShrinkIfNeeded() {
        if (rehashIndex != -1 || ht0 == null) { // 如果正在rehash，或ht0为空，不缩容
            return;
        }

        double loadFactor = (double) ht0.used.get() / ht0.size;

        // Redis的缩容条件：used/size < 0.1 且表大小大于初始大小
        if (loadFactor < 0.1 && ht0.size > DICT_HT_INITIAL_SIZE) {
            int newSize = ht0.size / 2;
            if (newSize < DICT_HT_INITIAL_SIZE) {
                newSize = DICT_HT_INITIAL_SIZE;
            }
            startRehash(newSize);
        }
    }

    /**
     * 清空哈希表。主线程串行写操作，由类级别锁保护。
     */
    public synchronized void clear() {
        ht0 = new DictHashTable<K, V>(DICT_HT_INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
    }

    /**
     * 获取线程安全的元素数量。
     * @return 元素总数
     */
    public int size(){
        long totalSize = ht0.used.get();
        if (ht1 != null) {
            totalSize += ht1.used.get();
        }
        return (int) totalSize;
    }

    /**
     * 获取所有键的集合。后台读操作，由类级别锁保护以创建快照。
     * @return 当前所有键的集合
     */
    public Set<K> keySet(){
        // 通过 createSafeSnapshot 获取快照，然后从快照中提取键
        return createSafeSnapshot().keySet();
    }

    /**
     * 获取所有键值对。后台读操作，由类级别锁保护以创建快照。
     * @return 当前所有键值对的Map
     */
    public Map<K, V> getAll(){
        return createSafeSnapshot(); // 直接返回 createSafeSnapshot 的结果
    }


    /**
     * 获取线程安全的快照 - 专门用于RDB保存和AOF重写。
     * 采用Redis的策略：在获取快照时，**短暂阻塞主线程写操作**以确保一致性。
     * 这是通过 `synchronized(this)` 实现的。
     *
     * @return 当前数据的线程安全快照
     */
    public synchronized Map<K, V> createSafeSnapshot() {
        final Map<K, V> snapshot = new HashMap<>();

        // 1. 在获取快照时，由于方法是 synchronized 的，主线程的写操作会被阻塞。
        // 这与Redis的行为一致：在SAVE/BGSAVE时，也会短暂阻塞写操作以获取一致的内存视图。

        // 2. 如果正在rehash，先完成当前的rehash步骤，确保所有数据都已迁移到ht1（如果可以）或稳定。
        // Redis在持久化时，会强制完成rehash。
        while (rehashIndex != -1) {
            rehashStep(); // 持续调用rehashStep直到rehash完成
        }

        // 3. 此时 rehashIndex 应该为 -1，ht1 为 null，所有数据都在 ht0 中。
        if (ht0 != null && ht0.table != null) {
            // 遍历ht0，将所有键值对添加到快照中
            for (int i = 0; i < ht0.size; i++) {
                DictEntry<K, V> entry = ht0.table.get(i); // volatile read
                while (entry != null) {
                    snapshot.put(entry.key, entry.value);
                    entry = entry.next; // volatile read
                }
            }
        }
        // ht1 此时应为 null，所以无需遍历 ht1

        return snapshot;
    }

}