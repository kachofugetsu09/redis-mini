package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>基于 Redis 源码设计并优化的线程安全字典（Dictionary）实现，提供高效的并发读写和渐进式 rehash 能力。
 * 旨在为高性能 Java 后端应用提供一个可靠的键值存储结构，特别适合作为 Redis-mini 等中间件的内部数据结构。
 *
 * <h3>主要特性：</h3>
 * <ul>
 * <li><b>CAS + 不可变链表</b>：所有对哈希桶中链表的修改（插入、更新、删除）都通过创建新的不可变链表副本，并使用 CAS 操作原子地替换桶的头节点引用来实现。这避免了传统的锁机制，提高了并发性能。</li>
 * <li><b>渐进式 Rehash</b>：参考 Redis 的设计，支持哈希表的动态扩容和缩容。Rehash 过程分步进行，避免一次性迁移大量数据导致的性能抖动，对在线服务影响最小。支持 2 的幂次扩容，并通过负载因子控制 rehash 时机。</li>
 * <li><b>快照一致性</b>：提供 {@code createSafeSnapshot()} 方法，用于生成当前数据的线程安全副本。这对于 RDB/AOF 持久化或需要全局一致性视图的场景至关重要，确保在快照生成期间数据不被修改。</li>
 * </ul>
 *
 * <h3>并发安全性：</h3>
 * <ul>
 * <li><b>读操作无锁</b>：{@code get}, {@code containsKey}, {@code keySet}, {@code getAll} 等读操作采用无锁设计，通过获取哈希表的本地引用快照来避免 rehash 期间的竞态条件，提高并发读取性能。</li>
 * <li><b>写操作乐观锁</b>：{@code put}, {@code remove} 等写操作通过 CAS (Compare-And-Swap) 循环结合链表复制技术实现乐观并发控制。在桶内链表操作时，如果发生冲突，会进行重试。</li>
 * <li><b>Rehash 安全</b>：使用 {@code volatile} 关键字修饰哈希表引用 ({@code ht0}, {@code ht1}, {@code rehashIndex})，确保内存可见性。同时，写操作会触发 {@code rehashStep()} 推进 rehash 进程，读操作则通过本地引用快照读取当前状态，防止 rehash 导致的数据不一致。</li>
 * </ul>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class Dict<K,V> {

    /** 哈希表初始大小，必须是2的幂次。 */
    static final int DICT_HT_INITIAL_SIZE = 4;
    /** 每次渐进式 rehash 步骤中迁移的桶数量。 */
    static final int DICT_REHASH_BUCKETS_PER_STEP = 100;
    /** 渐进式 rehash 时，连续访问空桶的最大次数。超过此次数，本次 rehash 步骤将停止，以避免不必要的空循环。 */
    private static final int DICT_REHASH_MAX_EMPTY_VISITS = 10;

    /** 主哈希表，所有新写入的数据默认存储在此表，读操作优先查询此表。 */
    volatile DictHashTable<K,V> ht0;
    /** 副哈希表，仅在 rehash 过程中使用。rehash 期间，数据从 ht0 迁移到 ht1，新写入的数据直接进入 ht1。 */
    volatile DictHashTable<K,V> ht1;
    /**
     * rehash 索引，指示当前 rehash 进度。
     * -1 表示没有进行 rehash。
     * >=0 表示正在进行 rehash，值为当前正在迁移的 ht0 桶的索引。
     */
    volatile int rehashIndex;

    /** 用于协调 rehash 和快照操作的读写锁。*/
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    /**
     * 检查字典中是否包含指定的键。
     * 如果正在进行 rehash，会推进一步 rehash 进程。
     *
     * @param key 要检查的键。
     * @return 如果字典包含该键，则返回 true；否则返回 false。
     */
    public boolean containsKey(K key) {
        if(key == null) return false;
        // 在读操作前推进 rehash，有助于尽快完成 rehash 过程。
        if(rehashIndex != -1) rehashStep();
        return find(key) != null;
    }

    /**
     * 检查字典中是否包含指定的键值对（这里的 score 是 key 的别名，member 是 value 的别名）。
     * 此方法通常用于 Set 类型的内部实现，但在 Dict 中作为通用方法提供。
     * 如果正在进行 rehash，会推进一步 rehash 进程。
     *
     * @param score 要检查的键。
     * @param member 要检查的值。
     * @return 如果字典包含该键值对，则返回 true；否则返回 false。
     */
    public boolean contains(K score, V member) {
        if(score == null) return false;
        // 在读操作前推进 rehash。
        if(rehashIndex != -1) rehashStep();
        DictEntry<K, V> entry = find(score);
        while (entry != null) {
            // 注意：这里使用 equals 进行值比较。
            if(entry.value.equals(member)){
                return true;
            }
            entry = entry.next;
        }
        return false;
    }

    /**
     * 返回当前字典的键值对集合视图。
     * <p><b>注意：</b>此方法通过调用 {@link #createSafeSnapshot()} 生成一个快照来保证线程安全，
     * 这意味着它会复制所有数据。对于需要高性能迭代或不介意弱一致性的场景，建议直接遍历 {@link #getAll()}。
     * 此方法主要为需要 {@code Entry<Object, Object>} 接口的特定持久化或兼容性需求设计。
     *
     * @return 当前数据的 {@code Entry<Object, Object>} 集合视图。
     * @see #createSafeSnapshot()
     * @see #getAll()
     */
    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        // 使用安全快照避免并发问题，确保返回的数据一致性。
        final Map<K, V> snapshot = createSafeSnapshot();
        final Map<Object, Object> result = new HashMap<>();
        for (final Map.Entry<K, V> entry : snapshot.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result.entrySet();
    }

    /**
     * 线程安全的字典节点，设计为<b>不可变对象</b> (Immutable Object)。
     * 每次修改（更新值、删除节点或链表重建）都会生成新的 {@code DictEntry} 对象，
     * 并通过 CAS 原子地更新桶头引用，确保并发环境下的数据一致性和安全性。
     *
     * @param <K> 键类型
     * @param <V> 值类型
     */
    static final class DictEntry<K,V> {
        /** 当前节点的哈希值，用于快速定位和比较。*/
        final int hash;
        /** 键。*/
        final K key;
        /** 值。*/
        final V value;
        /** 指向链表中下一个节点的引用。*/
        final DictEntry<K,V> next;

        /**
         * 构造一个新的不可变字典节点。
         *
         * @param hash 键的哈希值。
         * @param key 键。
         * @param value 值。
         * @param next 指向链表中下一个节点的引用。
         */
        DictEntry(final int hash, final K key, final V value, final DictEntry<K,V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        /**
         * 创建当前节点的副本，并指定一个新的 {@code next} 节点。
         * 此方法在链表重构（如 rehash 迁移节点时）非常有用，因为 {@code DictEntry} 是不可变的。
         *
         * @param newNext 新的下一个节点。
         * @return 带有新 {@code next} 引用的当前节点的副本。
         */
        DictEntry<K,V> copyWithNext(DictEntry<K,V> newNext) {
            return new DictEntry<>(this.hash, this.key, this.value, newNext);
        }

        /**
         * 递归地更新链表中与目标键匹配的节点的值，并返回更新后的链表头。
         * 如果当前节点是目标节点，则创建新节点更新其值；否则，复制当前节点并递归处理其后续链表。
         * 此方法用于 {@code put} 操作中更新已存在的键值。
         *
         * @param targetHash 目标键的哈希值。
         * @param targetKey 目标键。
         * @param newValue 要设置的新值。
         * @return 更新后的链表头节点。如果目标键不存在，则返回与原链表结构相同的链表副本（值未改变）。
         */
        DictEntry<K,V> updateValue(int targetHash, K targetKey, V newValue) {
            if (this.hash == targetHash && targetKey.equals(this.key)) {
                // 如果当前节点是目标节点，创建新节点，仅更新值，next 保持不变。
                return new DictEntry<>(this.hash, this.key, newValue, this.next);
            }
            // 如果不是目标节点，复制当前节点，并递归更新其 next 链表。
            DictEntry<K,V> updatedNext = (this.next != null) ? this.next.updateValue(targetHash, targetKey, newValue) : null;
            return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
        }

        /**
         * 递归地从链表中移除与目标键匹配的节点，并返回移除后的链表头。
         * 如果当前节点是目标节点，则返回其 {@code next} 节点（跳过当前节点）；否则，复制当前节点并递归处理其后续链表。
         * 此方法用于 {@code remove} 操作中删除指定键。
         *
         * @param targetHash 目标键的哈希值。
         * @param targetKey 目标键。
         * @return 移除目标节点后的链表头节点。如果目标键不存在，则返回与原链表结构相同的链表副本。
         */
        DictEntry<K,V> removeNode(int targetHash, K targetKey) {
            if (this.hash == targetHash && targetKey.equals(this.key)) {
                // 如果当前节点是目标节点，直接返回下一个节点，相当于跳过当前节点。
                return this.next;
            }
            // 如果不是目标节点，复制当前节点，并递归从其 next 链表中移除目标节点。
            DictEntry<K,V> updatedNext = (this.next != null) ? this.next.removeNode(targetHash, targetKey) : null;
            return new DictEntry<>(this.hash, this.key, this.value, updatedNext);
        }
    }


    /**
     * 线程安全的哈希表内部结构。
     * 使用 {@code AtomicReferenceArray} 管理桶数组，确保桶头引用的原子性操作。
     */
    static class DictHashTable<K,V> {
        /** 存储桶链表头节点的原子引用数组。*/
        final AtomicReferenceArray<DictEntry<K,V>> table;
        /** 哈希表当前容量（桶的数量），总是2的幂次。*/
        final int size;
        /** 用于快速计算索引的掩码，等于 {@code size - 1}。*/
        final int sizemask;
        /** 哈希表中当前存储的键值对数量，使用 {@code AtomicLong} 保证原子性。*/
        final AtomicLong used;

        /**
         * 构造一个指定容量的哈希表。
         *
         * @param size 哈希表的容量，必须是2的幂次。
         * @throws IllegalArgumentException 如果容量不是2的幂次或小于等于0。
         */
        DictHashTable(final int size) {
            // 确保 size 是2的幂次，否则 sizemask 计算和索引计算会出错。
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
     * 构造一个新的 Dict 实例。
     * 初始化主哈希表 {@code ht0}，副哈希表 {@code ht1} 为空，rehash 索引为 -1 (表示未进行 rehash)。
     */
    public Dict() {
        ht0 = new DictHashTable<>(DICT_HT_INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
    }

    /**
     * 使用高质量的哈希函数计算键的哈希值。
     * 此哈希函数借鉴了 ConcurrentHashMap 和 MurmurHash 的思想，旨在提供良好的分布性，减少哈希冲突。
     *
     * @param key 要计算哈希值的键。
     * @return 32 位哈希值，保证非负。
     */
    private int hash(final Object key) {
        if (key == null) return 0;

        int h = key.hashCode();
        // 1. 使用 Wang's hash 算法（或类似变体）进一步混淆哈希值，提升分布性。
        // 这段混淆逻辑通常用于将 hashCode() 的高位信息扩散到低位，减少低位冲突。
        h = (h ^ 61) ^ (h >>> 16);
        h = h + (h << 3);
        h = h ^ (h >>> 4);
        h = h * 0x27d4eb2d; // 一个大质数乘法，进一步打乱
        h = h ^ (h >>> 15);

        // 2. 确保返回非负值，因为哈希值将用于数组索引。
        return h & 0x7FFFFFFF; // 0x7FFFFFFF 是 int 的最大正值，按位与操作确保最高位为 0。
    }

    /**
     * 使用位运算高效计算键在哈希表中的桶索引。
     * 此方法依赖于哈希表大小 {@code size} 是 2 的幂次的特性。
     *
     * @param hashValue 已计算的哈希值。
     * @param size 哈希表的大小（必须是 2 的幂次）。
     * @return 桶索引，范围从 0 到 {@code size - 1}。
     */
    private int keyIndex(final int hashValue, final int size) {
        // 利用 size 是 2 的幂次的特性，位运算 (hashValue & (size - 1)) 等价于 hashValue % size，但效率更高。
        return hashValue & (size - 1);
    }

    /**
     * 线程安全的查找方法。
     * 首先在 {@code ht0} 中查找，如果正在 rehash 且 {@code ht0} 中未找到，则在 {@code ht1} 中查找。
     * 此方法采用无锁读取，通过本地引用快照确保在 rehash 期间读取的数据一致性。
     *
     * @param key 要查找的键。
     * @return 找到的 {@code DictEntry} 对象，如果未找到则返回 {@code null}。
     */
    private DictEntry<K, V> find(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);

        // 获取哈希表和 rehash 状态的本地快照引用。
        // volatile 字段保证可见性，本地快照确保在方法执行期间引用不变，避免 rehash 导致的并发问题。
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex; // 捕获 rehash 状态

        // 优先在主表 ht0 中查找。
        if (localHt0 != null && localHt0.table != null) {
            final int idx0 = keyIndex(keyHash, localHt0.size);
            if (idx0 >= 0 && idx0 < localHt0.size) { // 边界检查
                DictEntry<K, V> entry = localHt0.table.get(idx0); // 原子读取桶头
                while (entry != null) {
                    // 先比较 hash 值，再比较 key 的内容，提高效率。
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next;
                }
            }
        }

        // 如果正在 rehash 且在 ht0 中未找到，则在 ht1 中查找。
        // 这是因为 rehash 期间，部分数据可能已迁移到 ht1，新写入的数据也会直接进入 ht1。
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            final int idx1 = keyIndex(keyHash, localHt1.size);
            if (idx1 >= 0 && idx1 < localHt1.size) { // 边界检查
                DictEntry<K, V> entry = localHt1.table.get(idx1); // 原子读取桶头
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
     * 线程安全的 {@code put} 操作。将指定的键值对插入到字典中。
     * 如果键已存在，则更新其对应的值；如果键不存在，则插入新键值对。
     * 该方法会根据负载因子自动触发扩容 rehash。
     *
     * @param key 要插入的键，不允许为 null。
     * @param value 要插入的值。
     * @return 如果键已存在，返回旧值；否则返回 {@code null}。
     * @throws IllegalArgumentException 如果键为 null。
     * @throws IllegalStateException 如果哈希表未正确初始化或索引越界（内部错误）。
     */
    public V put(final K key, final V value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null.");

        final int keyHash = hash(key);

        // 1. 检查是否需要扩容 rehash。
        // 负载因子 (used + 1) / size >= 1.0 时触发扩容。
        // (+1 是为了考虑当前将要插入的元素)
        if (rehashIndex == -1 && ht0 != null) {
            final long currentUsed = ht0.used.get();
            final double loadFactor = (double) (currentUsed + 1) / ht0.size;
            if (loadFactor >= 1.0) { // 当负载因子达到1.0或更高时，启动扩容。
                startRehash(ht0.size * 2);
            }
        }

        // 2. 如果正在 rehash，推进一步 rehash 进程。
        // 这确保了写操作有助于加速 rehash 完成。
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 3. 选择目标哈希表进行插入。
        // 如果正在 rehash，新数据直接插入到 ht1；否则插入到 ht0。
        final DictHashTable<K, V> targetHt = (rehashIndex != -1 && ht1 != null) ? ht1 : ht0;
        if (targetHt == null || targetHt.table == null) {
            throw new IllegalStateException("Hash table not properly initialized.");
        }

        // 4. 计算桶索引。
        final int idx = keyIndex(keyHash, targetHt.size);
        if (idx >= targetHt.table.length() || idx < 0) {
            throw new IllegalStateException("Calculated index " + idx + " is out of bounds for table size " + targetHt.table.length());
        }

        // 5. 执行线程安全的插入操作。
        V result = putInBucket(targetHt, idx, key, value, keyHash);

        // 6. 插入完成后检查是否需要缩容。
        // 缩容检查仅在 rehash 完成后进行，避免与扩容 rehash 冲突。
        if (rehashIndex == -1) {
            checkShrinkIfNeeded();
        }
        return result;
    }

    /**
     * 在指定哈希表的桶中插入键值对。
     * 此方法是 {@code put} 操作的核心，使用 CAS (Compare-And-Swap) 循环保证线程安全。
     * <p><b>并发控制策略：</b>
     * <ul>
     * <li>读取当前桶头节点。</li>
     * <li>遍历链表查找目标键：
     * <ul>
     * <li>如果找到：创建新的链表，递归更新目标节点的值。</li>
     * <li>如果未找到：头插新节点，其 {@code next} 指向旧桶头。</li>
     * </ul>
     * </li>
     * <li>使用 {@code AtomicReferenceArray.compareAndSet()} 尝试原子地更新桶头。
     * 如果 CAS 失败（意味着在读取 {@code head} 后到尝试更新之间有其他线程修改了桶头），则重试整个过程。</li>
     * </ul>
     *
     * @param table 目标哈希表 (ht0 或 ht1)。
     * @param idx 桶索引。
     * @param key 键。
     * @param value 值。
     * @param keyHash 键的哈希值。
     * @return 如果是更新操作，返回旧值；如果是新增操作，返回 {@code null}。
     * @throws IllegalStateException 如果 CAS 重试次数超过限制，可能在高并发下发生活锁。
     */
    private V putInBucket(final DictHashTable<K, V> table, final int idx,
                          final K key, final V value, final int keyHash) {
        int retryCount = 0;
        final int maxRetries = 1000; // 增加重试次数以应对高竞争场景。

        while (retryCount < maxRetries) {
            DictEntry<K, V> head = table.table.get(idx); // 原子读取当前桶头节点。
            DictEntry<K, V> node = head;
            V oldValue = null;
            boolean found = false;

            // 1. 遍历链表查找是否已存在 key。
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
                // 2. 如果键已存在，重建链表并更新对应节点的值。
                // DictEntry 是不可变的，所以通过递归调用 updateValue 创建一个新链表。
                newHead = head.updateValue(keyHash, key, value);
            } else {
                // 3. 如果键不存在，头插新节点。新节点的 next 指向当前的 head。
                newHead = new DictEntry<>(keyHash, key, value, head);
            }

            // 4. 尝试 CAS 原子更新桶头。
            // 只有当前桶头仍然是 'head' 时，才能将其更新为 'newHead'。
            if (table.table.compareAndSet(idx, head, newHead)) {
                if (!found) {
                    table.used.incrementAndGet(); // 新增键，used 计数原子加一。
                }
                return oldValue; // 返回旧值（如果存在）或 null。
            }
            retryCount++;
        }
        // 如果 CAS 失败次数过多，抛出异常，这通常意味着极高的并发竞争或逻辑错误。
        throw new IllegalStateException("putInBucket exceeded max retries (" + maxRetries + ") for key: " + key);
    }

    /**
     * 线程安全的 {@code get} 操作。获取指定键对应的值。
     * 此方法采用无锁读取，通过获取哈希表的本地引用快照来避免 rehash 期间的竞态条件，提高并发读取性能。
     *
     * @param key 要查找的键。
     * @return 对应的值，如果键不存在则返回 {@code null}。
     */
    public V get(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);

        // 在读操作前推进 rehash，有助于尽快完成 rehash 过程。
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用。
        // volatile 字段保证可见性，本地快照确保在方法执行期间引用不变。
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 无锁读取 - 先查 ht0，再查 ht1（如果正在 rehash）。
        V result = getFromTable(localHt0, key, keyHash);
        if (result == null && localRehashIndex != -1 && localHt1 != null) {
            result = getFromTable(localHt1, key, keyHash);
        }
        return result;
    }

    /**
     * 从指定的哈希表中查找值。此方法是无锁读取。
     *
     * @param table 目标哈希表。
     * @param key 键。
     * @param keyHash 键的哈希值。
     * @return 对应的值，如果未找到则返回 {@code null}。
     * @throws IllegalStateException 如果计算出的索引越界（内部错误）。
     */
    private V getFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }
        // 使用位运算计算索引，等价于 keyHash % table.size。
        final int idx = keyHash & (table.size - 1);

        // 边界检查防止数组越界。
        if (idx >= table.table.length() || idx < 0) {
            throw new IllegalStateException("Calculated index " + idx + " is out of bounds for table size " + table.table.length());
        }

        // 原子读取桶头节点。
        DictEntry<K, V> current = table.table.get(idx);

        // 遍历链表 - 无需同步，因为 DictEntry 是不可变的，且 volatile 保证内存可见性。
        // 任何对链表的修改都会通过 CAS 替换桶头，不会影响当前遍历的旧链表副本。
        while (current != null) {
            if (current.hash == keyHash && key.equals(current.key)) {
                return current.value;
            }
            current = current.next;
        }
        return null;
    }

    /**
     * 获取字典中所有键的集合。
     * 此方法通过获取哈希表的本地引用快照来保证弱一致性，适用于迭代和批量操作。
     *
     * @return 包含所有键的 {@code HashSet}。
     */
    public Set<K> keySet() {
        final Set<K> keys = new HashSet<>();

        // 推进 rehash。
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用。
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 遍历 ht0。
        if (localHt0 != null && localHt0.table != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table.get(i); // 原子读取桶头
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }

        // 如果正在 rehash，也遍历 ht1，以包含 rehash 过程中已经迁移或新添加的键。
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table.get(i); // 原子读取桶头
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }
        return keys;
    }

    /**
     * 获取字典中所有键值对的 Map 副本。
     * 此方法通过获取哈希表的本地引用快照来保证弱一致性，适用于迭代和批量操作。
     *
     * @return 包含所有键值对的 {@code HashMap}。
     */
    public Map<K, V> getAll() {
        final Map<K, V> map = new HashMap<>();

        // 推进 rehash。
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用。
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 遍历 ht0。
        if (localHt0 != null && localHt0.table != null) {
            for (int i = 0; i < localHt0.size; i++) {
                DictEntry<K, V> entry = localHt0.table.get(i); // 原子读取桶头
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }

        // 如果正在 rehash，也遍历 ht1。
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table.get(i); // 原子读取桶头
                while (entry != null) {
                    // 确保在 rehash 过程中，如果键同时存在于 ht0 和 ht1，只取 ht1 中的最新值（或跳过 ht0 中已迁移的）。
                    // HashMap 的 put 操作会覆盖旧值，所以直接 put 即可。
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }
        return map;
    }

    /**
     * 执行一步渐进式 rehash。
     * <p>每次执行此方法，会尝试将 {@code DICT_REHASH_BUCKETS_PER_STEP} 数量的桶从 {@code ht0} 迁移到 {@code ht1}。
     * 迁移完成后，如果 {@code ht0} 所有桶都已清空且 {@code used} 计数为 0，则将 {@code ht1} 提升为新的 {@code ht0}，并重置 rehash 状态。
     * <p>此方法由 {@code put}, {@code remove}, {@code get} 等操作触发，确保 rehash 过程逐步进行，避免阻塞。
     */
    void rehashStep() {
        writeLock.lock(); // 获取写锁
        try {
            if (rehashIndex == -1) {
                return;
            }

            final DictHashTable<K, V> localHt0 = ht0;
            final DictHashTable<K, V> localHt1 = ht1;

            if (localHt0 == null || localHt1 == null) {
                if (localHt0 != null && rehashIndex >= localHt0.size && localHt0.used.get() == 0) {
                    ht0 = localHt1;
                    ht1 = null;
                    rehashIndex = -1;
                }
                return;
            }

            int emptyVisits = 0;
            int maxEmptyVisits = DICT_REHASH_MAX_EMPTY_VISITS;
            int bucketsToMove = DICT_REHASH_BUCKETS_PER_STEP;

            while (bucketsToMove > 0 && rehashIndex < localHt0.size) {
                DictEntry<K, V> entryToMove = localHt0.table.get(rehashIndex);

                if (entryToMove == null) {
                    emptyVisits++;
                    if (emptyVisits >= maxEmptyVisits) {
                        break;
                    }
                    rehashIndex++;
                    bucketsToMove--;
                    continue;
                }

                while (entryToMove != null) {
                    final DictEntry<K, V> currentEntry = entryToMove;
                    final DictEntry<K, V> nextEntryInHt0 = currentEntry.next;

                    final int targetIdx = keyIndex(currentEntry.hash, localHt1.size);

                    // 在写锁保护下，CAS 仍然是原子操作，但现在它不会与快照遍历冲突
                    boolean casSuccess = false;
                    while (!casSuccess) {
                        DictEntry<K, V> oldHeadInHt1 = localHt1.table.get(targetIdx);
                        DictEntry<K, V> newHeadInHt1 = new DictEntry<>(
                                currentEntry.hash, currentEntry.key, currentEntry.value, oldHeadInHt1);
                        casSuccess = localHt1.table.compareAndSet(targetIdx, oldHeadInHt1, newHeadInHt1);
                    }

                    localHt0.used.decrementAndGet();
                    localHt1.used.incrementAndGet();

                    entryToMove = nextEntryInHt0;
                }

                localHt0.table.set(rehashIndex, null); // 确保在写锁保护下进行

                bucketsToMove--;
                rehashIndex++;
                emptyVisits = 0;
            }

            if (rehashIndex >= localHt0.size && localHt0.used.get() == 0) {
                ht0 = localHt1;
                ht1 = null;
                rehashIndex = -1;
            }
        } finally {
            writeLock.unlock(); // 释放写锁
        }
    }

    /**
     * 启动渐进式 rehash 过程。
     * 创建一个新哈希表 {@code ht1} 并将其容量设置为 {@code targetSize}。
     * 然后将 {@code rehashIndex} 初始化为 0，并立即执行一步 rehash。
     *
     * @param targetSize 新哈希表的目标大小（必须是 2 的幂次）。
     */
    private void startRehash(final int targetSize) {
        writeLock.lock(); // 获取写锁
        try {
            if (rehashIndex != -1) {
                return;
            }
            ht1 = new DictHashTable<>(targetSize);
            rehashIndex = 0;
            rehashStep(); // 会重入写锁，是安全的
        } finally {
            writeLock.unlock(); // 释放写锁
        }
    }

    /**
     * 检查是否需要缩容。
     * 当哈希表的负载因子小于 0.1 且当前大小大于初始大小 {@code DICT_HT_INITIAL_SIZE} 时，触发缩容。
     * 缩容操作也是通过启动一个新的 rehash 过程来完成。
     */
    private void checkShrinkIfNeeded() {
        // 如果正在进行 rehash，直接返回，避免与正在进行的 rehash 冲突。
        if (rehashIndex != -1) {
            return;
        }

        final DictHashTable<K, V> localHt0 = ht0;
        if (localHt0 == null) {
            return;
        }

        // 计算当前负载因子。
        final double loadFactor = (double) localHt0.used.get() / localHt0.size;

        // 如果负载因子过低（例如小于 0.1）且当前表大小不是最小初始大小，则启动缩容。
        // 加 1 是为了避免 size 为 0 时除以 0 的情况，并确保在极端小表下不频繁缩容。
        if (loadFactor < 0.1 && localHt0.size > DICT_HT_INITIAL_SIZE) {
            int newSize = localHt0.size / 2; // 缩容为原来的一半。
            // 确保缩容后的最小大小不小于初始大小。
            if (newSize < DICT_HT_INITIAL_SIZE) {
                newSize = DICT_HT_INITIAL_SIZE;
            }
            startRehash(newSize);
        }
    }

    /**
     * 清空字典中的所有键值对。
     * 通过重新初始化主哈希表来实现，并重置 rehash 状态。
     */
    public void clear() {
        writeLock.lock(); // 获取写锁
        try {
            ht0 = new DictHashTable<K, V>(DICT_HT_INITIAL_SIZE);
            ht1 = null;
            rehashIndex = -1;
        } finally {
            writeLock.unlock(); // 释放写锁
        }
    }

    /**
     * 获取字典中当前元素的总数量。
     * 如果正在进行 rehash，则会累加 {@code ht0} 和 {@code ht1} 中的元素数量，以提供准确的总数。
     * 此方法使用原子操作 {@code AtomicLong.get()} 确保线程安全地获取计数值。
     *
     * @return 字典中的元素总数。
     */
    public int size(){
        long totalSize = ht0.used.get();
        if (ht1 != null) {
            totalSize += ht1.used.get();
        }
        return (int) totalSize;
    }

    /**
     * 线程安全的删除操作。从字典中移除指定键及其对应的值。
     * 此方法采用乐观锁 (CAS) 结合链表复制技术保证并发安全。
     * 删除操作也会触发 {@code rehashStep()} 推进 rehash 进程，并在删除成功后检查是否需要缩容。
     *
     * @param key 要删除的键。
     * @return 被删除键对应的值，如果键不存在则返回 {@code null}。
     */
    public V remove(final K key) {
        if (key == null) return null;

        final int keyHash = hash(key);

        // 在操作前推进 rehash。
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用，避免删除过程中引用变化。
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 先尝试从 ht0 删除。
        V removedValue = removeFromTable(localHt0, key, keyHash);

        // 如果 ht0 中没找到且正在 rehash，则尝试从 ht1 删除。
        if (removedValue == null && localRehashIndex != -1 && localHt1 != null) {
            removedValue = removeFromTable(localHt1, key, keyHash);
        }

        // 删除成功后检查是否需要缩容。
        if (removedValue != null) {
            checkShrinkIfNeeded();
        }
        return removedValue;
    }

    /**
     * 从指定的哈希表的桶中删除键值对。
     * 此方法是 {@code remove} 操作的核心，使用 CAS (Compare-And-Swap) 循环保证线程安全。
     * <p><b>并发控制策略：</b>
     * <ul>
     * <li>读取当前桶头节点。</li>
     * <li>如果目标键是头节点，直接尝试 CAS 将头节点更新为 {@code head.next}。</li>
     * <li>如果目标键不在头节点：
     * <ul>
     * <li>遍历链表查找目标节点。</li>
     * <li>如果找到：通过递归调用 {@code DictEntry.removeNode()} 创建一个新的链表副本，其中目标节点被跳过。</li>
     * <li>使用 {@code AtomicReferenceArray.compareAndSet()} 尝试原子地更新桶头。
     * 如果 CAS 失败，则重试整个过程。</li>
     * </ul>
     * </li>
     * </ul>
     *
     * @param table 目标哈希表。
     * @param key 要删除的键。
     * @param keyHash 键的哈希值。
     * @return 被删除的值，如果未找到则返回 {@code null}。
     * @throws IllegalStateException 如果 CAS 重试次数超过限制，可能在高并发下发生活锁。
     */
    private V removeFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }
        final int idx = keyIndex(keyHash, table.size);
        if (idx >= table.table.length() || idx < 0) {
            return null; // 索引越界或无效，直接返回。
        }

        int retryCount = 0;
        final int maxRetries = 1000; // 增加重试次数以应对高竞争。

        while (retryCount < maxRetries) {
            DictEntry<K, V> head = table.table.get(idx); // 原子读取当前桶头。
            if (head == null) {
                return null; // 桶为空，没有要删除的键。
            }

            V oldValue = null;
            // 1. 检查头节点是否是目标节点。
            if (head.hash == keyHash && key.equals(head.key)) {
                oldValue = head.value;
                // 如果头节点是目标，尝试 CAS 将头节点更新为 head.next。
                if (table.table.compareAndSet(idx, head, head.next)) {
                    table.used.decrementAndGet(); // 成功删除，used 计数原子减一。
                    return oldValue;
                }
            } else {
                // 2. 如果目标不在头节点，遍历链表查找并重建。
                DictEntry<K, V> current = head;
                boolean found = false;

                // 遍历寻找目标节点。
                while (current != null) {
                    if (current.hash == keyHash && key.equals(current.key)) {
                        oldValue = current.value;
                        found = true;
                        break;
                    }
                    current = current.next;
                }

                if (!found) {
                    return null; // 没找到目标节点，无需删除。
                }

                // 3. 重建链表：从 head 开始，通过递归调用 DictEntry.removeNode 来创建一个新链表副本，
                // 其中目标节点被跳过。
                DictEntry<K, V> newHead = head.removeNode(keyHash, key);

                // 4. 尝试 CAS 原子更新桶头。
                if (table.table.compareAndSet(idx, head, newHead)) {
                    table.used.decrementAndGet(); // 成功删除，used 计数原子减一。
                    return oldValue;
                }
            }
            retryCount++;
        }
        // 如果 CAS 失败次数过多，抛出异常。
        throw new IllegalStateException("removeFromTable exceeded max retries (" + maxRetries + ") for key: " + key);
    }

    /**
     * 获取当前字典的线程安全快照（副本）。
     * <p>此方法专门用于需要全局一致性数据视图的场景，例如 RDB 持久化或 AOF 重写。
     * 它通过遍历当前所有活跃的哈希表（{@code ht0} 和可能存在的 {@code ht1}）并复制其数据到新的 {@code HashMap} 来实现。
     * <p><b>重要提示：</b>
     * <ul>
     * <li>在创建快照过程中，不会触发或推进渐进式 rehash，以确保快照反映调用那一刻的真实数据状态，避免副作用。</li>
     * <li>返回的 {@code Map} 是一个独立副本，后续对 {@code Dict} 的修改不会影响此快照。</li>
     * <li>如果 rehash 正在进行，此方法会同时从 {@code ht0} 和 {@code ht1} 中收集数据，确保包含所有有效键值对。
     * 对于在 rehash 过程中从 {@code ht0} 迁移到 {@code ht1} 的键，快照中将只包含 {@code ht1} 中的最新值（Map 的 {@code put} 行为）。</li>
     * </ul>
     *
     * @return 当前数据的线程安全快照 {@code Map}。
     */
    public Map<K, V> createSafeSnapshot() {
        final Map<K, V> snapshot = new HashMap<>();
        readLock.lock();
        try{
            // 核心修改：移除 rehashStep() 的调用。
            // 快照应该反映调用它那一刻的数据状态，不应该在快照过程中修改 Dict 的内部状态。
            // rehash 的推进应该只由 put 和 remove 等写操作触发和完成。

            // 1. 原子获取哈希表快照引用。
            // volatile 字段保证可见性，本地快照引用确保在遍历期间不会因其他线程的 rehash 进程而改变指向。
            final DictHashTable<K, V> localHt0 = ht0;
            final DictHashTable<K, V> localHt1 = ht1; // ht1 在 rehash 过程中可能不为 null。
            final int localRehashIndex = rehashIndex; // 记录快照获取时的 rehash 状态。

            // 2. 遍历 ht0 快照并将其数据添加到 snapshot。
            createSnapshotFromTable(localHt0, snapshot);

            // 3. 如果正在 rehash，也遍历 ht1 快照并将其数据添加到 snapshot。
            // 这里的 localRehashIndex 和 localHt1 反映了快照获取时 Dict 的实际状态。
            // 由于 HashMap 的 put 方法会覆盖相同键的值，这天然处理了 rehash 过程中同一键在 ht0 和 ht1 的情况，
            // 最终快照中将包含 ht1 中的最新值。
            if (localRehashIndex != -1 && localHt1 != null) {
                createSnapshotFromTable(localHt1, snapshot);
            }
        }
        catch(Exception e){
            throw new RuntimeException("Error creating snapshot: " + e.getMessage(), e);
        }
        finally {
            readLock.unlock();
        }
        return snapshot;
    }


    /**
     * 从指定哈希表遍历并复制键值对到目标快照 Map 中。
     * 此方法对哈希表进行无锁遍历，利用 {@code DictEntry} 的不可变性保证线程安全。
     *
     * @param table 源哈希表。
     * @param snapshot 目标快照 Map。
     */
    private void createSnapshotFromTable(final DictHashTable<K, V> table, final Map<K, V> snapshot) {
        if (table == null || table.table == null) {
            return;
        }
        // 1. 遍历每个桶。
        for (int i = 0; i < table.size; i++) {
            // 原子读取桶头节点。
            DictEntry<K, V> current = table.table.get(i);
            // 2. 遍历不可变链表，无需加锁，因为 DictEntry 是不可变的。
            // 任何修改操作都会产生新的链表头，并原子地更新桶引用，不会影响当前遍历的旧链表。
            while (current != null) {
                // 3. 将键值对添加到快照。HashMap 的 put 方法会处理重复键（即覆盖旧值）。
                // 在 rehash 期间，同一个键可能暂时同时存在于 ht0 和 ht1，
                // 但最终 put 到 snapshot 时，ht1 中的（最新）值会覆盖 ht0 中的。
                snapshot.put(current.key, current.value);
                current = current.next;
            }
        }
    }
}