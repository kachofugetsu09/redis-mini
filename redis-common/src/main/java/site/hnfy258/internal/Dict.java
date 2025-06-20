package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 基于Redis源码设计的线程安全Dict实现
 *
 * <p>主要特性：
 * <ul>
 *   <li>CAS + 头节点锁：空桶使用CAS原子插入，非空桶使用头节点synchronized</li>
 *   <li>渐进式rehash：参考Redis源码，支持2的幂次扩容和负载因子控制</li>
 *   <li>快照一致性：提供createSafeSnapshot()方法，保证RDB/AOF持久化时的数据一致性</li>
 * </ul>
 *
 * <p>并发安全性：
 * <ul>
 *   <li>读操作：无锁读取，使用本地引用快照避免rehash竞态条件</li>
 *   <li>写操作：空桶使用CAS，非空桶使用头节点synchronized</li>
 *   <li>rehash安全：volatile字段 + 本地引用快照，防止rehash期间引用变化</li>
 * </ul>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class Dict<K,V> {
    
    // 常量定义
    static final int DICT_HT_INITIAL_SIZE = 4;
    private static final int DICT_REHASH_BUCKETS_PER_STEP = 100;
    private static final int DICT_REHASH_MAX_EMPTY_VISITS = 10;
    
    // 哈希表和rehash状态
    volatile DictHashTable<K,V> ht0;
    volatile DictHashTable<K,V> ht1;
    volatile int rehashIndex;

    public boolean containsKey(K key) {
        if(key == null) return false;
        if(rehashIndex != -1) rehashStep();
        return find(key) != null;
    }

    public boolean contains(K score, V member) {
        if(score == null) return false;
        if(rehashIndex != -1) rehashStep();
        DictEntry<K, V> entry = find(score);
        while (entry != null) {
            if(entry.value.equals(member)){
                return true;
            }
            entry = entry.next;
        }        return false;
    }

    /**
     * 线程安全的entrySet方法 - 用于持久化操作
     * 建议使用createSafeSnapshot()方法替代
     * 
     * @return 当前数据的entrySet
     */
    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        // 使用安全快照避免并发问题
        final Map<K, V> snapshot = createSafeSnapshot();
        final Map<Object, Object> result = new HashMap<>();
        for (final Map.Entry<K, V> entry : snapshot.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }        return result.entrySet();
    }

    /**
     * 线程安全的Dict节点
     */
    static class DictEntry<K,V> {
        final K key;
        volatile V value;
        volatile DictEntry<K,V> next;
        final int hash;  // 缓存hash值，避免重复计算

        DictEntry(final K key, final V value, final int hash) {
            this.key = key;
            this.value = value;
            this.hash = hash;
        }

        DictEntry(final K key, final V value, final int hash, final DictEntry<K,V> next) {
            this.key = key;
            this.value = value;
            this.hash = hash;
            this.next = next;
        }
    }    /**
     * 线程安全的哈希表
     */
    static class DictHashTable<K,V> {
        final AtomicReferenceArray<DictEntry<K,V>> table;
        final int size;
        final int sizemask;
        final AtomicLong used;  // 使用AtomicLong保证size的原子性

        DictHashTable(final int size) {
            // 确保size是2的幂次，否则sizemask计算会出错
            if (size <= 0 || (size & (size - 1)) != 0) {
                throw new IllegalArgumentException("Hash table size must be a power of 2, got: " + size);
            }
            
            this.table = new AtomicReferenceArray<>(size);
            this.size = size;
            this.sizemask = size - 1;            this.used = new AtomicLong(0);
        }
    }

    public Dict() {
        ht0 = new DictHashTable<K, V>(DICT_HT_INITIAL_SIZE);        ht1 = null;
        rehashIndex = -1;
    }

    /**
     * 使用高质量哈希函数计算hash值
     * 
     * @param key 要计算哈希值的键
     * @return 32位哈希值（保证非负）
     */
    private int hash(final Object key) {
        if (key == null) return 0;
        
        final int h = key.hashCode();
        // 1. 使用Wang's hash算法提升分布性
        int hash = h;
        hash = (hash ^ 61) ^ (hash >>> 16);
        hash = hash + (hash << 3);
        hash = hash ^ (hash >>> 4);
        hash = hash * 0x27d4eb2d;
        hash = hash ^ (hash >>> 15);
        
        // 2. 确保返回非负值
        return hash & 0x7FFFFFFF;
    }

    /**
     * 使用位运算计算索引
     * 
     * @param hashValue 已计算的hash值
     * @param size 哈希表大小（必须是2的幂次）
     * @return 桶索引（0到size-1）
     */
    private int keyIndex(final int hashValue, final int size) {
        // 使用位运算优化，利用size是2的幂次的特性
        return hashValue & (size - 1);
    }

    /**
     * 线程安全的查找方法
     * 
     * @param key 要查找的键
     * @return 找到的DictEntry或null
     */
    private DictEntry<K, V> find(final K key) {
        if (key == null) return null;

        // 计算hash值，避免重复计算
        final int keyHash = hash(key);

        // 获取当前哈希表的本地快照引用，防止rehash期间引用变化
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 优先在主表ht0中查找
        if (localHt0 != null && localHt0.table != null) {
            final int idx0 = keyIndex(keyHash, localHt0.size);
            if (idx0 >= 0 && idx0 < localHt0.size) {
                DictEntry<K, V> entry = localHt0.table.get(idx0);
                while (entry != null) {
                    // 先比较hash值，再比较key
                    if (entry.hash == keyHash && key.equals(entry.key)) {
                        return entry;
                    }
                    entry = entry.next;
                }
            }
        }

        // 如果正在rehash且在ht0中未找到，则在ht1中查找
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
        }        return null;
    }

    /**
     * 线程安全的put操作
     * 
     * @param key 要插入的键
     * @param value 要插入的值  
     * @return 旧值或null
     */
    public V put(final K key, final V value) {
        if (key == null) throw new IllegalArgumentException("key can not be null");
        
        // 计算hash值
        final int keyHash = hash(key);
        
        // 检查是否需要扩容
        if (rehashIndex == -1 && ht0 != null) {
            final long currentUsed = ht0.used.get();
            final double loadFactor = (double) (currentUsed + 1) / ht0.size;
            if (loadFactor >= 1.0) {
                startRehash(ht0.size * 2);
            }
        }
        
        // 执行渐进式rehash
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 选择要插入的哈希表（rehash时插入到ht1）
        final DictHashTable<K, V> targetHt = (rehashIndex != -1 && ht1 != null) ? ht1 : ht0;
        if (targetHt == null || targetHt.table == null) {
            throw new IllegalStateException("Hash table not properly initialized");
        }
        
        // 计算桶索引
        final int idx = keyIndex(keyHash, targetHt.size);
        if (idx >= targetHt.table.length() || idx < 0) {
            throw new IllegalStateException("Calculated index " + idx + " is out of bounds for table size " + targetHt.table.length());
        }
        
        // 线程安全的插入操作
        V result = putInBucket(targetHt, idx, key, value, keyHash);
        
        // 插入完成后检查是否需要缩容
        if (rehashIndex == -1) {
            checkShrinkIfNeeded();
        }
          return result;
    }

    /**
     * 在指定桶中插入键值对 - 使用CAS + synchronized保证线程安全
     * 仿照ConcurrentHashMap的putVal方法
     * 
     * @param table 目标哈希表
     * @param idx 桶索引
     * @param key 键
     * @param value 值
     * @param keyHash 键的hash值
     * @return 旧值或null
     */
    private V putInBucket(final DictHashTable<K, V> table, final int idx, 
                         final K key, final V value, final int keyHash) {
        int retryCount = 0;
        final int maxRetries = 1000; // 设置最大重试次数防止无限循环
        
        while (retryCount < maxRetries) {  // 使用循环重试保证操作成功
            DictEntry<K, V> head = table.table.get(idx);
            
            // 1. 如果桶为空，使用CAS原子插入
            if (head == null) {
                final DictEntry<K, V> newEntry = new DictEntry<>(key, value, keyHash);
                // CAS原子操作，确保只有一个线程能成功插入
                if (table.table.compareAndSet(idx, null, newEntry)) {
                    // 使用do-while循环确保计数器增加成功
                    long prev;
                    do {
                        prev = table.used.get();
                    } while (!table.used.compareAndSet(prev, prev + 1));
                    return null;
                }
                // CAS失败，说明有其他线程已插入，重试
                retryCount++;
                continue;
            }
            
            // 2. 桶不为空，使用头节点synchronized
            synchronized (head) {
                // 重新读取头节点，确保一致性
                DictEntry<K, V> current = table.table.get(idx);
                
                // 检查头节点是否变化，如果变化则重试
                if (current != head) {
                    retryCount++;
                    continue;
                }
                
                // 3. 遍历链表查找相同key
                while (current != null) {
                    if (current.hash == keyHash && key.equals(current.key)) {
                        // 找到相同key，更新value
                        final V oldValue = current.value;
                        current.value = value;  // volatile写
                        return oldValue;
                    }
                    current = current.next;
                }
                
                // 4. 没找到相同key，在头部插入新节点
                final DictEntry<K, V> newEntry = new DictEntry<>(key, value, keyHash, head);
                table.table.set(idx, newEntry);  // 原子写入
                // 使用do-while循环确保计数器增加成功
                long prev;
                do {
                    prev = table.used.get();
                } while (!table.used.compareAndSet(prev, prev + 1));
                return null;
            }
        }
        
        // 如果重试次数超过限制，抛出异常
        throw new IllegalStateException("putInBucket exceeded max retries (" + maxRetries + ") for key: " + key);
    }

    /**
     * 线程安全的get操作 - 仿照ConcurrentHashMap的无锁读取
     * 利用volatile保证内存可见性，使用本地引用快照避免rehash期间的竞态条件
     * 
     * @param key 要查找的键
     * @return 对应的值或null
     */
    public V get(final K key) {
        if (key == null) return null;
        
        // 1. 计算hash值
        final int keyHash = hash(key);
        
        // 2. 在操作前推进rehash
        if (rehashIndex != -1) {
            rehashStep();
        }
        
        // 3. 获取哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;
        
        // 4. 无锁读取 - 先查ht0，再查ht1
        V result = getFromTable(localHt0, key, keyHash);
        if (result == null && localRehashIndex != -1 && localHt1 != null) {
            result = getFromTable(localHt1, key, keyHash);
        }
          return result;
    }

    /**
     * 从指定哈希表中查找值 - 无锁读取
     * 
     * @param table 目标哈希表
     * @param key 键
     * @param keyHash 键的hash值
     * @return 对应的值或null
     */
    private V getFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }        final int idx = keyHash & (table.size - 1);
        
        // 添加边界检查防止数组越界
        if (idx >= table.table.length() || idx < 0) {
            throw new IllegalStateException("Calculated index " + idx + " is out of bounds for table size " + table.table.length());
        }
        
        // 原子读取头节点
        DictEntry<K, V> current = table.table.get(idx);
        
        // 遍历链表 - 无需同步，利用volatile保证可见性
        while (current != null) {
            if (current.hash == keyHash && key.equals(current.key)) {
                return current.value;  // volatile读
            }
            current = current.next;  // volatile读
        }
          return null;
    }

    /**
     * 线程安全的keySet方法 - 使用本地引用快照避免rehash竞态
     * 
     * @return 当前所有键的集合
     */
    public Set<K> keySet() {
        final Set<K> keys = new HashSet<>();
        
        // 推进rehash
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

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
        
        // 如果正在rehash，也遍历ht1
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
     * 获取所有键值对 - 使用本地引用快照保证弱一致性
     * 注意：此方法返回的是弱一致性快照，适用于迭代和批量操作
     * 
     * @return 当前所有键值对的Map
     */
    public Map<K, V> getAll() {
        final Map<K, V> map = new HashMap<>();

        // 推进rehash
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 获取哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

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

        // 如果正在rehash，也遍历ht1
        if (localRehashIndex != -1 && localHt1 != null && localHt1.table != null) {
            for (int i = 0; i < localHt1.size; i++) {
                DictEntry<K, V> entry = localHt1.table.get(i);
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }        return map;
    }

    /**
     * 执行一步渐进式rehash
     * 
     * <p>每次执行rehash时，会将一定数量的桶从ht0迁移到ht1。
     * 当所有桶都迁移完成后，将ht1设置为ht0，并清空ht1。
     */
    synchronized void rehashStep() {
        // 1. 如果没有在进行rehash，直接返回
        if (rehashIndex == -1) {
            return;
        }

        // 2. 获取当前哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        if (localHt0 == null || localHt1 == null) {
            return;
        }

        // 3. 迁移一定数量的桶
        int emptyVisits = 0;
        int maxEmptyVisits = DICT_REHASH_MAX_EMPTY_VISITS;
        int bucketsToMove = DICT_REHASH_BUCKETS_PER_STEP;
        
        while (bucketsToMove > 0 && rehashIndex < localHt0.size) {
            // 3.1 获取当前桶的引用
            DictEntry<K, V> entry = localHt0.table.get(rehashIndex);
            
            // 3.2 如果桶为空，增加空桶访问计数
            if (entry == null) {
                emptyVisits++;
                if (emptyVisits >= maxEmptyVisits) {
                    break;
                }
                rehashIndex++;
                continue;
            }

            // 3.3 迁移当前桶中的所有节点
            // 使用synchronized保护整个桶的迁移过程
            synchronized (localHt0.table) {
                // 重新检查entry，确保没有被其他线程修改
                entry = localHt0.table.get(rehashIndex);
                if (entry == null) {
                    rehashIndex++;
                    continue;
                }

                // 先计算需要迁移的节点数
                int nodeCount = 0;
                DictEntry<K, V> countNode = entry;
                while (countNode != null) {
                    nodeCount++;
                    countNode = countNode.next;
                }

                // 原子更新计数器
                long prevHt0Used;
                do {
                    prevHt0Used = localHt0.used.get();
                } while (!localHt0.used.compareAndSet(prevHt0Used, prevHt0Used - nodeCount));

                long prevHt1Used;
                do {
                    prevHt1Used = localHt1.used.get();
                } while (!localHt1.used.compareAndSet(prevHt1Used, prevHt1Used + nodeCount));

                while (entry != null) {
                    // 计算在新表中的位置
                    final int idx = keyIndex(entry.hash, localHt1.size);
                    
                    // 将节点插入到新表中
                    final DictEntry<K, V> next = entry.next;
                    entry.next = localHt1.table.get(idx);
                    localHt1.table.set(idx, entry);
                    
                    entry = next;
                }

                // 3.4 清空原桶
                localHt0.table.set(rehashIndex, null);
            }
            
            // 3.5 更新计数器
            bucketsToMove--;
            rehashIndex++;
        }

        // 4. 检查是否完成rehash
        if (rehashIndex >= localHt0.size) {
            // 4.1 将ht1设置为ht0
            ht0 = localHt1;
            
            // 4.2 清空ht1
            ht1 = null;
            
            // 4.3 重置rehash索引
            rehashIndex = -1;
        }
    }

    /**
     * 启动rehash过程
     * 
     * @param targetSize 目标大小（必须是2的幂次）
     */
    private synchronized void startRehash(final int targetSize) {
        // 1. 如果已经在进行rehash，直接返回
        if (rehashIndex != -1) {
            return;
        }

        // 2. 创建新的哈希表
        ht1 = new DictHashTable<>(targetSize);
        
        // 3. 设置rehash索引为0，开始rehash过程
        rehashIndex = 0;
        
        // 4. 执行一步rehash
        rehashStep();
    }

    /**
     * 检查是否需要缩容
     */
    private void checkShrinkIfNeeded() {
        // 1. 如果正在进行rehash，直接返回
        if (rehashIndex != -1) {
            return;
        }

        // 2. 获取当前哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        if (localHt0 == null) {
            return;
        }

        // 3. 计算负载因子
        final double loadFactor = (double) localHt0.used.get() / localHt0.size;
        
        // 4. 如果负载因子小于0.1且表大小大于初始大小，则进行缩容
        if (loadFactor < 0.1 && localHt0.size > DICT_HT_INITIAL_SIZE) {
            int newSize = localHt0.size / 2;
            if (newSize < DICT_HT_INITIAL_SIZE) {
                newSize = DICT_HT_INITIAL_SIZE;
            }
            startRehash(newSize);
        }
    }

    public void clear() {
        ht0 = new DictHashTable<K, V>(DICT_HT_INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
    }

    /**
     * 获取线程安全的元素数量 - 使用原子操作保证一致性
     * 
     * @return 元素总数
     */
    public int size(){
        long totalSize = ht0.used.get();
        if (ht1 != null) {
            totalSize += ht1.used.get();
        }        return (int) totalSize;
    }

    /**
     * 线程安全的删除操作 - 仿照ConcurrentHashMap的remove方法
     * 使用头节点synchronized保证并发安全，采用本地引用快照避免rehash竞态
     * 
     * @param key 要删除的键
     * @return 删除的值，如果不存在则返回null
     */
    public V remove(final K key) {
        if (key == null) return null;
        
        // 1. 计算hash值
        final int keyHash = hash(key);
        
        // 2. 在操作前推进rehash
        if (rehashIndex != -1) {
            rehashStep();
        }

        // 3. 获取哈希表的本地快照引用，避免删除过程中引用变化
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;

        // 4. 先尝试从ht0删除
        V removedValue = removeFromTable(localHt0, key, keyHash);
        
        // 5. 如果ht0中没找到且正在rehash，则从ht1删除
        if (removedValue == null && localRehashIndex != -1 && localHt1 != null) {
            removedValue = removeFromTable(localHt1, key, keyHash);
        }
        
        // 6. 删除成功后检查是否需要缩容
        if (removedValue != null) {
            checkShrinkIfNeeded();
        }
          return removedValue;
    }

    /**
     * 从指定的哈希表中删除键值对 - 使用头节点synchronized保证线程安全
     * 
     * @param table 目标哈希表
     * @param key 要删除的键
     * @param keyHash 键的hash值
     * @return 删除的值，如果不存在则返回null
     */
    private V removeFromTable(final DictHashTable<K, V> table, final K key, final int keyHash) {
        if (table == null || table.table == null) {
            return null;
        }
        final int idx = keyIndex(keyHash, table.size);
        
        // 添加边界检查防止数组越界
        if (idx >= table.table.length() || idx < 0) {
            return null;
        }
        
        DictEntry<K, V> head = table.table.get(idx);
        
        // 1. 如果桶为空，直接返回
        if (head == null) {
            return null;
        }
        
        // 2. 使用头节点synchronized - 仿照ConcurrentHashMap
        synchronized (head) {
            // 重新读取头节点，确保一致性
            DictEntry<K, V> current = table.table.get(idx);
            DictEntry<K, V> prev = null;            while (current != null) {
                if (current.hash == keyHash && key.equals(current.key)) {
                    // 找到了，从链表中移除
                    if (prev == null) {
                        // 删除头节点
                        table.table.set(idx, current.next);  // 原子写入
                    } else {
                        // 删除中间或尾部节点
                        prev.next = current.next;  // volatile写
                    }
                    table.used.decrementAndGet();
                    return current.value;
                }
                prev = current;
                current = current.next;
            }
        }
          return null;
    }

    /**
     * 获取线程安全的快照 - 专门用于RDB保存和AOF重写
     * 返回当前时刻的数据副本，避免并发修改问题
     * 使用本地引用快照保证一致性
     * 
     * @return 当前数据的线程安全快照
     */
    public Map<K, V> createSafeSnapshot() {
        final Map<K, V> snapshot = new HashMap<>();
        
        // 1. 推进rehash
        if (rehashIndex != -1) {
            rehashStep();
        }
        
        // 2. 获取哈希表的本地快照引用
        final DictHashTable<K, V> localHt0 = ht0;
        final DictHashTable<K, V> localHt1 = ht1;
        final int localRehashIndex = rehashIndex;
        
        // 3. 创建ht0的快照
        createSnapshotFromTable(localHt0, snapshot);
        
        // 4. 如果正在rehash，也创建ht1的快照
        if (localRehashIndex != -1 && localHt1 != null) {
            createSnapshotFromTable(localHt1, snapshot);
        }
          return snapshot;
    }

    /**
     * 从指定哈希表创建快照 - 使用synchronized保证数据一致性
     * 
     * @param table 源哈希表
     * @param snapshot 目标快照Map
     */
    private void createSnapshotFromTable(final DictHashTable<K, V> table, final Map<K, V> snapshot) {
        if (table == null || table.table == null) {
            return;
        }
        
        // 按桶遍历，每个桶使用头节点synchronized
        for (int i = 0; i < table.size; i++) {
            DictEntry<K, V> head = table.table.get(i);
            if (head == null) {
                continue;
            }
            
            // 使用头节点synchronized保证链表遍历的一致性
            synchronized (head) {
                DictEntry<K, V> current = table.table.get(i);  // 重新读取
                while (current != null) {
                    // 避免重复key（rehash期间可能存在）
                    if (!snapshot.containsKey(current.key)) {
                        snapshot.put(current.key, current.value);
                    }
                    current = current.next;
                }
            }
        }
    }
}
