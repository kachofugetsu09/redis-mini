package site.hnfy258.internal;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
public class Dict<K,V> {
    private static final int INITIAL_SIZE = 4;
    private static final double LOAD_FACTOR = 0.75;
    private static final int REHASH_MAX_SIZE = 5;
    private static final int MAX_EMPTY_PERCENT = 10;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    private final AtomicReference<DictState<K, V>> currentSnapshotState;
    
    private DictHashTable<K, V> ht0;
    private DictHashTable<K, V> ht1;
    private int rehashIndex;
    static class DictEntry<K, V> {
        K key;
        V value;
        DictEntry<K, V> next;
        DictEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    static class DictState<K, V> {
        final DictHashTable<K, V> ht0;
        final DictHashTable<K, V> ht1;
        final int rehashIndex;

        DictState(DictHashTable<K, V> ht0, DictHashTable<K, V> ht1, int rehashIndex) {
            this.ht0 = ht0;
            this.ht1 = ht1;
            this.rehashIndex = rehashIndex;
        }
    }
    static class DictHashTable<K, V> {
        DictEntry<K, V>[] table;
        int size;
        int mask;
        int used;
        @SuppressWarnings("unchecked")
        DictHashTable(int size) {
            this.table = (DictEntry<K, V>[]) new DictEntry[size];
            this.size = size;
            this.mask = size - 1;
            this.used = 0;
        }
    }
    public Dict() {
        ht0 = new DictHashTable<>(INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
        this.currentSnapshotState = new AtomicReference<>(new DictState<>(ht0, ht1, rehashIndex));
    }
    private int hash(Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }
    private int keyIndex(Object key, int size) {
        return hash(key) & (size - 1);
    }
    private DictEntry<K, V> find(K key) {
        if (key == null) return null;
        
        // 在 ht0 中查找
        int idx = ht0.mask & key.hashCode();
        DictEntry<K, V> entry = ht0.table[idx];
        while (entry != null) {
            if (Objects.equals(key, entry.key)) {
                return entry;
            }
            entry = entry.next;
        }

        // 如果正在rehash，在ht1中查找
        if (rehashIndex != -1 && ht1 != null) {
            idx = ht1.mask & key.hashCode();
            entry = ht1.table[idx];
            while (entry != null) {
                if (Objects.equals(key, entry.key)) {
                    return entry;
                }
                entry = entry.next;
            }
        }
        return null;
    }
    public V put(K key, V value) {
        writeLock.lock();
        try {
            if (key == null) throw new IllegalArgumentException("key can not be null");

            // 1. 检查是否需要 rehash
            if (rehashIndex == -1) {
                double loadFactor = (double) ht0.used / ht0.size;
                if (loadFactor > LOAD_FACTOR) {
                    startRehash(ht0.size);
                }
            }

            // 2. 执行渐进式 rehash
            if (rehashIndex != -1) {
                rehashStep();
            }

            // 3. 选择要插入的哈希表
            DictHashTable<K, V> ht = (rehashIndex != -1 && ht1 != null) ? ht1 : ht0;
            
            // 4. 查找是否已存在
            DictEntry<K, V> entry = find(key);
            if (entry != null) {
                V oldValue = entry.value;
                entry.value = value;
                updateSnapshotState();
                return oldValue;
            }

            // 5. 创建新节点并插入
            int idx = ht.mask & key.hashCode();
            entry = new DictEntry<>(key, value);
            entry.next = ht.table[idx];
            ht.table[idx] = entry;
            ht.used++;
            
            updateSnapshotState();
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    private void updateSnapshotState() {
        // 创建新的 DictState 实例
        DictState<K, V> newState = new DictState<>(ht0, ht1, rehashIndex);
        // 使用原子操作更新当前状态
        currentSnapshotState.set(newState);
    }

    public V get(K key) {
        readLock.lock();
        try {
            DictEntry<K, V> entry = find(key);
            return entry != null ? entry.value : null;
        } finally {
            readLock.unlock();
        }
    }
    public V remove(K key) {
        writeLock.lock();
        try {
            if (key == null) return null;
            if (rehashIndex != -1 && ht1 != null) {
                rehashStep();
            }

            // 在ht0中查找并删除
            int idx = keyIndex(key, ht0.size);
            DictEntry<K, V> entry = ht0.table[idx];
            DictEntry<K, V> prev = null;
            while (entry != null) {
                if (key.equals(entry.key)) {
                    if (prev == null) {
                        ht0.table[idx] = entry.next;
                    } else {
                        prev.next = entry.next;
                    }
                    ht0.used--;
                    updateSnapshotState();  // 在ht0中删除后更新快照状态
                    return entry.value;
                }
                prev = entry;
                entry = entry.next;
            }

            // 如果正在rehash，在ht1中查找并删除
            if (rehashIndex != -1 && ht1 != null) {
                idx = keyIndex(key, ht1.size);
                entry = ht1.table[idx];
                prev = null;
                while (entry != null) {
                    if (key.equals(entry.key)) {
                        if (prev == null) {
                            ht1.table[idx] = entry.next;
                        } else {
                            prev.next = entry.next;
                        }
                        ht1.used--;
                        updateSnapshotState();  // 在ht1中删除后更新快照状态
                        return entry.value;
                    }
                    prev = entry;
                    entry = entry.next;
                }
            }

            return null;
        } finally {
            writeLock.unlock();
        }
    }
    private void startRehash(int size) {
        if (size <= 0) return;
        ht1 = new DictHashTable<>(size * 2);
        rehashIndex = 0;
        updateSnapshotState();
    }
    private void rehashStep() {
        if (rehashIndex == -1 || ht1 == null) {
            return;
        }

        boolean stateChanged = false;  // 跟踪是否发生了状态改变

        // 计算允许扫描的最大空桶数（总桶数的10%）
        int maxEmptyBuckets = (int) (ht0.size * (MAX_EMPTY_PERCENT / 100.0));
        int emptyBucketsVisited = 0;
        int entriesMoved = 0;  // 记录实际迁移的键值对数量

        // 当未超过最大空桶数且未达到最大迁移键值对数时继续扫描
        while (emptyBucketsVisited < maxEmptyBuckets && 
               entriesMoved < REHASH_MAX_SIZE && 
               rehashIndex < ht0.size) {
            
            // 跳过空桶
            if (ht0.table[rehashIndex] == null) {
                rehashIndex++;
                emptyBucketsVisited++;
                continue;
            }

            // 处理非空桶中的所有entry
            DictEntry<K, V> entry = ht0.table[rehashIndex];
            DictEntry<K, V> lastMoved = null;  // 跟踪最后一个被迁移的entry
            
            while (entry != null && entriesMoved < REHASH_MAX_SIZE) {
                DictEntry<K, V> next = entry.next;

                // 计算在ht1中的新位置
                int idx = ht1.mask & entry.key.hashCode();
                
                // 将entry迁移到ht1
                entry.next = ht1.table[idx];
                ht1.table[idx] = entry;
                ht1.used++;
                ht0.used--;
                entriesMoved++;
                stateChanged = true;
                
                lastMoved = entry;
                entry = next;
            }

            // 如果当前桶的所有entry都已迁移完成
            if (entry == null) {
                ht0.table[rehashIndex] = null;
                rehashIndex++;
            } else {
                // 如果当前桶还有entry未迁移（因为达到了REHASH_MAX_SIZE限制）
                ht0.table[rehashIndex] = entry;
                stateChanged = true;  // 确保部分迁移也触发状态更新
            }
        }

        // 检查是否完成了所有rehash工作
        if (ht0.used == 0) {
            ht0 = ht1;
            ht1 = null;
            rehashIndex = -1;
            stateChanged = true;
        }

        // 如果状态发生了任何改变，更新快照
        if (stateChanged) {
            updateSnapshotState();
        }
    }
    public Set<K> keySet() {
        readLock.lock();
        try {
            Set<K> keys = new HashSet<>();
            for (int i = 0; i < ht0.size; i++) {
                DictEntry<K, V> entry = ht0.table[i];
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
            if (rehashIndex != -1 && ht1 != null) {
                for (int i = 0; i < ht1.size; i++) {
                    DictEntry<K, V> entry = ht1.table[i];
                    while (entry != null) {
                        keys.add(entry.key);
                        entry = entry.next;
                    }
                }
            }
            return keys;
        } finally {
            readLock.unlock();
        }
    }
    public Map<K, V> getAll() {
        readLock.lock();
        try {
            Map<K, V> map = new HashMap<>();
            for (int i = 0; i < ht0.size; i++) {
                DictEntry<K, V> entry = ht0.table[i];
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
            if (rehashIndex != -1 && ht1 != null) {
                for (int i = 0; i < ht1.size; i++) {
                    DictEntry<K, V> entry = ht1.table[i];
                    while (entry != null) {
                        map.put(entry.key, entry.value);
                        entry = entry.next;
                    }
                }
            }
            return map;
        } finally {
            readLock.unlock();
        }
    }
    public void clear() {
        writeLock.lock();
        try {
            ht0 = new DictHashTable<>(INITIAL_SIZE);
            ht1 = null;
            rehashIndex = -1;
            updateSnapshotState();
        } finally {
            writeLock.unlock();
        }
    }
    public int size() {
        readLock.lock();
        try {
            return ht0.used + (ht1 != null ? ht1.used : 0);
        } finally {
            readLock.unlock();
        }
    }
    public boolean containsKey(K key) {
        readLock.lock();
        try {
            if (key == null) return false;
            return find(key) != null;
        } finally {
            readLock.unlock();
        }
    }
    public boolean contains(K key, V value) {
        readLock.lock();
        try {
            if (key == null) return false;
            DictEntry<K, V> entry = find(key);
            while (entry != null) {
                if (entry.value.equals(value)) {
                    return true;
                }
                entry = entry.next;
            }
            return false;
        } finally {
            readLock.unlock();
        }
    }
    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        readLock.lock();
        try {
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < ht0.size; i++) {
                DictEntry<K, V> entry = ht0.table[i];
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
            
            if (rehashIndex != -1 && ht1 != null) {
                for (int i = 0; i < ht1.size; i++) {
                    DictEntry<K, V> entry = ht1.table[i];
                    while (entry != null) {
                        map.put(entry.key, entry.value);
                        entry = entry.next;
                    }
                }
            }
            
            return map.entrySet();
        } finally {
            readLock.unlock();
        }
    }
    public DictSnapshot<K, V> createSnapshot() {
        DictState<K, V> stateToSnapshot;
        readLock.lock(); // 获取读锁，确保在获取 DictState 时数据一致
        try {
            // 这一步是 O(1) 原子读取操作，在读锁保护下获取到最新的 DictState
            stateToSnapshot = currentSnapshotState.get();
        } finally {
            readLock.unlock(); // 释放读锁
        }

        // 接下来的深拷贝在后台线程（或调用线程的剩余部分）独立进行，不影响主线程
        DictHashTable<K, V> ht0Copy = null;
        DictHashTable<K, V> ht1Copy = null;

        if (stateToSnapshot.ht0 != null) {
            ht0Copy = deepCopyHashTable(stateToSnapshot.ht0);
        }
        if (stateToSnapshot.rehashIndex != -1 && stateToSnapshot.ht1 != null) {
            ht1Copy = deepCopyHashTable(stateToSnapshot.ht1);
        }
        return new DictSnapshot<>(ht0Copy, ht1Copy, stateToSnapshot.rehashIndex);
    }
    public static class DictSnapshot<K, V> implements Iterable<Map.Entry<K, V>> {
        private final DictHashTable<K, V> ht0Snapshot;
        private final DictHashTable<K, V> ht1Snapshot;
        private final int rehashIndexSnapshot;
        
        DictSnapshot(DictHashTable<K, V> ht0, DictHashTable<K, V> ht1, int rehashIndex) {
            this.ht0Snapshot = ht0;
            this.ht1Snapshot = ht1;
            this.rehashIndexSnapshot = rehashIndex;
        }
        
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new DictSnapshotIterator();
        }
        
        private class DictSnapshotIterator implements Iterator<Map.Entry<K, V>> {
            private int currentTable = 0; // 0 for ht0, 1 for ht1
            private int currentIndex = 0;
            private DictEntry<K, V> currentEntry = null;
            private DictEntry<K, V> nextEntry = null;
            private boolean hasNextCached = false;
            @Override
            public boolean hasNext() {
                if (hasNextCached) {
                    return nextEntry != null;
                }
                findNext();
                hasNextCached = true;
                return nextEntry != null;
            }
            @Override
            public Map.Entry<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                currentEntry = nextEntry;
                hasNextCached = false;
                return new AbstractMap.SimpleImmutableEntry<>(currentEntry.key, currentEntry.value);
            }
            private void findNext() {
// 如果当前entry有下一个节点
                if (currentEntry != null && currentEntry.next != null) {
                    nextEntry = currentEntry.next;
                    return;
                }
// 在当前表中寻找下一个非空桶
                DictHashTable<K, V> currentHt = (currentTable == 0) ? ht0Snapshot : ht1Snapshot;
                if (currentHt != null && currentHt.table != null) {
                    for (int i = currentIndex + 1; i < currentHt.size; i++) {
                        if (currentHt.table[i] != null) {
                            currentIndex = i;
                            nextEntry = currentHt.table[i];
                            return;
                        }
                    }
                }
// 当前表遍历完了，切换到下一个表
                if (currentTable == 0 && ht1Snapshot != null) {
                    currentTable = 1;
                    currentIndex = -1;
                    findNext();
                    return;
                }
// 所有表都遍历完了
                nextEntry = null;
            }
        }
        // 提供一些便利方法
        public Stream<Map.Entry<K, V>> stream() {
            return StreamSupport.stream(spliterator(), false);
        }
        public Stream<K> keyStream() {
            return stream().map(Map.Entry::getKey);
        }
        public Stream<V> valueStream() {
            return stream().map(Map.Entry::getValue);
        }
        // 如果需要转换为Map（这是O(n)操作，由调用者决定是否执行）
        public Map<K, V> toMap() {
            Map<K, V> result = new HashMap<>();
            for (Map.Entry<K, V> entry : this) {
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
    }

    private DictHashTable<K, V> deepCopyHashTable(DictHashTable<K, V> frozen) {
        DictHashTable<K, V> copy = new DictHashTable<>(frozen.size);
        for (int i = 0; i < frozen.size; i++) {
            DictEntry<K, V> entry = frozen.table[i];
            // 遍历链表并复制每个 DictEntry
            DictEntry<K, V> currentCopiedEntry = null;
            DictEntry<K, V> headOfCopiedList = null;

            while (entry != null) {
                DictEntry<K, V> newEntry = new DictEntry<>(entry.key, entry.value);
                if (headOfCopiedList == null) {
                    headOfCopiedList = newEntry;
                    currentCopiedEntry = newEntry;
                } else {
                    currentCopiedEntry.next = newEntry;
                    currentCopiedEntry = newEntry;
                }
                copy.used++;
                entry = entry.next;
            }
            // 将复制好的链表头放到新表的对应索引位置
            copy.table[i] = headOfCopiedList;
        }
        return copy;
    }

}