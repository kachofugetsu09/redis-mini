package site.hnfy258.internal;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;

public class Dict<K,V> {

    private static final int INITIAL_SIZE = 4;
    private static final double LOAD_FACTOR = 0.75;
    private static final int REHASH_MAX_SIZE =5;
    private static final int MAX_EMPTY_PERCENT = 10;

    private AtomicBoolean isSnapshotting = new AtomicBoolean(false);
    
    // 记录快照期间被修改的key
    private Set<K> modifiedKeys = new HashSet<>();

    static enum ForwardType {
        REMOVE,INSERT,UPDATE
    }


    private DictHashTable<K,V> ht0;
    private DictHashTable<K,V> ht1;
    private int rehashIndex;

    static class ForwardNode{
        final AtomicReference<Object> oldValue;  // 快照时的旧值
        final AtomicReference<Object> newValue;  // 等待应用的新值
        final AtomicReference<ForwardType> operation; // 操作类型
        
        ForwardNode(Object oldValue, Object newValue, ForwardType operation){
            this.oldValue = new AtomicReference<>(oldValue);
            this.newValue = new AtomicReference<>(newValue);
            this.operation = new AtomicReference<>(operation);
        }

        private static final Object REMOVED_SIGNAL = new Object();
        
        // 获取当前应该返回的值（快照期间返回旧值，否则返回新值）
        public Object getCurrentValue(boolean isSnapshotting) {
            if (isSnapshotting) {
                Object old = oldValue.get();
                return old == REMOVED_SIGNAL ? null : old;
            } else {
                Object newVal = newValue.get();
                return newVal == REMOVED_SIGNAL ? null : newVal;
            }
        }
        
        // 应用新值到旧值位置（快照完成后调用）
        public void applyNewValue() {
            oldValue.set(newValue.get());
        }
    }

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
            Object value = entry.value;
            if(value instanceof ForwardNode) {
                ForwardNode forwardNode = (ForwardNode) value;
                value = forwardNode.getCurrentValue(false);
            }
            if(value != null && value.equals(member)){
                return true;
            }
            entry = entry.next;
        }
        return false;
    }

    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        if(rehashIndex != -1) rehashStep();
        Map<Object, Object> map = new HashMap<>();
        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                Object value = entry.value;
                if(value instanceof ForwardNode) {
                    ForwardNode forwardNode = (ForwardNode) value;
                    value = forwardNode.getCurrentValue(false);
                }
                if(value != null) {
                    map.put(entry.key, value);
                }
                entry = entry.next;
            }
        }
        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    Object value = entry.value;
                    if(value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) value;
                        value = forwardNode.getCurrentValue(false);
                    }
                    if(value != null) {
                        map.put(entry.key, value);
                    }
                    entry = entry.next;
                }
            }
        }
        return map.entrySet();
    }

    static class DictEntry<K,V>{
        K key;
        volatile Object value;
        DictEntry<K,V> next;

        DictEntry(K key, Object value){
            this.key = key;
            this.value = value;
        }
    }

    static class DictHashTable<K,V>{
        DictEntry<K,V>[] table;
        int size;
        int sizemask;
        int used;

        @SuppressWarnings("unchecked")
        DictHashTable(int size)
        {
            this.table = (DictEntry<K, V>[]) new DictEntry[size];
            this.size = size;
            this.sizemask = size - 1;
            this.used = 0;
        }
    }

    public Dict(){
        ht0 = new DictHashTable<>(INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
    }

    private int hash(Object key){
        if(key == null) return 0;
        int h = key.hashCode();
        return h^(h>>>16);
    }

    private int keyIndex(Object key, int size){
        return hash(key) & (size-1);
    }

    private DictEntry<K,V> find(K key){
        if(key == null) return null;

        if(rehashIndex != -1) rehashStep();

        int idx = keyIndex(key, ht0.size);
        DictEntry<K,V> entry = ht0.table[idx];
        while(entry != null){
            if(entry.key.equals(key)){
                return entry;
            }
            entry = entry.next;
        }

        if(rehashIndex != -1 || ht1 != null){
            idx = keyIndex(key, ht1.size);
            entry = ht1.table[idx];
            while(entry != null){
                if(entry.key.equals(key)){
                    return entry;
                }
                entry = entry.next;
            }
        }
        return null;
    }

    public Object put(K key, V value){
        if(key == null) throw new IllegalArgumentException("key can not be null");
        
        synchronized (this) {
            //如果不在rehash
            if(rehashIndex == -1){
                double loadFactor = (double) ht0.used / ht0.size;
                if(loadFactor > LOAD_FACTOR){
                    startRehash(ht0.size);
                }
            }
            //如果在rehash
            if(rehashIndex != -1) rehashStep();

            Object oldValue = null;
            DictEntry<K,V> entry = find(key);

            if(entry != null){
                // 如果正在创建快照
                if(isSnapshotting.get()) {
                    // 如果entry.value已经是ForwardNode，更新其新值
                    if(entry.value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) entry.value;
                        oldValue = forwardNode.getCurrentValue(false); // 获取快照时的旧值
                        forwardNode.newValue.set(value); // 更新新值
                        forwardNode.operation.set(ForwardType.UPDATE);
                    } else {
                        // 创建新的ForwardNode
                        oldValue = entry.value;
                        entry.value = new ForwardNode(entry.value, value, ForwardType.UPDATE);
                        modifiedKeys.add(key); // 记录被修改的key
                    }
                } else {
                    // 不在快照期间，直接更新
                    oldValue = entry.value;
                    entry.value = value;
                }
                return oldValue;
            }

            // 新增entry
            int idx;
            if(rehashIndex != -1){
                idx = keyIndex(key, ht1.size);
                DictEntry<K, V> newEntry = new DictEntry<>(key, 
                    isSnapshotting.get() ? new ForwardNode(null, value, ForwardType.INSERT) : value);
                newEntry.next = ht1.table[idx];
                ht1.table[idx] = newEntry;
                ht1.used++;
                if(isSnapshotting.get()) {
                    modifiedKeys.add(key); // 记录新增的key
                }
            }
            else{
                idx = keyIndex(key, ht0.size);
                DictEntry<K, V> newEntry = new DictEntry<>(key, 
                    isSnapshotting.get() ? new ForwardNode(null, value, ForwardType.INSERT) : value);
                newEntry.next = ht0.table[idx];
                ht0.table[idx] = newEntry;
                ht0.used++;
                if(isSnapshotting.get()) {
                    modifiedKeys.add(key); // 记录新增的key
                }
            }

            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public V get(K key){
        DictEntry<K, V> entry = find(key);
        if(entry == null) return null;
        
        if(entry.value instanceof ForwardNode) {
            ForwardNode forwardNode = (ForwardNode) entry.value;
            Object value = forwardNode.getCurrentValue(false);
            return (V) value;
        }
        
        return (V) entry.value;
    }

    @SuppressWarnings("unchecked")
    public V remove(K key){
        if(key ==null) return null;
        if(rehashIndex != -1) rehashStep();

        DictEntry<K,V> entry = find(key);
        if(entry == null) return null;

        Object oldValue = entry.value;
        
        // 如果正在创建快照
        if(isSnapshotting.get()) {
            if(entry.value instanceof ForwardNode) {
                ForwardNode forwardNode = (ForwardNode) entry.value;
                oldValue = forwardNode.getCurrentValue(false);
                forwardNode.newValue.set(ForwardNode.REMOVED_SIGNAL); // 标记为删除
                forwardNode.operation.set(ForwardType.REMOVE);
            } else {
                // 创建新的ForwardNode，标记为删除
                oldValue = entry.value;
                entry.value = new ForwardNode(entry.value, ForwardNode.REMOVED_SIGNAL, ForwardType.REMOVE);
                modifiedKeys.add(key); // 记录被修改的key
            }
        } else {
            // 不在快照期间，直接删除
            removeEntryFromTable(key);
        }
        
        return (V) oldValue;
    }
    
    private void removeEntryFromTable(K key) {
        int idx = keyIndex(key, ht0.size);
        DictEntry<K, V> entry = ht0.table[idx];
        DictEntry<K,V> prev = null;

        while(entry != null){
            if(key.equals(entry.key)){
                if(prev ==null){
                    ht0.table[idx] = entry.next;
                }else{
                    prev.next = entry.next;
                }
                ht0.used--;
                return;
            }
            prev = entry;
            entry = entry.next;
        }

        if(rehashIndex != -1 || ht1 != null){
            idx = keyIndex(key, ht1.size);
            entry = ht1.table[idx];
            prev = null;
            while(entry != null){
                if(key.equals(entry.key)){
                    if(prev ==null){
                        ht1.table[idx] = entry.next;
                    }else{
                        prev.next = entry.next;
                    }
                    ht1.used--;
                    return;
                }
                prev = entry;
                entry = entry.next;
            }
        }
    }

    private void startRehash(int size){
        ht1 = new DictHashTable<>(size*2);
        rehashIndex =0;
    }

    private void rehashStep(){
        if(rehashIndex ==-1 || ht1 ==null) return;

        int emptyVisited = 0;
        int processed =0;

        while(emptyVisited < MAX_EMPTY_PERCENT
                && processed < REHASH_MAX_SIZE
                && rehashIndex <ht0.size){
            if(ht0.table[rehashIndex] == null){
                rehashIndex++;
                emptyVisited++;
                continue;
            }

            DictEntry<K, V> entry = ht0.table[rehashIndex];
            while(entry != null){
                DictEntry<K,V> next = entry.next;

                int idx = keyIndex(entry.key, ht1.size);
                entry.next = ht1.table[idx];
                ht1.table[idx] = entry;
                ht0.used--;
                ht1.used++;

                entry =next;
            }

            ht0.table[rehashIndex] = null;
            rehashIndex++;
            processed++;
        }
        //检查是否已经完成rehash过程
        if(rehashIndex >=  ht0.size){
            ht0 = ht1;
            ht1 =null;
            rehashIndex = -1;
        }
    }

    public Set<K> keySet(){
        Set<K> keys = new HashSet<>();
        if(rehashIndex != -1) rehashStep();

        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                Object value = entry.value;
                if(value instanceof ForwardNode) {
                    ForwardNode forwardNode = (ForwardNode) value;
                    value = forwardNode.getCurrentValue(isSnapshotting.get());
                }
                if(value != null) {
                    keys.add(entry.key);
                }
                entry = entry.next;
            }
        }
        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    Object value = entry.value;
                    if(value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) value;
                        value = forwardNode.getCurrentValue(isSnapshotting.get());
                    }
                    if(value != null) {
                        keys.add(entry.key);
                    }
                    entry = entry.next;
                }
            }
        }
        return keys;
    }

    @SuppressWarnings("unchecked")
    public Map<K,V> getAll(){
        Map<K,V> map = new HashMap<>();

        if(rehashIndex != -1) rehashStep();

        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                Object value = entry.value;
                if(value instanceof ForwardNode) {
                    ForwardNode forwardNode = (ForwardNode) value;
                    value = forwardNode.getCurrentValue(false);
                }
                if(value != null) {
                    map.put(entry.key, (V) value);
                }
                entry = entry.next;
            }
        }

        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    Object value = entry.value;
                    if(value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) value;
                        value = forwardNode.getCurrentValue(isSnapshotting.get());
                    }
                    if(value != null) {
                        map.put(entry.key, (V) value);
                    }
                    entry = entry.next;
                }
            }
        }

        return map;
    }


    public void clear(){
        ht0 = new DictHashTable<>(INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
        modifiedKeys.clear();
        isSnapshotting.set(false);
    }

    public int size(){
        int count = 0;
        
        // 遍历 ht0
        for(int i = 0; i < ht0.size; i++) {
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null) {
                Object value = entry.value;
                if(value instanceof ForwardNode) {
                    ForwardNode forwardNode = (ForwardNode) value;
                    // 主线程看到的是新值
                    value = forwardNode.getCurrentValue(false);
                }
                if(value != null) {
                    count++;
                }
                entry = entry.next;
            }
        }
        
        // 遍历 ht1（如果存在）
        if(ht1 != null) {
            for(int i = 0; i < ht1.size; i++) {
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null) {
                    Object value = entry.value;
                    if(value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) value;
                        // 主线程看到的是新值
                        value = forwardNode.getCurrentValue(false);
                    }
                    if(value != null) {
                        count++;
                    }
                    entry = entry.next;
                }
            }
        }
        
        return count;
    }

    // 开始创建快照
    public void startSnapshot() {
        isSnapshotting.set(true);
    }
    
    // 完成快照，应用所有ForwardNode中的新值
    public void finishSnapshot() {
        isSnapshotting.set(false);
        applyForwardNodes();
    }
    
    // 应用所有ForwardNode中的新值
    private  void applyForwardNodes() {
        synchronized (this){
            // 只处理被修改的key，避免遍历整个哈希表
            for(K key : modifiedKeys) {
                DictEntry<K,V> entry = find(key);
                if(entry != null && entry.value instanceof ForwardNode) {
                    ForwardNode forwardNode = (ForwardNode) entry.value;

                    if(forwardNode.newValue.get() == ForwardNode.REMOVED_SIGNAL) {
                        // 删除该entry
                        removeEntryFromTable(key);
                    } else {
                        // 应用新值
                        entry.value = forwardNode.newValue.get();
                    }
                }
            }
            // 清空修改记录
            modifiedKeys.clear();
        }

    }

    // 创建快照并返回快照数据
    @SuppressWarnings("unchecked")
    public Map<K,V> createSnapshot() {
        startSnapshot();
        
        Map<K,V> snapshot = new HashMap<>();
        
        try{
            // 获取快照时的数据
            for(int i = 0; i < ht0.size; i++) {
                DictEntry<K,V> entry = ht0.table[i];
                while(entry != null) {
                    Object value = entry.value;
                    if(value instanceof ForwardNode) {
                        ForwardNode forwardNode = (ForwardNode) value;
                        Object snapshotValue = forwardNode.getCurrentValue(true);
                        if(snapshotValue != null) {
                            snapshot.put(entry.key, (V) snapshotValue);
                        }
                    } else if(value != null) {
                        snapshot.put(entry.key, (V) value);
                    }
                    entry = entry.next;
                }
            }

            if(ht1 != null) {
                for(int i = 0; i < ht1.size; i++) {
                    DictEntry<K,V> entry = ht1.table[i];
                    while(entry != null) {
                        Object value = entry.value;
                        if(value instanceof ForwardNode) {
                            ForwardNode forwardNode = (ForwardNode) value;
                            Object snapshotValue = forwardNode.getCurrentValue(true);
                            if(snapshotValue != null) {
                                snapshot.put(entry.key, (V) snapshotValue);
                            }
                        } else if(value != null) {
                            snapshot.put(entry.key, (V) value);
                        }
                        entry = entry.next;
                    }
                }
            }

            return snapshot;
        }finally {
            finishSnapshot();
        }
    }

    /**
     * 异步创建RDB快照
     * @return CompletableFuture包装的快照数据
     */
    public CompletableFuture<DictSnapshot<K,V>> createRdbSnapshot() {
        return CompletableFuture.supplyAsync(() -> {
            // 同步创建快照，确保快照创建是原子的
            synchronized (this) {
                // 开始快照
                startSnapshot();
                
                // 创建快照，包含快照时刻的所有有效数据
                return new DictSnapshot<>(this);
            }
        });
    }

    /**
     * 线程安全的字典快照包装类
     */
    public static class DictSnapshot<K,V> implements Iterable<Map.Entry<K,V>> {
        private final Map<K,V> snapshotData;
        private final Dict<K,V> sourceDict;
        
        @SuppressWarnings("unchecked")
        DictSnapshot(Dict<K,V> dict) {
            this.sourceDict = dict;
            this.snapshotData = new HashMap<>();
            


            
            // 获取快照时的数据 - 只包含快照时刻存在的键
            for(int i = 0; i < dict.ht0.size; i++) {
                DictEntry<K,V> entry = dict.ht0.table[i];
                while(entry != null) {

                    Object value = entry.value;
                    if(value instanceof ForwardNode) {

                        ForwardNode forwardNode = (ForwardNode) value;
                        // 快照应该看到旧值，但如果是新增的键（oldValue为null），则不包含在快照中
                        Object snapshotValue = forwardNode.getCurrentValue(true);
                        if(snapshotValue != null) {
                            snapshotData.put(entry.key, (V) snapshotValue);

                        } else {

                        }
                    } else if(value != null) {

                        snapshotData.put(entry.key, (V) value);

                    }
                    entry = entry.next;
                }
            }
            
            if(dict.ht1 != null) {
                for(int i = 0; i < dict.ht1.size; i++) {
                    DictEntry<K,V> entry = dict.ht1.table[i];
                    while(entry != null) {

                        Object value = entry.value;
                        if(value instanceof ForwardNode) {

                            ForwardNode forwardNode = (ForwardNode) value;
                            // 快照应该看到旧值，但如果是新增的键（oldValue为null），则不包含在快照中
                            Object snapshotValue = forwardNode.getCurrentValue(true);
                            if(snapshotValue != null) {
                                snapshotData.put(entry.key, (V) snapshotValue);

                            }
                        } else if(value != null) {

                            snapshotData.put(entry.key, (V) value);

                        }
                        entry = entry.next;
                    }
                }
            }

        }
        
        @Override
        public Iterator<Map.Entry<K,V>> iterator() {
            return snapshotData.entrySet().iterator();
        }
        
        public Map<K,V> toMap() {
            return new HashMap<>(snapshotData);
        }
        
        public int size() {
            return snapshotData.size();
        }
        
        public boolean isEmpty() {
            return snapshotData.isEmpty();
        }
        
        /**
         * 完成快照，应用所有ForwardNode中的新值
         */
        public void finishSnapshot() {
            if (sourceDict != null) {
                sourceDict.finishSnapshot();
            }
        }
    }

}