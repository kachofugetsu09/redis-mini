package site.hnfy258.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Dict<K,V> {

    private static final int INITIAL_SIZE = 4;
    private static final double LOAD_FACTOR = 0.75;
    private static final int REHASH_MAX_SIZE =5;
    private static final int MAX_EMPTY_PERCENT = 10;


    private DictHashTable<K,V> ht0;
    private DictHashTable<K,V> ht1;
    private int rehashIndex;

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
        }
        return false;
    }

    public Iterable<? extends Map.Entry<Object, Object>> entrySet() {
        if(rehashIndex != -1) rehashStep();
        Map<Object, Object> map = new HashMap<>();
        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                map.put( entry.key, entry.value);
                entry = entry.next;
            }
        }
        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }
        return map.entrySet();
    }

    static class DictEntry<K,V>{
        K key;
        V value;
        DictEntry<K,V> next;

        DictEntry(K key, V value){
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
        if(ht0.table == null){  // 确保初始化成功
            ht0.table = new DictEntry[INITIAL_SIZE];
        }
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

        // 1. 先在 ht0 中查找
        if(ht0 == null) return null;  // 防止 ht0 为空
        int idx = keyIndex(key, ht0.size);
        if(idx < 0 || idx >= ht0.size || ht0.table == null) return null;  // 防止数组越界
        
        DictEntry<K,V> entry = ht0.table[idx];
        while(entry != null){
            if(key.equals(entry.key)){  // 使用 equals 而不是 entry.key.equals 防止 key 为空
                return entry;
            }
            entry = entry.next;
        }

        // 2. 如果在 rehash 或 ht1 存在，则在 ht1 中查找
        if(rehashIndex != -1 && ht1 != null && ht1.table != null){  // 确保 ht1 和其 table 都不为空
            idx = keyIndex(key, ht1.size);
            if(idx >= 0 && idx < ht1.size){  // 确保索引有效
                entry = ht1.table[idx];
                while(entry != null){
                    if(key.equals(entry.key)){
                        return entry;
                    }
                    entry = entry.next;
                }
            }
        }
        return null;
    }

    public V put(K key, V value){
        if(key == null) throw new IllegalArgumentException("key can not be null");
        
        // 1. 检查是否需要 rehash
        if(rehashIndex == -1 && ht0 != null){  // 确保 ht0 不为空
            double loadFactor = (double) ht0.used / ht0.size;
            if(loadFactor > LOAD_FACTOR){
                startRehash(ht0.size);
            }
        }

        // 2. 执行渐进式 rehash
        if(rehashIndex != -1) rehashStep();

        // 3. 查找是否已存在
        DictEntry<K,V> entry = find(key);
        if(entry != null){
            V oldValue = entry.value;
            entry.value = value;
            return oldValue;
        }

        // 4. 选择要插入的哈希表
        DictHashTable<K,V> ht = (rehashIndex != -1 && ht1 != null) ? ht1 : ht0;
        if(ht == null || ht.table == null) {  // 确保目标哈希表可用
            ht = new DictHashTable<>(INITIAL_SIZE);
            if(ht.table == null){
                ht.table = new DictEntry[INITIAL_SIZE];
            }
            if(rehashIndex == -1){
                ht0 = ht;
            } else {
                ht1 = ht;
            }
        }

        // 5. 计算索引并检查有效性
        int idx = keyIndex(key, ht.size);
        if(idx < 0 || idx >= ht.size) return null;  // 索引无效则返回

        // 6. 创建新节点并插入
        DictEntry<K,V> newEntry = new DictEntry<>(key, value);
        newEntry.next = ht.table[idx];
        ht.table[idx] = newEntry;
        ht.used++;

        return null;
    }

    public V get(K key){
        DictEntry<K, V> entry = find(key);
        return entry!=null?entry.value:null;
    }

    public V remove(K key){
        if(key ==null) return null;
        if(rehashIndex != -1) rehashStep();

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
                return entry.value;
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
                    return entry.value;
                }
                prev = entry;
                entry = entry.next;
            }
        }
        return null;
    }

    private void startRehash(int size){
        if(size <= 0) return;  // 防止size为0或负数
        ht1 = new DictHashTable<>(size * 2);
        if(ht1.table == null){  // 确保初始化成功
            ht1.table = new DictEntry[size * 2];
        }
        rehashIndex = 0;
    }

    private void rehashStep(){
        // 1. 增加全面的空值检查
        if(rehashIndex == -1 || ht1 == null || ht1.table == null || 
           ht0 == null || ht0.table == null || rehashIndex >= ht0.size) {
            return;
        }

        int emptyVisited = 0;
        int processed = 0;

        // 2. 确保不会越界
        while(emptyVisited < MAX_EMPTY_PERCENT
                && processed < REHASH_MAX_SIZE
                && rehashIndex < ht0.size){
            
            if(ht0.table[rehashIndex] == null){
                rehashIndex++;
                emptyVisited++;
                continue;
            }

            // 3. 移动整个链表
            DictEntry<K,V> entry = ht0.table[rehashIndex];
            while(entry != null){
                DictEntry<K,V> next = entry.next;  // 保存下一个节点

                // 4. 计算新的索引并检查有效性
                int idx = keyIndex(entry.key, ht1.size);
                if(idx >= 0 && idx < ht1.size){  // 确保索引有效
                    entry.next = ht1.table[idx];  // 头插法
                    ht1.table[idx] = entry;
                    
                    // 5. 更新计数
                    ht0.used--;
                    ht1.used++;
                }

                entry = next;  // 移动到下一个节点
            }

            // 6. 清空已处理的桶
            ht0.table[rehashIndex] = null;
            rehashIndex++;
            processed++;
        }

        // 7. 检查是否完成 rehash
        if(rehashIndex >= ht0.size){
            ht0 = ht1;
            ht1 = null;
            rehashIndex = -1;
        }
    }

    public Set<K> keySet(){
        Set<K> keys = new HashSet<>();
        if(rehashIndex != -1) rehashStep();

        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                keys.add(entry.key);
                entry = entry.next;
            }
        }
        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }
        return keys;
    }

    public Map<K,V> getAll(){
        Map<K,V> map = new HashMap<>();

        if(rehashIndex != -1) rehashStep();

        for(int i =0; i < ht0.size; i++){
            DictEntry<K,V> entry = ht0.table[i];
            while(entry != null){
                map.put(entry.key, entry.value);
                entry = entry.next;
            }
        }

        if(rehashIndex != -1 && ht1 !=null){
            for(int i =0; i < ht1.size; i++){
                DictEntry<K,V> entry = ht1.table[i];
                while(entry != null){
                    map.put(entry.key, entry.value);
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
    }

    public int size(){
        return ht0.used+(ht1 != null?ht1.used:0);
    }

}
