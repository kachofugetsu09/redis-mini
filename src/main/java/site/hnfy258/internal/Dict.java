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
        ht0 = new DictHashTable(INITIAL_SIZE);
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

    public V put(K key, V value){
        if(key == null) throw new IllegalArgumentException("key can not be null");
        //如果不在rehash
        if(rehashIndex == -1){
            double loadFactor = (double) ht0.used / ht0.size;
            if(loadFactor > LOAD_FACTOR){
                startRehash(ht0.size);
            }
        }
        //如果在rehash
        if(rehashIndex != -1) rehashStep();

        V oldValue = null;
        DictEntry<K,V> entry = find(key);

        if(entry != null){
            oldValue = entry.value;
            entry.value = value;
            return oldValue;
        }

        int idx;
        if(rehashIndex != -1){
            idx = keyIndex(key, ht1.size);
            DictEntry<K, V> newEntry = new DictEntry<>(key, value);
            newEntry.next = ht1.table[idx];
            ht1.table[idx] = newEntry;
            ht1.used++;
        }
        else{
            idx = keyIndex(key, ht0.size);
            DictEntry<K, V> newEntry = new DictEntry<>(key, value);
            newEntry.next = ht0.table[idx];
            ht0.table[idx] = newEntry;
            ht0.used++;
        }

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
        ht1 = new DictHashTable(size*2);
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
