package site.hnfy258.internal;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
public class SkipList <T extends Comparable<T>>{
    private static final int MAX_LEVEL =32;
    private static final double P =0.25;
    private SkipListNode<T> head;
    private int level;
    private int size;
    private Random random;
    private SkipListNode<T> tail;

    public static class SkipListNode<T> {
        public double score;
        public T member;
        SkipListNode<T> backward;
        SkipListLevel[] level;

        public SkipListNode(int l,double score, T member)
        {
            this.level = new SkipListLevel[l];
            for (int i = 0; i < l; i++) {
                this.level[i] = new SkipListLevel(null,0);
            }
            this.score = score;
            this.member = member;
            this.backward = null;
        }

        static class SkipListLevel<T>{
            SkipListNode forward;
            long span;

            public SkipListLevel(SkipListNode<T> forward, long span) {
                this.forward = forward;
                this.span = span;
            }
        }
    }

    public SkipList(){
        head = new SkipListNode(MAX_LEVEL,Double.NEGATIVE_INFINITY, null);
        level = 1;
        size = 0;
        this.random = new Random();
    }

    public SkipListNode<T> insert(double score, T member){
        //创建update数组，用以储存该插入的位置
        SkipListNode<T>[] update = new SkipListNode[MAX_LEVEL];
        long[] rank = new long[MAX_LEVEL];
        SkipListNode<T> x = head;
        int i;

        for(i = level - 1; i >= 0; i--){
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            getPosition(score,member,x,i,update);
            update[i] =x;
        }

        //算出新的level
        int newLevel = randomLevel();
        if(newLevel > level){
            for(i = level;i<newLevel;i++){
                rank[i] = 0;
                update[i] = head;
                update[i].level[i].span = size;
            }
            level =newLevel;
        }

        //创建新节点
        x = new SkipListNode<>(newLevel,score,member);

        for(i=0;i<newLevel;i++){
            x.level[i].forward = update[i].level[i].forward;
            update[i].level[i].forward = x;

            x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);

            update[i].level[i].span++;
        }

        x.backward = (update[0]==this.head)?null:update[0];

        if(x.level[0].forward != null){
            x.level[0].forward.backward = x;
        }
        else{
            tail = x;
        }
        size++;
        return x;
    }

    public boolean delete(double score, T member){
        //创建update数组，用以储存该插入的位置
        SkipListNode<T>[] update = new SkipListNode[MAX_LEVEL];
        SkipListNode<T> x = head;
        int i;

        for(i = level - 1; i >= 0; i--){
            x = getPosition(score,member,x,i,update);
        }
        x = x.level[0].forward;

        if(x != null && x.score == score && compare(x.member,member)==0){
            skipListDelete(x,update);
            return true;
        }
        return false;

    }

    private void skipListDelete(SkipListNode<T> x, SkipListNode<T>[] update) {
        int i;
        for(i=0;i<level;i++){
            if(update[i].level[i].forward ==x){
                update[i].level[i].span += x.level[i].span - 1;
                update[i].level[i].forward = x.level[i].forward;
            }else{
                update[i].level[i].span--;
            }
        }

        if(x.level[0].forward !=null){
            x.level[0].forward.backward = x.backward;
        }
        else{
            tail = x.backward;
        }

        //检查空层，删除空层
        while(level >1 && head.level[level -1].forward == null){
            level--;
        }
        size--;
    }

    private int randomLevel() {
        int level = 1;
        while(random.nextDouble() <P && level< MAX_LEVEL){
            level++;
        }

        return level;
    }


    //查找当前节点应该在的位置
    //1.他的后驱节点不能为空
    //2.他的后驱节点的分数小于插入的分数
    //3.如果后驱节点的分数等于当前分数，但是他的member 小于当前插入的member，插入。
    private SkipListNode<T> getPosition(double score, T member, SkipListNode<T> x, int i, SkipListNode<T>[] update) {
        while(x.level[i].forward != null
        && ((x.level[i].forward.score < score)||(x.level[i].forward.score == score && compare((T)x.level[i].forward.member, member) < 0))){
            x = x.level[i].forward;
        }
        update[i] =x;
        return x;
    }


    public int compare(T o1, T o2){
        if(o1 ==null || o2 == null){
            throw new NullPointerException("can't be null");
        }
        return o1.compareTo(o2);
    }

    public List<SkipListNode<T>> getElementByRankRange(long start, long end){
        List<SkipListNode<T>> list = new ArrayList<>();

        // 检查边界条件
        if (size == 0 || start > end || start >= size) {
            return list;
        }

        // 边界检查
        start = Math.max(0, start);
        end = Math.min(end, size - 1);

        // Redis索引从0开始，而SkipList从1开始，所以需要+1
        long startRank = start + 1;
        long endRank = end + 1;

        // 特殊情况处理
        if (startRank > size || endRank < 1) {
            return list;
        }

        SkipListNode<T> x = head;
        if (x == null) {
            return list;
        }

        long traversed = 0;

        // 使用跳跃表特性快速定位到start位置
        for (int i = level - 1; i >= 0; i--) {
            while (x != null && x.level != null && x.level.length > i && 
                  x.level[i] != null && x.level[i].forward != null && 
                  (traversed + x.level[i].span) <= startRank - 1) {
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
        }

        // 到了start位置的前一个节点，移到start位置
        if (x != null && x.level != null && x.level.length > 0 && 
            x.level[0] != null && x.level[0].forward != null) {
            x = x.level[0].forward;
            traversed++;
        } else {
            return list; // 如果已经到达尾部，返回空列表
        }

        // 收集从start到end的所有节点
        while (x != null && traversed <= endRank) {
            list.add(x);
            if (x.level != null && x.level.length > 0 && 
                x.level[0] != null && x.level[0].forward != null) {
                x = x.level[0].forward;
                traversed++;
            } else {
                break;
            }
        }

        return list;
    }

    public SkipListNode<T> getElementByRank(long rank){
        if(rank<=0 || rank >size){
            return null;
        }
        SkipListNode<T> x = head;
        long traversed = 0;

        for(int i=level-1;i>=0;i--){
            while(x.level[i].forward != null && traversed+x.level[i].span <= rank){
                traversed+=x.level[i].span;
            }
            if(traversed == rank){
                return x;
            }
        }
        return null;
    }


    public List<SkipListNode<T>> getElementByScoreRange(double min,double max){
        List<SkipListNode<T>> list = new ArrayList<SkipListNode<T>>();

        SkipListNode<T> x = head;

        for(int i=level-1;i>=0;i--){
            while(x.level[i].forward != null && x.level[i].forward.score < min){
                x = x.level[i].forward;
            }
        }
        x = x.level[0].forward;

        while(x !=null && x.score <= max){
            list.add(x);
            x = x.level[0].forward;
        }
        return list;
    }
}
