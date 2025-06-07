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

        static class SkipListLevel{
            SkipListNode forward;
            long span;

            public SkipListLevel(SkipListNode forward, long span) {
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
            // 1. 正确初始化rank数组，存储跨越的排名
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            // 2. 使用getPosition方法进行搜索并更新rank数组
            x = getPosition(score, member, x, i, update, rank);
        }

        //算出新的level
        int newLevel = randomLevel();
        if(newLevel > level){
            for(i = level;i<newLevel;i++){
                rank[i] = 0;
                update[i] = head;
                update[i].level[i].span = size;
            }
            level = newLevel;
        }

        //创建新节点
        x = new SkipListNode<>(newLevel,score,member);

        for(i = 0; i < newLevel; i++){
            x.level[i].forward = update[i].level[i].forward;
            update[i].level[i].forward = x;

            // 3. 正确计算新节点的span
            x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);
            // 4. 正确计算前驱节点的span
            update[i].level[i].span = (rank[0] - rank[i]) + 1;
        }

        // 5. 对于未触及的层级，增加span（因为插入了新节点）
        for(i = newLevel; i < level; i++){
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
        long[] rank = new long[MAX_LEVEL];
        SkipListNode<T> x = head;
        int i;

        for(i = level - 1; i >= 0; i--){
            // 1. 正确初始化rank数组
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            // 2. 使用修改后的getPosition方法
            x = getPosition(score, member, x, i, update, rank);
        }
        x = x.level[0].forward;        if(x != null && x.score == score && compare(x.member,member)==0){
            skipListDelete(x,update);
            return true;
        }
        return false;

    }

    private void skipListDelete(SkipListNode<T> x, SkipListNode<T>[] update) {
        int i;
        for(i=0;i<level;i++){
            if(update[i].level[i].forward ==x){
                // 如果当前层级的前驱节点直接指向要删除的节点
                // 需要将删除节点的span加到前驱节点，然后减1（因为删除了一个节点）
                update[i].level[i].span += x.level[i].span - 1;
                update[i].level[i].forward = x.level[i].forward;
            }else{
                // 如果当前层级的前驱节点不直接指向要删除的节点
                // 说明要删除的节点在更低层级，只需要将span减1
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
    }    /**
     * 查找当前节点应该在的位置，同时更新rank数组
     * @param score 目标分数
     * @param member 目标成员
     * @param x 当前节点
     * @param i 当前层级
     * @param update 更新数组
     * @param rank rank数组，用于记录跨越的排名
     * @return 找到的位置节点
     */
    private SkipListNode<T> getPosition(double score, T member, SkipListNode<T> x, int i, 
                                       SkipListNode<T>[] update, long[] rank) {
        while(x.level[i].forward != null
        && ((x.level[i].forward.score < score)||(x.level[i].forward.score == score && compare((T)x.level[i].forward.member, member) < 0))){
            // 累加span到rank中
            rank[i] += x.level[i].span;
            x = x.level[i].forward;
        }
        update[i] = x;
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

        // 收集从start到end的所有节点到临时列表
        List<SkipListNode<T>> tempList = new ArrayList<>();
        while (x != null && traversed <= endRank) {
            tempList.add(x);
            if (x.level != null && x.level.length > 0 && 
                x.level[0] != null && x.level[0].forward != null) {
                x = x.level[0].forward;
                traversed++;
            } else {
                break;
            }
        }
        
        // 反转临时列表添加到最终结果列表，保证分数从低到高
        for (int i = tempList.size() - 1; i >= 0; i--) {
            list.add(tempList.get(i));
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
            while(x.level[i].forward != null && (traversed + x.level[i].span) <= rank){
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
        }
        
        // 如果traversed等于rank，说明x就是第rank个节点
        if(traversed == rank){
            return x;
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
