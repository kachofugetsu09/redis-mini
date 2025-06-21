package site.hnfy258.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {
    private SkipList<String> skipList;

    @BeforeEach
    void setUp() {
        skipList = new SkipList<>();
    }

    @Test
    void testInitialState() {
        assertTrue(skipList.isEmpty());
        assertEquals(0, skipList.size());
        assertNull(skipList.getFirst());
        assertNull(skipList.getLast());
    }

    @Test
    void testInsertAndSize() {
        skipList.insert(1.0, "one");
        assertEquals(1, skipList.size());
        assertFalse(skipList.isEmpty());

        skipList.insert(2.0, "two");
        assertEquals(2, skipList.size());

        SkipList.SkipListNode<String> first = skipList.getFirst();
        assertNotNull(first);
        assertEquals("one", first.member);
        assertEquals(1.0, first.score);

        SkipList.SkipListNode<String> last = skipList.getLast();
        assertNotNull(last);
        assertEquals("two", last.member);
        assertEquals(2.0, last.score);
    }

    @Test
    void testDelete() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");

        assertTrue(skipList.delete(2.0, "two"));
        assertEquals(2, skipList.size());
        assertFalse(skipList.delete(2.0, "two")); // 已经删除，应该返回false

        SkipList.SkipListNode<String> first = skipList.getFirst();
        SkipList.SkipListNode<String> last = skipList.getLast();
        assertEquals("one", first.member);
        assertEquals("three", last.member);
    }

    @Test
    void testGetElementByRank() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");

        SkipList.SkipListNode<String> node = skipList.getElementByRank(2);
        assertNotNull(node);
        assertEquals("two", node.member);
        assertEquals(2.0, node.score);

        assertNull(skipList.getElementByRank(0)); // 无效的排名
        assertNull(skipList.getElementByRank(4)); // 超出范围的排名
    }

    @Test
    void testGetElementByRankRange() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");
        skipList.insert(4.0, "four");

        List<SkipList.SkipListNode<String>> range = skipList.getElementByRankRange(1, 2);
        assertEquals(2, range.size());
        assertEquals("two", range.get(0).member);
        assertEquals("three", range.get(1).member);

        // 测试边界情况
        assertTrue(skipList.getElementByRankRange(-1, 0).isEmpty()); // 无效的起始位置
        assertTrue(skipList.getElementByRankRange(4, 5).isEmpty()); // 超出范围
        assertEquals(4, skipList.getElementByRankRange(0, 10).size()); // 超出末尾
    }

    @Test
    void testGetElementByScoreRange() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");
        skipList.insert(4.0, "four");

        List<SkipList.SkipListNode<String>> range = skipList.getElementByScoreRange(2.0, 3.0);
        assertEquals(2, range.size());
        assertEquals("two", range.get(0).member);
        assertEquals("three", range.get(1).member);

        // 测试边界情况
        assertTrue(skipList.getElementByScoreRange(5.0, 6.0).isEmpty()); // 范围在所有元素之后
        assertTrue(skipList.getElementByScoreRange(-1.0, 0.0).isEmpty()); // 范围在所有元素之前
        assertEquals(4, skipList.getElementByScoreRange(0.0, 5.0).size()); // 包含所有元素
    }

    @Test
    void testGetRank() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");

        assertEquals(1, skipList.getRank(1.0, "one"));
        assertEquals(2, skipList.getRank(2.0, "two"));
        assertEquals(3, skipList.getRank(3.0, "three"));
        assertEquals(0, skipList.getRank(4.0, "four")); // 不存在的元素
    }

    @Test
    void testSameScoreDifferentMembers() {
        skipList.insert(1.0, "a");
        skipList.insert(1.0, "b");
        skipList.insert(1.0, "c");

        List<SkipList.SkipListNode<String>> nodes = skipList.getElementByScoreRange(1.0, 1.0);
        assertEquals(3, nodes.size());
        // 验证按字典序排序
        assertEquals("a", nodes.get(0).member);
        assertEquals("b", nodes.get(1).member);
        assertEquals("c", nodes.get(2).member);
    }

    @Test
    void testBackwardPointers() {
        skipList.insert(1.0, "one");
        skipList.insert(2.0, "two");
        skipList.insert(3.0, "three");

        SkipList.SkipListNode<String> last = skipList.getLast();
        assertNotNull(last);
        assertEquals("three", last.member);

        SkipList.SkipListNode<String> second = last.backward;
        assertNotNull(second);
        assertEquals("two", second.member);

        SkipList.SkipListNode<String> first = second.backward;
        assertNotNull(first);
        assertEquals("one", first.member);

        assertNull(first.backward); // 第一个节点的backward应该是null
    }
} 