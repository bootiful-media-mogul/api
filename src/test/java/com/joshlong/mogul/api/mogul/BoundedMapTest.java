package com.joshlong.mogul.api.mogul;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

// todo were getting rate limited in the user of the userinfo endpoint! 
// need some sort of memoization 
// im creating a bounded expiring memory map  

class BoundedMapTest {

    // Example usage 
    @Test
    void test() {
        var map = new BoundedMap<>(3);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");
        assertFalse(map.containsKey("key1"), "there should be no key1");
        map.get("key2");
        map.put("key3", "updated value");
        map.put("key5", "value5");
        assertTrue(!map.containsKey("key4"), "there should be no key1");
        System.out.println("Final map: " + map);  // shows key2, key3, key5
    }
}