package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Unit tests for SVUtils.
 */
public class SVUtilsUnitTest extends BaseTest {

    @Test(groups = "spark")
    void hashMapCapacityTest() {
        Assert.assertEquals(SVUtils.hashMapCapacity(150),201);
    }

    @Test(groups = "spark", expectedExceptions = NoSuchElementException.class)
    void emptySingletonIteratorTest() {
        Iterator<Object> itr = new SVUtils.SingletonIterator<>();
        Assert.assertFalse(itr.hasNext());
        itr.next();
    }

    @Test(groups = "spark", expectedExceptions = NoSuchElementException.class)
    void singletonIteratorTest() {
        Iterator<Object> itr = new SVUtils.SingletonIterator<>(this);
        Assert.assertTrue(itr.hasNext());
        Assert.assertTrue(itr.next()==this);
        Assert.assertFalse(itr.hasNext());
        itr.next();
    }

    @Test(groups = "spark", expectedExceptions = NoSuchElementException.class)
    void emptySingletonIterableTest() {
        Iterator<Object> itr = new SVUtils.SingletonIterable<>().iterator();
        Assert.assertFalse(itr.hasNext());
        itr.next();
    }

    @Test(groups = "spark", expectedExceptions = NoSuchElementException.class)
    void singletonIterableTest() {
        Iterator<Object> itr = new SVUtils.SingletonIterable<Object>(this).iterator();
        Assert.assertTrue(itr.hasNext());
        Assert.assertTrue(itr.next()==this);
        Assert.assertFalse(itr.hasNext());
        itr.next();
    }
}
