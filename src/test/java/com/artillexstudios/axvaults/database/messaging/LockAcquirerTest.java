package com.artillexstudios.axvaults.database.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LockAcquirerTest {
    private ScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void acquiresImmediatelyWhenLockIsFree() throws Exception {
        assertNull(LockAcquirer.acquire(executor, () -> true, 1000, 50).get(1, TimeUnit.SECONDS));
    }

    @Test
    void acquiresOnRetryAfterPreviousOwnerReleases() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        var future = LockAcquirer.acquire(
                executor,
                () -> attempts.incrementAndGet() >= 3,
                1000,
                30
        );

        assertNull(future.get(2, TimeUnit.SECONDS));
        assertEquals(3, attempts.get());
    }

    @Test
    void refusesToTakeOverAnActiveOwnersLock() {
        var future = LockAcquirer.acquire(executor, () -> false, 0, 10);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, failure.getCause());
    }

    @Test
    void preservesBackendFailureInsteadOfGrantingAccess() {
        IllegalStateException backendFailure = new IllegalStateException("Redis unavailable");
        var future = LockAcquirer.acquire(executor, () -> { throw backendFailure; }, 1000, 10);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertEquals(backendFailure, failure.getCause());
    }
}
