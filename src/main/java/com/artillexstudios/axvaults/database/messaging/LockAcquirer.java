package com.artillexstudios.axvaults.database.messaging;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public final class LockAcquirer {

    private LockAcquirer() {
    }

    public static CompletableFuture<Void> acquire(@NotNull ScheduledExecutorService executor, @NotNull BooleanSupplier tryLock,
                                                   long timeoutMillis, long retryMillis) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);

        Runnable attempt = new Runnable() {
            @Override
            public void run() {
                if (result.isDone()) return;
                try {
                    if (tryLock.getAsBoolean()) {
                        result.complete(null);
                    } else if (System.nanoTime() >= deadline) {
                        result.completeExceptionally(new TimeoutException("Vaults are still locked by another server"));
                    } else {
                        executor.schedule(this, retryMillis, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception ex) {
                    result.completeExceptionally(ex);
                }
            }
        };

        try {
            executor.execute(attempt);
        } catch (Exception ex) {
            result.completeExceptionally(ex);
        }
        return result;
    }
}
