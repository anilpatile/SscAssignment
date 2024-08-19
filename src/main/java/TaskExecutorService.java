import java.util.*;
import java.util.concurrent.*;

public class TaskExecutorService implements Main.TaskExecutor {

    // A map to track running tasks per TaskGroup
    private final Map<UUID, Queue<FutureTaskWrapper<?>>> taskGroupMap = new HashMap<>();
    // ExecutorService to handle task execution
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Override
    public synchronized <T> Future<T> submitTask(Main.Task<T> task) {
        UUID groupUUID = task.taskGroup().groupUUID();

        FutureTaskWrapper<T> futureTask = new FutureTaskWrapper<>(task);

        taskGroupMap.putIfAbsent(groupUUID, new LinkedList<>());

        Queue<FutureTaskWrapper<?>> groupQueue = taskGroupMap.get(groupUUID);
        groupQueue.add(futureTask);

        if (groupQueue.size() == 1) {
            executeNextTask(groupUUID);
        }

        return futureTask.getFuture();
    }

    // Execute the next task in the group
    private synchronized void executeNextTask(UUID groupUUID) {
        Queue<FutureTaskWrapper<?>> groupQueue = taskGroupMap.get(groupUUID);

        if (groupQueue == null || groupQueue.isEmpty()) {
            return;
        }

        FutureTaskWrapper<?> nextTask = groupQueue.peek();

        executorService.submit(() -> {
            try {
                nextTask.run();
                groupQueue.poll();
                executeNextTask(groupUUID); // Trigger the next task in the queue
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // Wrapper class to handle FutureTask and link to the Task object
    private static class FutureTaskWrapper<T> {
        private final FutureTask<T> futureTask;
        private final Main.Task<T> task;

        public FutureTaskWrapper(Main.Task<T> task) {
            this.task = task;
            this.futureTask = new FutureTask<>(task.taskAction());
        }

        public void run() {
            futureTask.run();
        }

        public Future<T> getFuture() {
            return futureTask;
        }
    }

    // Shutdown method to stop the executor service gracefully
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    // Main method for testing
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        TaskExecutorService executorService = new TaskExecutorService();

        Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<Integer> task1 = new Main.Task<>(
                UUID.randomUUID(),
                group1,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(2000);
                    return 1;
                }
        );

        Main.Task<Integer> task2 = new Main.Task<>(
                UUID.randomUUID(),
                group1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return 2;
                }
        );

        Main.Task<Integer> task3 = new Main.Task<>(
                UUID.randomUUID(),
                group2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(1500);
                    return 3;
                }
        );

        Future<Integer> future1 = executorService.submitTask(task1);
        Future<Integer> future2 = executorService.submitTask(task2);
        Future<Integer> future3 = executorService.submitTask(task3);

        System.out.println("Task 1 Result: " + future1.get());
        System.out.println("Task 2 Result: " + future2.get());
        System.out.println("Task 3 Result: " + future3.get());

        executorService.shutdown();
    }
}