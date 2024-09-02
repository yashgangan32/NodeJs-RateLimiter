const fs = require('fs');
const Redis = require('ioredis');
const redis = new Redis();

const TASK_LIMIT_PER_SECOND = 1;
const TASK_LIMIT_PER_MINUTE = 20;

// Task function
async function task(user_id) {
  const logMessage = `${user_id}-task completed at-${Date.now()}\n`;
  console.log(logMessage);

  // Append log to file
  fs.appendFile('task_logs.txt', logMessage, (err) => {
    if (err) console.error('Error writing to log file:', err);
  });
}

// Handle task request
async function handleTaskRequest(user_id, res) {
  try {
    // Increment counters for rate limiting
    const currentSecondCount = await redis.incr(`user:${user_id}:second`);
    const currentMinuteCount = await redis.incr(`user:${user_id}:minute`);

    if (currentSecondCount === 1) {
      redis.expire(`user:${user_id}:second`, 1);
    }
    if (currentMinuteCount === 1) {
      redis.expire(`user:${user_id}:minute`, 60);
    }

    if (currentSecondCount > TASK_LIMIT_PER_SECOND || currentMinuteCount > TASK_LIMIT_PER_MINUTE) {
      // Add to queue
      await redis.rpush(`queue:${user_id}`, Date.now());
      return res.status(429).json({ error: 'Rate limit exceeded, your task has been queued.' });
    }

    // Process task immediately
    await task(user_id);
    res.status(200).json({ message: 'Task completed successfully.' });

    // Check the queue for any pending tasks
    processQueue(user_id);

  } catch (err) {
    console.error('Error handling task request:', err);
    res.status(500).json({ error: 'Internal server error.' });
  }
}

// Process the queued tasks
async function processQueue(user_id) {
  const nextTaskTimestamp = await redis.lpop(`queue:${user_id}`);
  if (nextTaskTimestamp) {
    const currentTime = Date.now();
    const delay = Math.max(0, 1000 - (currentTime - parseInt(nextTaskTimestamp, 10)));

    setTimeout(async () => {
      await task(user_id);
      processQueue(user_id);// Recursively process the next task in the queue
    }, delay);
  }
}
// Gracefully close Redis connection on termination signals
function closeRedisConnection() {
  redis.quit(() => {
    console.log('Redis connection closed gracefully.');
  });
}

process.on('SIGINT', closeRedisConnection);  // Handle Ctrl+C
process.on('SIGTERM', closeRedisConnection); // Handle kill command

module.exports = { handleTaskRequest };
