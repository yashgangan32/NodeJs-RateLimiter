const cluster = require('cluster');
const os = require('os');
const express = require('express');
const Redis = require('ioredis');
const { handleTaskRequest } = require('./taskHandler');// Import the task handler

const numCPUs = os.cpus().length;

require('dotenv').config();


const redis = new Redis({
  port: process.env.REDIS_PORT,
  host: process.env.REDIS_HOST,
});


redis.on('connect', () => {
  console.log('Connected to Redis on port 6380');
});


if (cluster.isMaster) {
  // Create worker processes (2 replicas as instructed)
  for (let i = 0; i < Math.min(2, numCPUs); i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Forking a new one...`);
    cluster.fork();
  });
} else {
  // Worker processes have a HTTP server
  const app = express();
  app.use(express.json());

  //POST route to handle task requests
  app.post('/task', (req, res) => {
    const { user_id } = req.body;
    if (!user_id) {
      return res.status(400).json({ error: 'user_id is required' });
    }

    handleTaskRequest(user_id, res);  // Call the function to handle task processing
  });
  const port = process.env.PORT;
  app.listen(port, () => {
    console.log(`Worker ${process.pid} started`);
  });
}


