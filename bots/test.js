const TelegramBot = require('node-telegram-bot-api');
const sqlite3 = require('sqlite3').verbose();
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');
const winston = require('winston');
const RateLimiter = require('limiter').RateLimiter;

// Logger Configuration
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' })
    ]
});

// Configuration Management
let BOT_TOKENS = process.env.BOT_TOKENS ? 
    JSON.parse(process.env.BOT_TOKENS) : 
    require('./config.json').BOT_TOKENS;

// Debounced Configuration Reload
let reloadTimeout;
fs.watch(path.join(__dirname, 'config.json'), (eventType, filename) => {
    if (eventType === 'change') {
        clearTimeout(reloadTimeout);
        reloadTimeout = setTimeout(() => {
            logger.info('Reloading configuration...');
            try {
                const newConfig = require('./config.json');
                BOT_TOKENS = newConfig.BOT_TOKENS;
                logger.info('Configuration reloaded successfully');
            } catch (error) {
                logger.error('Error reloading configuration:', error);
            }
        }, 1000); // 1-second debounce
    }
});

// Bot Configuration
const GROUP_NAME = "meandsaint";
const DB_PATH = 'quantik_chat.db';
const AVERAGE_TYPING_SPEED = 50; // Characters per second
const TYPING_VARIANCE = 0.2; // ±20% variance in typing speed

const { messages, commonPhrases } = require('./data');

// Rate Limiter
const rateLimiter = new RateLimiter({
    tokensPerInterval: 20, // Max 20 messages per interval
    interval: 'minute' // Per minute
});

// Database Manager
class DatabaseManager {
    constructor(dbPath) {
        this.dbPath = dbPath;
        this.db = null;
        this.connected = false;
    }

    async connect() {
        try {
            this.db = new sqlite3.Database(this.dbPath);
            await this.initializeDatabase();
            this.connected = true;
            logger.info('Database connected successfully');
        } catch (error) {
            logger.error('Database connection error:', error);
            throw error;
        }
    }

    initializeDatabase() {
        this.db.serialize(() => {
            this.db.run('PRAGMA journal_mode=WAL');
            this.db.run(`
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT,
                    content TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    bot_id TEXT
                )
            `);
            this.db.run('CREATE INDEX IF NOT EXISTS idx_timestamp ON messages (timestamp)');
            this.db.run('CREATE INDEX IF NOT EXISTS idx_bot_id ON messages (bot_id)');
            
            // Clear all messages on startup
            this.db.run('DELETE FROM messages', (err) => {
                if (err) {
                    logger.error('Error clearing messages:', err);
                } else {
                    logger.info('Cleared message history on startup');
                }
            });
        });
    }

    async getRecentMessages(days = 3, excludeBotId = null) {
        const query = `
            SELECT content FROM messages 
            WHERE timestamp >= datetime('now', '-${days} days') 
            ${excludeBotId ? "AND bot_id != ?" : ""}
            ORDER BY timestamp DESC
        `;
        const params = excludeBotId ? [excludeBotId] : [];

        return new Promise((resolve, reject) => {
            this.db.all(query, params, (err, rows) => {
                if (err) reject(err);
                else resolve(rows.map(row => row.content));
            });
        });
    }

    async insertMessage(msgType, content, botId) {
        const query = `
            INSERT INTO messages (type, content, bot_id)
            VALUES (?, ?, ?)
        `;
        const params = [msgType, content, botId];

        return new Promise((resolve, reject) => {
            this.db.run(query, params, function (err) {
                if (err) reject(err);
                else resolve(this.lastID);
            });
        });
    }

    async close() {
        if (this.connected && this.db) {
            await new Promise((resolve, reject) => {
                this.db.close((err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
            this.connected = false;
            logger.info('Database connection closed');
        }
    }
}

// Bot Class
class QuantikBot {
    constructor(botToken, groupName, dbManager, instanceId) {
        this.botToken = botToken;
        this.groupName = groupName;
        this.dbManager = dbManager;
        this.instanceId = instanceId;
        this.running = true;
        this.lastMessageTime = 0;
        this.sleepTime = this.randomSleepTime();
        this.replyChance = 0.6; // 30% chance to reply
        this.chatId = null;
        this.lastSentMessages = new Set(); // Track recent messages
        this.minMessageInterval = 15 * 60 * 1000; // Minimum 15 minutes between messages
        this.maxMessageInterval = 120 * 60 * 1000; // Maximum 2 hours between messages
        this.botUsername = null; // Store bot's username
        this.retryAttempts = 0;
        this.maxRetries = 3;
        this.retryDelay = 5000; // 5 seconds
        this.pollingRetries = 0;
        this.maxPollingRetries = 5;
        this.connectionRetries = 0;
        this.maxConnectionRetries = 5;
        this.connectionRetryDelay = 5000;
        this.networkRetries = 0;
        this.maxNetworkRetries = 5;
        this.networkRetryDelay = 10000; // 10 seconds
        this.pollingOptions = {
            polling: {
                interval: 3000, // Increased interval
                params: {
                    timeout: 30,
                    allowed_updates: ['message'] // Only listen for messages
                },
                autoStart: false, // Don't auto-start polling
                errorEntry: false, // Handle errors manually
                request: {
                    timeout: 30000,
                    forever: true,
                    pool: { maxSockets: 5 }
                }
            }
        };

        this.setupBot();
    }

    async setupBot() {
        try {
            this.bot = new TelegramBot(this.botToken, this.pollingOptions);
            
            // Add request interceptor for retries
            this.bot._request = this.wrapRequestWithRetry(this.bot._request.bind(this.bot));
            
            // Setup error handlers
            this.setupErrorHandlers();

            // Initialize bot
            await this.initializeWithRetry();
            logger.info(`Bot ${this.instanceId} initialized successfully`);
        } catch (error) {
            logger.error(`Bot ${this.instanceId} initialization error:`, error);
            throw error;
        }
    }

    async simulateTyping(chatId, message) {
        const typingDelay = (message.length / AVERAGE_TYPING_SPEED) * 1000; // Base delay
        const variance = typingDelay * TYPING_VARIANCE * (Math.random() * 2 - 1); // ±20% variance
        const totalDelay = typingDelay + variance;

        await this.bot.sendChatAction(chatId, 'typing');
        await this.sleep(totalDelay);
    }

    async sendMessageWithTyping(chatId, message) {
        await this.simulateTyping(chatId, message);
        await this.bot.sendMessage(chatId, message);
    }

    async sendMessageWithRateLimit(chatId, message) {
        const remainingMessages = await rateLimiter.removeTokens(1);
        if (remainingMessages < 0) {
            logger.warn(`Bot ${this.instanceId} rate limit exceeded, waiting...`);
            await this.sleep(60000); // Wait 1 minute
            return this.sendMessageWithRateLimit(chatId, message);
        }
        await this.sendMessageWithTyping(chatId, message);
    }

    // ... (rest of the code remains the same)
    setupErrorHandlers() {
        this.bot.on('error', async (error) => {
            logger.error(`Bot ${this.instanceId} general error:`, error);
            await this.handleError(error);
        });

        this.bot.on('polling_error', async (error) => {
            if (error.code === 'EFATAL' || error.code === 'ETIMEDOUT') {
                logger.error(`Bot ${this.instanceId} polling error, attempting recovery:`, error);
                await this.restartPolling();
            } else {
                logger.error(`Bot ${this.instanceId} polling error:`, error);
                await this.handleError(error);
            }
        });
    }

    wrapRequestWithRetry(originalRequest) {
        return async (...args) => {
            let lastError;
            for (let i = 0; i <= this.maxNetworkRetries; i++) {
                try {
                    const result = await originalRequest(...args);
                    if (i > 0) {
                        logger.info(`Bot ${this.instanceId} request succeeded after ${i} retries`);
                    }
                    return result;
                } catch (error) {
                    lastError = error;
                    if (i === this.maxNetworkRetries) {
                        throw error;
                    }
                    const delay = this.networkRetryDelay * Math.pow(2, i);
                    logger.warn(`Bot ${this.instanceId} request failed, retrying in ${delay}ms...`);
                    await this.sleep(delay);
                }
            }
            throw lastError;
        };
    }

    async initializeWithRetry() {
        let lastError;
        for (let i = 0; i <= this.maxNetworkRetries; i++) {
            try {
                const botInfo = await this.bot.getMe();
                this.botUsername = botInfo.username;
                await this.joinGroup();
                this.setupHandlers();
                await this.bot.startPolling();
                return;
            } catch (error) {
                lastError = error;
                if (i === this.maxNetworkRetries) {
                    throw error;
                }
                const delay = this.networkRetryDelay * Math.pow(2, i);
                logger.warn(`Bot ${this.instanceId} initialization failed, retrying in ${delay}ms...`);
                await this.sleep(delay);
            }
        }
        throw lastError;
    }

    async joinGroup() {
        let attempts = 0;
        while (attempts < this.maxRetries) {
            try {
                // First try to get chat info directly
                const chat = await this.bot.getChat(`@${this.groupName}`);
                this.chatId = chat.id;
                logger.info(`Bot ${this.instanceId} chat ID: ${this.chatId}`); // Log the chat ID
    
                // Get bot info
                const botInfo = await this.bot.getMe();
                this.botUsername = botInfo.username;
    
                // Verify bot membership
                const member = await this.bot.getChatMember(this.chatId, botInfo.id);
                logger.info(`Bot ${this.instanceId} member status: ${member.status}`); // Log member status
    
                if (!member || !['member', 'administrator'].includes(member.status)) {
                    logger.info(`Bot ${this.instanceId} (${this.botUsername}) not a member, attempting to join...`);
                    throw new Error('Bot must be manually added to the group');
                }
    
                logger.info(`Bot ${this.instanceId} (${this.botUsername}) successfully connected to group ${this.groupName}`);
                return;
    
            } catch (error) {
                attempts++;
                if (attempts >= this.maxRetries) {
                    logger.error(`Bot ${this.instanceId} failed to join group after ${attempts} attempts: ${error.message}`);
                    throw error;
                }
                
                logger.warn(`Bot ${this.instanceId} retry ${attempts}/${this.maxRetries} joining group...`);
                await this.sleep(this.retryDelay * attempts); // Exponential backoff
            }
        }
    }

    setupHandlers() {
        this.bot.on('message', (msg) => this.handleMessage(msg));
    }

    getCurrentSimulatedTime() {
        const now = new Date();
        const botOffset = (this.instanceId * 2) % 24;
        now.setHours((now.getHours() + botOffset) % 24);
        return now.getHours();
    }

    async sendRandomMessage() {
        // Set initial delay based on bot instance
        let initialDelay;
        if (this.instanceId <= 15) {
            if (this.instanceId <= 5) {
                // For the first 5 bots, use these fixed delays in minutes:
                // Bot1: 0, Bot2: 10, Bot3: 15, Bot4: 30, Bot5: 45
                const schedule = [0, 5, 8, 15, 27];
                initialDelay = schedule[this.instanceId - 1] * 60 * 1000;
            } else {
                // For bots 6 to 15, increase delay linearly from 45 to 90 minutes.
                const minDelay = 45, maxDelay = 90;
                const factor = (this.instanceId - 6) / (15 - 6);
                initialDelay = (minDelay + (maxDelay - minDelay) * factor) * 60 * 1000;
            }
        } else {
            // For instanceId > 15, a random delay between 2 and 20 minutes.
            const randomMinutes = Math.floor(Math.random() * (20 - 2 + 1)) + 2;
            initialDelay = randomMinutes * 60 * 1000;
        }
        logger.info(`Bot ${this.instanceId} waiting for initial delay of ${initialDelay/60000} minutes`);
        await this.sleep(initialDelay);

        while (this.running) {
            // Simulate human "think time"
            await this.sleep(3000); // 3-second delay
            try {
                if (!this.chatId) {
                    logger.error(`Bot ${this.instanceId} has no chat ID, attempting to rejoin group...`);
                    await this.joinGroup();
                    continue;
                }
                if (this.isBotSleeping()) {
                    logger.info(`Bot ${this.instanceId} is sleeping...`);
                    await this.sleep(3600 * 1000);
                    continue;
                }
                const recentMessages = await this.dbManager.getRecentMessages(3);
                const allMessages = [
                    ...(messages.generalChatter || []),
                    ...(messages.bullishHype || []),
                    ...(messages.casual || [])
                ];
                const availableMessages = allMessages.filter(msg =>
                    msg &&
                    !recentMessages.includes(msg) &&
                    !this.lastSentMessages.has(msg)
                );
                logger.info(`Bot ${this.instanceId} has ${availableMessages.length} new messages available`);
                if (availableMessages.length === 0) {
                    logger.info(`No new messages available. Sleeping for 10 minutes before retrying...`);
                    await this.sleep(10 * 60 * 1000);
                    continue;
                }
                const messageText = availableMessages[Math.floor(Math.random() * availableMessages.length)];
                this.lastSentMessages.add(messageText);
                if (this.lastSentMessages.size > 50) {
                    this.lastSentMessages.delete(this.lastSentMessages.values().next().value);
                }
                await this.bot.sendMessage(`@${this.groupName}`, messageText);
                this.lastMessageTime = Date.now();
                await this.dbManager.insertMessage("auto", messageText, `bot_${this.instanceId}`);
                logger.info(`Bot ${this.instanceId} sent message: ${messageText}`);
                // For subsequent messages, set random delay between 2 and 40 minutes.
                const delay = this.getRandomDelay(120, 2400);
                logger.info(`Bot ${this.instanceId} waiting for ${delay} seconds before next message`);
                await this.sleep(delay * 1000);
            } catch (error) {
                logger.error(`Bot ${this.instanceId} error in sendRandomMessage:`, error);
                await this.sleep(60000);
            }
        }
    }

    async handleMessage(msg) {
        try {
            // Only process text messages with a valid sender.
            if (!msg?.text || !msg?.from) return;
            // Do not reply to itself.
            if (msg.from.username === this.botUsername) return;
            
            const text = msg.text.toLowerCase();
            let response = null;
            let availableMessages = [];
            
            if (msg.from.is_bot) {
                // For bot-to-bot messages, use coordinated replies.
                availableMessages = messages.botCoordinatedReplies || [];
                const recentMessages = await this.dbManager.getRecentMessages(3);
                availableMessages = availableMessages.filter(reply =>
                    reply &&
                    !recentMessages.includes(reply) &&
                    !this.lastSentMessages.has(reply)
                );
                if (availableMessages.length === 0) return;
                response = availableMessages[Math.floor(Math.random() * availableMessages.length)];
            } else {
                // For human messages, scan common phrases.
                for (const [phrase, resp] of Object.entries(commonPhrases)) {
                    if (text.includes(phrase)) {
                        response = resp;
                        break;
                    }
                }
                if (!response) return;
            }
            
            // Delay reply (5-45 seconds) to mimic human timing.
            const replyDelay = Math.floor(Math.random() * 40000) + 5000;
            await this.sleep(replyDelay);
            
            await this.bot.sendMessage(msg.chat.id, response, { 
                reply_to_message_id: msg.message_id 
            });
            this.lastSentMessages.add(response);
            this.lastMessageTime = Date.now();
            await this.dbManager.insertMessage("reply", response, `bot_${this.instanceId}`);
        } catch (error) {
            logger.error(`Bot ${this.instanceId} message handling error:`, error);
        }
    }

    isBotSleeping() {
        const hour = this.getCurrentSimulatedTime();
        return hour >= this.sleepTime.start && hour < this.sleepTime.end;
    }

    randomSleepTime() {
        const start = Math.floor(Math.random() * 6) + 22;
        const end = (start + Math.floor(Math.random() * 6) + 3) % 24;
        return { start, end };
    }

    shouldReplyToMessage() {
        return Math.random() < this.replyChance;
    }

    getRandomDelay(min, max) {
        // Skew towards longer delays using exponential distribution
        const lambda = 1 / ((max - min) / 4);
        const randomValue = -Math.log(1 - Math.random()) / lambda;
        return Math.floor(Math.min(max, min + randomValue));
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async stop() {
        try {
            this.running = false;
            if (this.bot) {
                await this.bot.stopPolling();
                // Add small delay to ensure cleanup
                await this.sleep(1000);
                this.bot.removeAllListeners();
            }
            logger.info(`Bot ${this.instanceId} stopped successfully`);
        } catch (error) {
            logger.error(`Error stopping bot ${this.instanceId}:`, error);
        }
    }

    async restartPolling() {
        try {
            await this.bot.stopPolling();
            await this.sleep(5000);
            await this.bot.startPolling();
            logger.info(`Bot ${this.instanceId} polling restarted successfully`);
        } catch (error) {
            logger.error(`Bot ${this.instanceId} polling restart failed:`, error);
            await this.handleError(error);
        }
    }

    async handleError(error) {
        if (this.connectionRetries >= this.maxConnectionRetries) {
            logger.error(`Bot ${this.instanceId} max retries reached, stopping...`);
            await this.stop();
            process.exit(1);
            return;
        }

        this.connectionRetries++;
        const delay = this.connectionRetryDelay * this.connectionRetries;
        
        logger.info(`Bot ${this.instanceId} attempting reconnection in ${delay}ms (attempt ${this.connectionRetries}/${this.maxConnectionRetries})`);
        
        try {
            await this.sleep(delay);
            await this.bot.stopPolling();
            await this.initializeBot();
            this.connectionRetries = 0; // Reset counter on successful reconnect
            logger.info(`Bot ${this.instanceId} reconnected successfully`);
        } catch (error) {
            logger.error(`Bot ${this.instanceId} reconnection failed:`, error);
            await this.handleError(error); // Retry
        }
    }
}

// Main Entry Point
if (isMainThread) {
    // ... (main thread code remains the same)
} else {
    // Worker thread code
    const { token, groupName, dbPath, instanceId } = workerData;
    
    async function initializeWorker() {
        let dbManager = null;
        let bot = null;
        try {
            dbManager = new DatabaseManager(dbPath);
            await dbManager.connect();
            
            bot = new QuantikBot(token, groupName, dbManager, instanceId);
            global.currentBot = bot;

            // Add exponential backoff for retries
            let retryCount = 0;
            const maxRetries = 5;
            
            while (retryCount < maxRetries) {
                try {
                    await bot.sendRandomMessage();
                    break;
                } catch (error) {
                    retryCount++;
                    if (retryCount === maxRetries) {
                        throw error;
                    }
                    const delay = 10000 * Math.pow(2, retryCount);
                    logger.warn(`Worker ${instanceId} retry ${retryCount}/${maxRetries} in ${delay}ms`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
        } catch (error) {
            logger.error(`Worker ${instanceId} initialization error:`, error);
            if (dbManager) {
                await dbManager.close();
            }
            process.exit(1);
        }
    }

    initializeWorker().catch(error => {
        logger.error('Worker fatal error:', error);
        process.exit(1);
    });
}