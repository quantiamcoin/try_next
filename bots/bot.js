const TelegramBot = require('node-telegram-bot-api');
const sqlite3 = require('sqlite3').verbose();
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');
const winston = require('winston');

// Add logger configuration
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

// Move tokens to config file
const BOT_TOKENS = process.env.BOT_TOKENS ? 
    JSON.parse(process.env.BOT_TOKENS) : 
    require('./config.json').BOT_TOKENS;

// Bot Configuration
const GROUP_NAME = "meandsaint";
const DB_PATH = 'quantik_chat.db';

const { messages, commonPhrases, botConfig } = require('./data');


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

        this.lastGreetingDate = null;  // Track last greeting date

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

    needsGreeting() {
        const today = new Date().toDateString();
        return !this.lastGreetingDate || this.lastGreetingDate !== today;
    }

    async sendDailyGreeting() {
        const greeting = this.getGreeting();
        const greetingDelay = botConfig.typingSimulation.baseDelay + 
            greeting.length * botConfig.typingSimulation.perCharacterDelay;

        await this.bot.sendChatAction(this.chatId, 'typing');
        await this.sleep(greetingDelay);
        await this.bot.sendMessage(`@${this.groupName}`, greeting);
        await this.dbManager.insertMessage("greeting", greeting, `bot_${this.instanceId}`);
        
        this.lastGreetingDate = new Date().toDateString();
        logger.info(`Bot ${this.instanceId} sent daily greeting: ${greeting}`);
    }

    async sendRandomMessage() {
        // Initial delay
        let initialDelay;
        if (this.instanceId <= 15) {
            if (this.instanceId <= 5) {
                const schedule = [0, 3, 6, 12, 20];
                initialDelay = schedule[this.instanceId - 1] * 60 * 1000;
            } else {
                const minDelay = 30, maxDelay = 60;
                const factor = (this.instanceId - 6) / (15 - 6);
                initialDelay = (minDelay + (maxDelay - minDelay) * factor) * 60 * 1000;
            }
        } else {
            const randomMinutes = Math.floor(Math.random() * (20 - 2 + 1)) + 2;
            initialDelay = randomMinutes * 60 * 1000;
        }
        logger.info(`Bot ${this.instanceId} waiting for initial delay of ${initialDelay/60000} minutes`);
        await this.sleep(initialDelay);
    
        while (this.running) {
            // Simulate human "think time"
            await this.sleep(3000);
    
            try {
                if (!this.chatId) {
                    logger.error(`Bot ${this.instanceId} has no chat ID, attempting to rejoin group...`);
                    await this.joinGroup();
                    continue;
                }

                // Check if bot needs to send a greeting
                if (this.needsGreeting() && !this.isBotSleeping()) {
                    await this.sendDailyGreeting();
                    await this.sleep(5 * 60 * 1000); // Wait 5 minutes after greeting
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
    
                // Simulate typing behavior
                await this.bot.sendChatAction(this.chatId, 'typing');
                const typingDelay = Math.floor(Math.random() * (6000 - 2000 + 1)) + 2000; // 2-6 seconds delay
                await this.sleep(typingDelay);
    
                await this.bot.sendMessage(`@${this.groupName}`, messageText);
                this.lastMessageTime = Date.now();
                await this.dbManager.insertMessage("auto", messageText, `bot_${this.instanceId}`);
    
                logger.info(`Bot ${this.instanceId} sent message: ${messageText}`);
    
                // Random delay between 2 and 40 minutes for the next message
                const delay = this.getRandomDelay(2, 40) * 60 * 1000;
                await this.sleep(delay);
            } catch (error) {
                logger.error(`Bot ${this.instanceId} encountered an error while sending a message:`, error);
            }
        }
    }
    
    getContextualReply(message) {
        const text = message.toLowerCase();
        const { messageContexts, replyTemplates, sentimentPatterns } = require('./data');
        
        // Determine message context
        let context = 'casual';
        for (const [key, keywords] of Object.entries(messageContexts)) {
            if (keywords.some(keyword => text.includes(keyword))) {
                context = key;
                break;
            }
        }

        // Determine message sentiment and type
        const isQuestion = sentimentPatterns.question.some(p => text.includes(p));
        const isPositive = sentimentPatterns.positive.some(p => text.includes(p));
        
        // Get appropriate reply template
        const templates = replyTemplates[context] || {
            question: messages.casual,
            statement: messages.casual,
            positive: messages.casual,
            negative: messages.casual
        };

        // Select replies based on message type and sentiment
        let availableReplies;
        if (isQuestion) {
            availableReplies = templates.question;
        } else if (isPositive) {
            availableReplies = templates.positive;
        } else {
            availableReplies = templates.statement;
        }

        return availableReplies;
    }

    async handleMessage(msg) {
        try {
            if (!msg?.text || !msg?.from || msg.from.username === this.botUsername) return;
            
            const text = msg.text.toLowerCase();
            let response = null;
            
            // Enhanced bot-to-bot interaction
            if (msg.from.is_bot) {
                // Increased interaction chance (80%)
                if (Math.random() > 0.2) {
                    // Get contextual replies based on message content
                    const contextReplies = this.getContextualReply(text);
                    const recentMessages = await this.dbManager.getRecentMessages(3, `bot_${this.instanceId}`);
                    
                    // Filter available replies
                    const availableReplies = contextReplies.filter(reply =>
                        !recentMessages.includes(reply) &&
                        !this.lastSentMessages.has(reply)
                    );

                    if (availableReplies.length > 0) {
                        response = availableReplies[Math.floor(Math.random() * availableReplies.length)];
                        
                        // Simulate realistic typing
                        await this.bot.sendChatAction(msg.chat.id, 'typing');
                        const typingDelay = botConfig.typingSimulation.baseDelay + 
                            response.length * botConfig.typingSimulation.perCharacterDelay;
                        await this.sleep(typingDelay);
                        
                        await this.bot.sendMessage(msg.chat.id, response);
                        this.lastSentMessages.add(response);
                        this.lastMessageTime = Date.now();
                        await this.dbManager.insertMessage("contextual_reply", response, `bot_${this.instanceId}`);
                        return; // Ensure the function exits after bot-to-bot reply
                    }
                }
                return; // Ensure the function exits if no bot-to-bot reply is sent
            }

            // Human interaction with context
            if (commonPhrases[text]) {
                response = commonPhrases[text];
            } else {
                const contextReplies = this.getContextualReply(text);
                const recentMessages = await this.dbManager.getRecentMessages(3, `bot_${this.instanceId}`);
                const availableReplies = contextReplies.filter(reply => 
                    !recentMessages.includes(reply) &&
                    !this.lastSentMessages.has(reply)
                );
                
                if (availableReplies.length > 0) {
                    response = availableReplies[Math.floor(Math.random() * availableReplies.length)];
                }
            }

            if (response) {
                // Simulate realistic typing
                await this.bot.sendChatAction(msg.chat.id, 'typing');
                const typingDelay = botConfig.typingSimulation.baseDelay + 
                    response.length * botConfig.typingSimulation.perCharacterDelay;
                await this.sleep(typingDelay);
                
                await this.bot.sendMessage(msg.chat.id, response, {
                    reply_to_message_id: msg.message_id
                });
                this.lastSentMessages.add(response);
                this.lastMessageTime = Date.now();
                await this.dbManager.insertMessage("contextual_reply", response, `bot_${this.instanceId}`);
            }
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

    getGreeting() {
        const hour = this.getCurrentSimulatedTime();
        let greetings;
        
        if (hour >= 5 && hour < 12) {
            greetings = botConfig.greetingRules.morning;
        } else if (hour >= 12 && hour < 17) {
            greetings = botConfig.greetingRules.afternoon;
        } else if (hour >= 17 && hour < 21) {
            greetings = botConfig.greetingRules.evening;
        } else {
            greetings = botConfig.greetingRules.night;
        }
        
        return greetings[Math.floor(Math.random() * greetings.length)];
    }
}

// Main Entry Point
if (isMainThread) {
    const activeBots = new Map();
    
    async function cleanup() {
        logger.info('Cleaning up...');
        for (const [id, bot] of activeBots.entries()) {
            try {
                await bot.stop();
            } catch (error) {
                logger.error(`Error stopping bot ${id}:`, error);
            }
        }
        process.exit(0);
    }

    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);

    function shuffleArray(array) {
        return array.sort(() => Math.random() - 0.5);
    }

    const shuffledTokens = shuffleArray([...BOT_TOKENS]);

    shuffledTokens.forEach((token, index) => {
        try {
            const worker = new Worker(__filename, {
                workerData: { 
                    token, 
                    groupName: GROUP_NAME, 
                    dbPath: DB_PATH, 
                    instanceId: index + 1 
                }
            });

            worker.on('error', (error) => {
                logger.error(`Worker ${index + 1} error:`, error);
            });

            worker.on('exit', (code) => {
                if (code !== 0) {
                    logger.error(`Worker ${index + 1} exited with code ${code}`);
                }
            });

        } catch (error) {
            logger.error(`Error creating worker ${index + 1}:`, error);
        }
    });
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

    process.on('unhandledRejection', async (error) => {
        logger.error('Unhandled rejection:', error);
        try {
            if (global.currentBot) {
                await global.currentBot.stop();
            }
        } finally {
            process.exit(1);
        }
    });

    process.on('uncaughtException', async (error) => {
        logger.error('Uncaught exception:', error);
        try {
            // Attempt graceful shutdown
            if (global.currentBot) {
                await global.currentBot.stop();
            }
        } finally {
            process.exit(1);
        }
    });

    initializeWorker().catch(error => {
        logger.error('Worker fatal error:', error);
        process.exit(1);
    });
}
