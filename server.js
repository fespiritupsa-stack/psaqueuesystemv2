const express = require('express');
const path = require('path');
const http = require('http');
const os = require('os');
const { Server } = require('socket.io');
const { MongoClient, ObjectId } = require('mongodb');

// --- FIREBASE ADMIN SDK SETUP ---
const admin = require('firebase-admin');

let serviceAccount;

if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } catch (err) {
    console.error("❌ Failed to parse FIREBASE_SERVICE_ACCOUNT env variable as JSON:", err.message);
    process.exit(1);
  }
} else {
    try {
        serviceAccount = require('./serviceAccountKey.json');
    } catch (err) {
        console.error("❌ Firebase credentials missing! Provide FIREBASE_SERVICE_ACCOUNT env var or serviceAccountKey.json");
        process.exit(1);
    }
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const windowLocks = {};

// --- 1. CONFIGURATION ---
const PORT = process.env.PORT ? Number(process.env.PORT) : 3000; 
const uri = "mongodb://deynyelicawalo_db_user:h9sM5NYNeO0R96vw@ac-bbriiqg-shard-00-00.yeogezu.mongodb.net:27017,ac-bbriiqg-shard-00-01.yeogezu.mongodb.net:27017,ac-bbriiqg-shard-00-02.yeogezu.mongodb.net:27017/?ssl=true&replicaSet=atlas-uamnqz-shard-0&authSource=admin&appName=Cluster0";
const client = new MongoClient(uri);

let db;
let adminOTPs = {};
let requeueTimers = {};

// --- 2. GLOBAL QUEUE STATE ---
function createDefaultTicketSequences() {
    return {
        national: { regular: 1, priority: 1 },
        civil: { regular: 1, priority: 1 }
    };
}

function normalizeTicketSequences(savedSequences) {
    const normalized = createDefaultTicketSequences();
    const categories = ['regular', 'priority'];

    for (const dept of Object.keys(normalized)) {
        const savedDept = savedSequences?.[dept];

        if (typeof savedDept === 'number') {
            const nextNumber = Math.max(1, Math.floor(savedDept) || 1);
            normalized[dept].regular = nextNumber;
            normalized[dept].priority = nextNumber;
            continue;
        }

        if (savedDept && typeof savedDept === 'object') {
            for (const category of categories) {
                const numericValue = Number(savedDept[category]);
                normalized[dept][category] = Number.isFinite(numericValue) && numericValue > 0
                    ? Math.floor(numericValue)
                    : 1;
            }
        }
    }

    return normalized;
}

let ticketSequences = createDefaultTicketSequences();
let ticketSequenceDate = getTodayString();
let queueStore = {
    waiting: { national: [], civil: [] },
    currentServing: {
        national: { window1: null, window2: null, priorityWindow: null },
        civil: { window1: null, window2: null, priorityWindow: null }
    }
};
const activeUsers = {};

function getTodayString() {
    const now = new Date();
    return now.toISOString().slice(0, 10);
}

function buildDateFilter(dateString) {
    if (!dateString) return null;
    const [year, month, day] = dateString.split('-').map(Number);
    if (!year || !month || !day) return null;
    const start = new Date(Date.UTC(year, month - 1, day) - 8 * 3600000); // midnight PHT
    const end = new Date(start.getTime() + 24 * 3600000);
    return { iso_timestamp: { $gte: start, $lt: end } };
}

async function ensureDailyTicketReset() {
    const today = getTodayString();
    if (ticketSequenceDate !== today) {
        await performDailyReset();
        console.log(`🗓️ Daily ticket sequence reset: ${today} -> 001`);
    }
}

async function performDailyReset() {
    try {
        const today = getTodayString();
        console.log(`🕛 Performing nightly reset for ${today}...`);

        const cutoff = new Date(today);
        cutoff.setHours(0, 0, 0, 0);
        const logsToArchive = await db.collection('ticket_logs')
            .find({ iso_timestamp: { $lt: cutoff } }).toArray();
        if (logsToArchive.length > 0) {
            const archivedLogs = logsToArchive.map(l => ({ ...l, archivedAt: new Date() }));
            await db.collection('master_history').insertMany(archivedLogs);
            console.log(`✅ Archived ${logsToArchive.length} ticket log entries to master_history.`);
        }

        await db.collection('ticket_logs')
            .deleteMany({ iso_timestamp: { $lt: cutoff } });
        
        Object.values(requeueTimers).forEach(clearTimeout);
        requeueTimers = {};

        ticketSequences = createDefaultTicketSequences();
        ticketSequenceDate = today;
        queueStore.waiting = { national: [], civil: [] };
        queueStore.currentServing = {
            national: { window1: null, window2: null, priorityWindow: null },
            civil: { window1: null, window2: null, priorityWindow: null }
        };

        await saveCurrentState();
        io.emit('queue_update', { currentServing: queueStore.currentServing, waitingQueue: queueStore.waiting });
        io.emit('ticket_logs_updated');
        console.log('🧹 Midnight queue reset complete. Ticket sequence restarted at 001.');
    } catch (err) {
        console.error('Midnight reset failed:', err);
    }
}

async function resetTodayQueueCounters() {
    try {
        Object.values(requeueTimers).forEach(clearTimeout);
        requeueTimers = {};

        ticketSequences = createDefaultTicketSequences();
        ticketSequenceDate = getTodayString();
        queueStore.waiting = { national: [], civil: [] };
        queueStore.currentServing = {
            national: { window1: null, window2: null, priorityWindow: null },
            civil: { window1: null, window2: null, priorityWindow: null }
        };

        await saveCurrentState();
        io.emit('queue_update', { currentServing: queueStore.currentServing, waitingQueue: queueStore.waiting });
        io.emit('ticket_logs_updated');
        io.emit('master_history_updated');
        console.log('🔄 Today queue reset after same-day delete_range operation.');
    } catch (err) {
        console.error('Today queue reset failed:', err);
    }
}

function scheduleMidnightReset() {
    const now = new Date();
    const nextMidnight = new Date(now);
    nextMidnight.setHours(24, 0, 0, 0, 0);
    const delay = nextMidnight.getTime() - now.getTime();
    setTimeout(async () => {
        await performDailyReset();
        scheduleMidnightReset();
    }, delay);
}

// --- 3. PERSISTENCE & LOGGING HELPERS ---
async function logEvent(collectionName, data) {
    if (!db) return;
    try {
        const entry = {
            timestamp_readable: new Date().toLocaleString('en-PH'),
            iso_timestamp: new Date(),
            ...data
        };
        await db.collection(collectionName).insertOne(entry);
        if (collectionName === 'ticket_logs') {
            io.emit('ticket_logs_updated');
        }
    } catch (err) { console.error("Logging failed:", err); }
}

async function saveCurrentState() {
    if (!db) return;
    try {
        await db.collection('system_state').updateOne(
            { id: 'active_queue' },
            { $set: { queueStore, ticketSequences, ticketSequenceDate, lastUpdated: new Date() } },
            { upsert: true }
        );
    } catch (err) { console.error("Save state failed:", err); }
}

function cancelRequeueTimer(ticketLabel) {
    if (!ticketLabel) return;
    if (requeueTimers[ticketLabel]) {
        clearTimeout(requeueTimers[ticketLabel]);
        delete requeueTimers[ticketLabel];
    }
}

async function processExpiredRequeuedTickets() {
    const now = Date.now();
    let changed = false;
    const expiryMs = 600000;

    for (const dept of ['national', 'civil']) {
        const queue = queueStore.waiting[dept];

        for (let i = queue.length - 1; i >= 0; i--) {
            const ticket = queue[i];
            if (ticket?.requeueTime && now - ticket.requeueTime >= expiryMs) {
                queue.splice(i, 1);
                cancelRequeueTimer(ticket.label);
                await logEvent('ticket_logs', { label: ticket.label, department: dept, action: 'AUTO_TERMINATED_AFTER_REQUEUE', reason: '10 minute requeue timeout' });
                changed = true;
                console.log(`[AUTO-TERMINATE] Expired ticket ${ticket.label} removed from ${dept} waiting queue.`);
            }
        }
    }

    if (changed) {
        await saveCurrentState();
        io.emit('queue_update', { currentServing: queueStore.currentServing, waitingQueue: queueStore.waiting });
    }
}

setInterval(processExpiredRequeuedTickets, 30000); 

// --- 4. DATABASE CONNECTION ---
async function connectDB() {
    try {
        await client.connect();
        db = client.db('psa_queue_system');
        const saved = await db.collection('system_state').findOne({ id: 'active_queue' }); 
        if (saved) {
            queueStore = saved.queueStore || queueStore;
            ticketSequences = normalizeTicketSequences(saved.ticketSequences);
            ticketSequenceDate = saved.ticketSequenceDate || getTodayString();
            if (ticketSequenceDate !== getTodayString()) {
                await performDailyReset();
                console.log("🗓️ Daily ticket sequence reset after restart");
            }
            console.log("🔄 Persistent State Recovered");
        }
        console.log("✅ Connected to MongoDB Atlas: Archive & History Ready");
        scheduleMidnightReset();
    } catch (e) { console.error("❌ DB Failed:", e.message); process.exit(1); }
}
connectDB();

app.use(express.json());

app.get('/', (req, res) => {
    res.redirect('/login.html');
});

app.get('/stats_Dashboard.html', async (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'stats_Dashboard.html'));
});

app.use(express.static(path.join(__dirname, 'public')));

const departmentMap = {
    'national': 'National ID',
    'civil': 'Civil Registration'
};

const windowMap = {
    'window1': 'Window 1',
    'window2': 'Window 2',
    'priorityWindow': 'Priority Window',
};

function getDepartmentName(dept) {
    return departmentMap[dept] || dept;
}

function getWindowName(windowKey) {
    return windowMap[windowKey] || windowKey;
}

// --- 5. API ROUTES ---

app.post('/api/send-welcome-email', async (req, res) => {
    const { email, tempPassword, firstName } = req.body;

    const htmlContent = `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
            <h2 style="color: #2c5364; text-align: center;">Welcome to the PSA Queue System!</h2>
            <p>Hello ${firstName || 'Staff'},</p>
            <p>An administrator has created an account for you. Below are your temporary login credentials:</p>
            <div style="background-color: #f4f4f4; padding: 15px; border-radius: 5px; text-align: center; margin: 20px 0;">
                <p style="margin: 0; color: #333;"><strong>Email:</strong> ${email}</p>
                <p style="margin: 10px 0 0 0; color: #333;"><strong>Temporary Password:</strong> <span style="font-family: monospace; font-size: 1.2em; color: #d9534f;">${tempPassword}</span></p>
            </div>
            <p style="color: #666; font-size: 0.9em;"><em>For security reasons, please log in and change your password immediately.</em></p>
            <br>
            <p style="color: #333;">Best Regards,<br><strong>System Administrator</strong></p>
        </div>
    `;

    try {
        // PASTE YOUR GOOGLE SCRIPT WEB APP URL HERE:
        const scriptUrl = 'https://script.google.com/macros/s/AKfycbxfVjlbaPs6vZPItf_cXPzPcDVFhMDhLbZym3kvV4hb2eOOywa6o7ZLGNGPgLZZ64ztHw/exec'; 

        const response = await fetch(scriptUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                to: email,
                subject: 'Welcome to the PSA Queue System - Your Account Details',
                html: htmlContent
            })
        });

        const result = await response.json();
        
        if (result.success) {
            res.json({ success: true });
        } else {
            console.error("Google Script Error:", result.error);
            res.status(500).json({ success: false, error: 'Failed to trigger email via Google' });
        }
    } catch (error) {
        console.error("Bridge connection failed:", error);
        res.status(500).json({ success: false, error: 'Network error reaching email bridge' });
    }
});


// PRE-FLIGHT CHECK: Duplicate validation
app.post('/api/check-user-exists', async (req, res) => {
    try {
        const { email, employeeId } = req.body;
        
        const existingEmail = await db.collection('employee_accounts').findOne({ email: email });
        if (existingEmail) return res.json({ exists: true, field: 'Email' });
        
        const existingEmpId = await db.collection('employee_accounts').findOne({ employeeId: employeeId });
        if (existingEmpId) return res.json({ exists: true, field: 'Employee ID' });
        
        res.json({ exists: false });
    } catch (err) {
        res.status(500).json({ error: 'Database validation error' });
    }
});

app.get('/api/system-logs/roles', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database offline" });
        const roles = await db.collection('employee_accounts').find({}).toArray();
        res.json({ success: true, roles });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/api/get-role', async (req, res) => {
    try {
        const email = req.body.email;
        let user = await db.collection('employee_accounts').findOne({ email: email });
        
        const masterAdmins = ['akosideynyel@gmail.com', 'marviccanque@gmail.com', 'paultimothy1477@gmail.com']; 
        
        // FIX: Default to 'unauthorized' if they don't exist in DB to prevent deleted Firebase accounts from logging in.
        let finalRole = 'unauthorized';

        if (masterAdmins.includes(email)) {
            finalRole = 'admin';
            if (!user) {
                await db.collection('employee_accounts').insertOne({
                    email: email,
                    role: 'admin',
                    createdAt: new Date(),
                    updatedAt: new Date()
                });
            } else if (user.role !== 'admin') {
                await db.collection('employee_accounts').updateOne(
                    { email: email },
                    { $set: { role: 'admin', updatedAt: new Date() } }
                );
            }
        } else if (user) {
            finalRole = user.role;
        }

        res.json({ success: true, role: finalRole });
    } catch (error) {
        console.error('Role fetch error:', error);
        res.status(500).json({ success: false, error: 'Failed to retrieve role' });
    }
});

// --- GET EMPLOYEE PROFILE ENDPOINT (WITH LOGS) ---
app.get('/api/get-employee', async (req, res) => {
    try {
        console.log(`[SERVER] /api/get-employee requested by: ${req.query.email}`);
        
        if (!db) {
            console.log(`[SERVER] ERROR: Database offline`);
            return res.status(500).json({ success: false, message: "Database offline" });
        }
        
        const userEmail = req.query.email;
        if (!userEmail) return res.status(400).json({ success: false, message: "Email is required" });

        const employee = await db.collection('employee_accounts').findOne({ email: userEmail });

        if (employee) {
            console.log(`[SERVER] Success! Found profile for: ${userEmail}`);
            res.json({ success: true, employee: employee });
        } else {
            console.log(`[SERVER] User not found in database: ${userEmail}`);
            res.json({ success: false, message: "User not found" });
        }
    } catch (error) {
        console.error("[SERVER] Error fetching employee:", error);
        res.status(500).json({ success: false, message: "Server error" });
    }
});

app.post('/api/set-role', async (req, res) => {
    await db.collection('employee_accounts').updateOne(
        { email: req.body.email }, 
        { 
            $set: { ...req.body, updatedAt: new Date() },
            $setOnInsert: { createdAt: new Date() }
        }, 
        { upsert: true }
    );
    res.json({ success: true });
});

app.post('/api/ban-user', async (req, res) => {
    try {
        const { email } = req.body;
        await db.collection('employee_accounts').updateOne(
            { email: email },
            { $set: { role: 'banned', updatedAt: new Date() } }
        );
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// --- UPDATED DELETE USER ROUTE ---
app.delete('/api/delete-user/:email', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database offline" });
        const emailToDelete = req.params.email;
        
        try {
            const userRecord = await admin.auth().getUserByEmail(emailToDelete);
            await admin.auth().deleteUser(userRecord.uid);
            console.log(`[SERVER] Successfully deleted user from Firebase Auth: ${emailToDelete}`);
        } catch (firebaseErr) {
            console.error(`[SERVER] Error deleting user from Firebase Auth:`, firebaseErr.message);

        }

        await db.collection('employee_accounts').deleteOne({ email: emailToDelete });
        
        io.emit('force_logout_signal', emailToDelete);

        res.json({ success: true });
    } catch (err) {
        console.error(`[SERVER] Failed to delete user:`, err.message);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/api/request-admin-otp', (req, res) => {
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    adminOTPs[req.body.email] = otp;
    console.log(`\n🔑 ADMIN OTP for ${req.body.email}: ${otp}\n`);
    res.json({ success: true });
});

app.post('/api/verify-admin-otp', (req, res) => {
    const { email, otp } = req.body;
    if (adminOTPs[email] === otp) { delete adminOTPs[email]; res.json({ success: true }); }
    else { res.json({ success: false, error: "Invalid OTP." }); }
});

app.get('/api/system-logs/:folder', async (req, res) => {
    try {
        const collectionName = req.params.folder;
        const filter = {};
        if (req.query.department) filter.department = req.query.department;
        
        const dateFilter = buildDateFilter(req.query.date);
        if (dateFilter) Object.assign(filter, dateFilter);
        
        if (collectionName === 'ticket_logs') {
            const [liveLogs, historyLogs] = await Promise.all([
                db.collection('ticket_logs').find(filter).sort({ iso_timestamp: -1 }).toArray(),
                db.collection('master_history').find(filter).sort({ iso_timestamp: -1 }).toArray()
            ]);
            const combinedLogs = [...liveLogs, ...historyLogs].sort((a, b) => {
                const aTime = new Date(a.iso_timestamp || a.archivedAt || 0).getTime();
                const bTime = new Date(b.iso_timestamp || b.archivedAt || 0).getTime();
                return bTime - aTime;
            });
            const deduplicatedLogs = deduplicateLogs(combinedLogs);
            return res.json({ success: true, logs: deduplicatedLogs, collection: 'combined' });
        }
        
        const logs = await db.collection(collectionName).find(filter).sort({ iso_timestamp: -1 }).toArray();
        res.json({ success: true, logs, collection: collectionName });
    } catch (err) { res.status(500).json({ success: false }); }
});

app.delete('/api/system-logs/:folder/:id', async (req, res) => {
    try {
        await db.collection(req.params.folder).deleteOne({ _id: new ObjectId(req.params.id) });
        io.emit('ticket_logs_updated');
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/system-logs/delete_range', async (req, res) => {
    try {
        const { start, end, collections } = req.body || {};
        if (!start || !end) return res.status(400).json({ success: false, error: 'start and end are required in ISO format' });
        const startDate = new Date(start);
        const endDate = new Date(end);
        if (isNaN(startDate.getTime()) || isNaN(endDate.getTime()) || startDate >= endDate) return res.status(400).json({ success: false, error: 'invalid date range' });

        const targets = Array.isArray(collections) && collections.length > 0 ? collections : ['ticket_logs', 'master_history'];
        const filter = { iso_timestamp: { $gte: startDate, $lt: endDate } };

        const results = {};
        for (const col of targets) {
            try {
                const r = await db.collection(col).deleteMany(filter);
                results[col] = r.deletedCount;
            } catch (e) {
                results[col] = 'error';
            }
        }

        const todayStart = new Date();
        todayStart.setHours(0, 0, 0, 0);
        const tomorrowStart = new Date(todayStart);
        tomorrowStart.setDate(todayStart.getDate() + 1);
        const rangeIncludesToday = startDate < tomorrowStart && endDate > todayStart;

        if (rangeIncludesToday && targets.includes('ticket_logs')) {
            await resetTodayQueueCounters();
        }

        io.emit('ticket_logs_updated');
        io.emit('master_history_updated');
        res.json({ success: true, deleted: results, resetTodayQueue: rangeIncludesToday && targets.includes('ticket_logs') });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/api/log-user-logout', async (req, res) => {
    try {
        const { email, userId } = req.body;
        await logEvent('auth_logs', {
            email: email || 'Unknown',
            userId: userId || email || 'Unknown',
            action: 'LOGOUT',
            status: 'logout',
            windows: 'N/A'
        });
        io.emit('user_account_logs_updated');
        res.json({ success: true });
    } catch (err) {
        console.error('Logout log failed:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.get('/api/ticket-statistics', async (req, res) => {
    try {
        if (!db) return res.json({ success: false, error: "Database not connected" });

        const now = new Date();
        const weekMs = 7 * 24 * 60 * 60 * 1000;
        const oldestWindow = new Date(now.getTime() - (5 * weekMs));
        const dateFilter = { iso_timestamp: { $gte: oldestWindow } };

        const [ticketLogs, masterLogs] = await Promise.all([
            db.collection('master_history').find({ action: 'ISSUED', ...dateFilter }).toArray()
        ]);

        const allLogs = [...ticketLogs, ...masterLogs];
        const logs = deduplicateLogs(allLogs);
        const weekData = [];

        for (let i = 5; i >= 0; i--) {
            const weekEnd = new Date(now.getTime() - (i * weekMs));
            const weekStart = new Date(weekEnd.getTime() - weekMs);
            
            weekData.push({
                start: weekStart,
                end: weekEnd,
                label: `${weekStart.toLocaleDateString(undefined, {month:'short', day:'numeric'})} - ${weekEnd.toLocaleDateString(undefined, {month:'short', day:'numeric'})}`,
                national: 0,
                civil: 0
            });
        }

        logs.forEach(log => {
            const timestamp = log.iso_timestamp ? new Date(log.iso_timestamp) : new Date(log.timestamp || log.timestamp_readable || log.archivedAt || Date.now());
            const logTime = timestamp.getTime();
            const bucket = weekData.find(w => logTime >= w.start.getTime() && logTime <= w.end.getTime());
            if (bucket) {
                if (log.department === 'national') bucket.national++;
                else if (log.department === 'civil') bucket.civil++;
            }
        });

        const labels = weekData.map(w => w.label);
        const national = weekData.map(w => w.national);
        const civil = weekData.map(w => w.civil);

        res.json({ success: true, weekLabels: labels, national, civil });
    } catch (err) { 
        res.status(500).json({ success: false, error: err.message }); 
    }
});

app.get('/api/daily-transactions', async (req, res) => {
    try {
        if (!db) return res.json({ success: false, error: "Database not connected" });
        const requestedDate = req.query.date || getTodayString();
        const dateFilter = buildDateFilter(requestedDate) || buildDateFilter(getTodayString());
        const filter = { action: 'ISSUED', ...(dateFilter || {}) };
        
        const liveOnly = req.query.liveOnly === 'true';

        if (liveOnly) {
            // Only retrieve from ticket_logs (live data)
            const logs = await db.collection('ticket_logs').find(filter).toArray();      
            let total = 0, national = 0, civil = 0;
            logs.forEach(log => {
                total++;
                if (log.department === 'national') national++;
                else if (log.department === 'civil') civil++;
            });
            return res.json({ success: true, total, national, civil });
        }

        const [ticketLogs, masterHistory] = await Promise.all([
            db.collection('ticket_logs').find(filter).toArray(),
            db.collection('master_history').find(filter).toArray()
        ]);

        const allLogs = deduplicateLogs([...ticketLogs, ...masterHistory]);
          
        let total = 0, national = 0, civil = 0;
        allLogs.forEach(log => {
            total++;
            if (log.department === 'national') national++;
            else if (log.department === 'civil') civil++;
        });
        
        res.json({ success: true, total, national, civil });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/national-id-ticket-categories', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database not connected" });
        const requestedDate = req.query.date || getTodayString();
        const dateFilter = buildDateFilter(requestedDate) || buildDateFilter(getTodayString());
        const query = { department: 'national', action: 'ISSUED', ...(dateFilter || {}) };
        
        const liveOnly = req.query.liveOnly === 'true';

        if (liveOnly) {
            const tickets = await db.collection('ticket_logs').find(query).toArray();
            let regularCount = 0, priorityCount = 0;
            tickets.forEach(ticket => {
                if (ticket.isPriority === true || ticket.isPriority === 'true') priorityCount++;
                else regularCount++;
            });
            return res.json({ success: true, regular: regularCount, priority: priorityCount });
        }

        const [liveTickets, historyTickets] = await Promise.all([
            db.collection('ticket_logs').find(query).toArray(),
            db.collection('master_history').find(query).toArray()
        ]);
        
        const nationalIdTickets = deduplicateLogs([...liveTickets, ...historyTickets]);

        let regularCount = 0, priorityCount = 0;
        nationalIdTickets.forEach(ticket => {
            if (ticket.isPriority === true || ticket.isPriority === 'true') priorityCount++;
            else regularCount++;
        });

        res.json({ success: true, regular: regularCount, priority: priorityCount });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/civil-registration-ticket-categories', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database not connected" });
        const requestedDate = req.query.date || getTodayString();
        const dateFilter = buildDateFilter(requestedDate) || buildDateFilter(getTodayString());
        const query = { department: 'civil', action: 'ISSUED', ...(dateFilter || {}) };
        
        const liveOnly = req.query.liveOnly === 'true';

        if (liveOnly) {
            const tickets = await db.collection('ticket_logs').find(query).toArray();
            let regular = 0, priority = 0;
            tickets.forEach(t => { 
                if (t.isPriority === true || t.isPriority === 'true') priority++; 
                else regular++; 
            });
            return res.json({ success: true, regular, priority });
        }

        const [liveTickets, historyTickets] = await Promise.all([
            db.collection('ticket_logs').find(query).toArray(),
            db.collection('master_history').find(query).toArray()
        ]);

        const tickets = deduplicateLogs([...liveTickets, ...historyTickets]);
        
        let regular = 0, priority = 0;
        tickets.forEach(t => { 
            if (t.isPriority === true || t.isPriority === 'true') priority++; 
            else regular++; 
        });
        res.json({ success: true, regular, priority });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/civil-registration-ticket-status-summary', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database not connected" });
        const requestedDate = req.query.date || getTodayString();
        const dateFilter = buildDateFilter(requestedDate) || buildDateFilter(getTodayString());
        const match = { department: 'civil', action: { $in: ['COMPLETED', 'TERMINATED', 'REQUEUED'] }, ...(dateFilter || {}) };
        
        const liveOnly = req.query.liveOnly === 'true';

        if (liveOnly) {
            // Only retrieve from ticket_logs (live data)
            const docs = await db.collection('ticket_logs').find(match).toArray();
            const summaryMap = { COMPLETED: 0, TERMINATED: 0, REQUEUED: 0 };
            docs.forEach(doc => { if (summaryMap[doc.action] !== undefined) summaryMap[doc.action]++; });
            return res.json({
                success: true,
                completed: summaryMap['COMPLETED'],
                terminated: summaryMap['TERMINATED'],
                requeued: summaryMap['REQUEUED']
            });
        }

        // Original behavior: combine both collections
        const [liveDocs, historyDocs] = await Promise.all([
            db.collection('ticket_logs').find(match).toArray(),
            db.collection('master_history').find(match).toArray()
        ]);

        const allDocs = deduplicateLogs([...liveDocs, ...historyDocs]);
        const summaryMap = { COMPLETED: 0, TERMINATED: 0, REQUEUED: 0 };
        allDocs.forEach(doc => { if (summaryMap[doc.action] !== undefined) summaryMap[doc.action]++; });
        
        res.json({ 
            success: true, 
            completed: summaryMap['COMPLETED'], 
            terminated: summaryMap['TERMINATED'], 
            requeued: summaryMap['REQUEUED'] 
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/national-id-ticket-status-summary', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database not connected" });
        const requestedDate = req.query.date || getTodayString();
        const dateFilter = buildDateFilter(requestedDate) || buildDateFilter(getTodayString());
        const match = { department: 'national', action: { $in: ['COMPLETED', 'TERMINATED', 'REQUEUED'] }, ...(dateFilter || {}) };
        
        const liveOnly = req.query.liveOnly === 'true';

        if (liveOnly) {
            const docs = await db.collection('ticket_logs').find(match).toArray();
            const summaryMap = { COMPLETED: 0, TERMINATED: 0, REQUEUED: 0 };
            docs.forEach(doc => { if (summaryMap[doc.action] !== undefined) summaryMap[doc.action]++; });
            return res.json({ 
                success: true, 
                completed: summaryMap['COMPLETED'], 
                terminated: summaryMap['TERMINATED'], 
                requeued: summaryMap['REQUEUED'] 
            });
        }

        // Original behavior: combine both collections
        const [liveDocs, historyDocs] = await Promise.all([
            db.collection('ticket_logs').find(match).toArray(),
            db.collection('master_history').find(match).toArray()
        ]);

        const allDocs = deduplicateLogs([...liveDocs, ...historyDocs]);
        const summaryMap = { COMPLETED: 0, TERMINATED: 0, REQUEUED: 0 };
        allDocs.forEach(doc => { if (summaryMap[doc.action] !== undefined) summaryMap[doc.action]++; });
        
        res.json({ 
            success: true, 
            completed: summaryMap['COMPLETED'], 
            terminated: summaryMap['TERMINATED'], 
            requeued: summaryMap['REQUEUED'] 
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

function deduplicateLogs(logs) {
    const seen = new Map();
    const deduplicated = [];
    logs.forEach(log => {
        const key = `${log.iso_timestamp || ''}|${log.label || ''}|${log.department || ''}`;
        if (!seen.has(key)) {
            seen.set(key, true);
            deduplicated.push(log);
        }
    });
    return deduplicated;
}

app.get('/api/ticket-logs-range', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database offline" });

        const { start, end, department } = req.query;
        if (!start || !end) return res.status(400).json({ success: false, error: "start and end required" });

        const startDate = new Date(start);
        startDate.setHours(0, 0, 0, 0);
        const endDate = new Date(end);
        endDate.setHours(23, 59, 59, 999);

        const filter = { iso_timestamp: { $gte: startDate, $lte: endDate } };
        if (department) filter.department = department;

        const [live, history] = await Promise.all([
            db.collection('ticket_logs').find(filter).toArray(),
            db.collection('master_history').find(filter).toArray()
        ]);

        const logs = deduplicateLogs([...live, ...history]);
        res.json({ success: true, logs, count: logs.length });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// New endpoint for yearly aggregated ticket statistics (combines both collections, resets yearly)
app.get('/api/yearly-ticket-aggregate', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, error: "Database offline" });

        const currentYear = new Date().getFullYear();
        const yearStart = new Date(currentYear, 0, 1);
        const yearEnd = new Date(currentYear, 11, 31, 23, 59, 59, 999);

        const filter = {
            action: 'ISSUED',
            iso_timestamp: { $gte: yearStart, $lte: yearEnd }
        };

        const [live, history] = await Promise.all([
            db.collection('ticket_logs').find(filter).toArray(),
            db.collection('master_history').find(filter).toArray()
        ]);

        const logs = deduplicateLogs([...live, ...history]);

        // Aggregate by month and department
        const monthlyData = {};
        for (let month = 1; month <= 12; month++) {
            monthlyData[month] = { national: 0, civil: 0 };
        }

        logs.forEach(log => {
            const logDate = new Date(log.iso_timestamp || log.archivedAt || new Date());
            const month = logDate.getMonth() + 1;
            
            if (log.department === 'national') {
                monthlyData[month].national++;
            } else if (log.department === 'civil') {
                monthlyData[month].civil++;
            }
        });

        // Format response
        const monthlyNational = Array.from({ length: 12 }, (_, i) => monthlyData[i + 1].national);
        const monthlyCivil = Array.from({ length: 12 }, (_, i) => monthlyData[i + 1].civil);

        res.json({ 
            success: true, 
            year: currentYear,
            monthlyNational, 
            monthlyCivil, 
            totalLogs: logs.length 
        });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// --- 6. SOCKET.IO ---
io.on('connection', (socket) => {
    const uniqueActiveUsers = Array.from(new Map(Object.values(activeUsers).map(u => [u.email, u])).values());
    socket.emit('active_users_list', uniqueActiveUsers);

    socket.on('register_active_user', async (data) => {
        activeUsers[socket.id] = { email: data.email, role: data.role, location: data.location, lastSeen: new Date() };
        await logEvent('auth_logs', { email: data.email, action: 'LOGIN', location: data.location });
        broadcastActiveUsers();
    });

    socket.on('register_controller_window', (data) => {
        const { dept, windowKey, email } = data;
        const lockKey = `${dept}_${windowKey}`;

        if (windowLocks[lockKey] && windowLocks[lockKey] !== email) {
            socket.emit('window_lock_error', { message: `The ${getWindowName(windowKey)} in ${getDepartmentName(dept)} is already controlled by another staff.` });
            return;
        } else {
            windowLocks[lockKey] = email;
        }
    });

    const syncState = () => io.emit('queue_update', { currentServing: queueStore.currentServing, waitingQueue: queueStore.waiting });
    
    const performSystemReset = async () => {
        try {
            const logs = await db.collection('ticket_logs').find({}).toArray();
            if (logs.length > 0) {
                const archivedLogs = logs.map(l => ({ ...l, archivedAt: new Date() }));
                await db.collection('master_history').insertMany(archivedLogs);
            }

            await db.collection('ticket_logs').deleteMany({});
            Object.values(requeueTimers).forEach(clearTimeout);
            requeueTimers = {};

            ticketSequences = createDefaultTicketSequences();
            ticketSequenceDate = getTodayString();
            queueStore.waiting = { national: [], civil: [] };
            queueStore.currentServing = {
                national: { window1: null, window2: null, priorityWindow: null },
                civil: { window1: null, window2: null, priorityWindow: null }
            };

            await saveCurrentState();
            syncState();
            io.emit('ticket_logs_updated');
            socket.emit('reset_queues_success');
            socket.emit('reset_Dailyqueues_success'); 
        } catch (err) { console.error("Reset Failed:", err); }
    };

    socket.on('reset_system', performSystemReset);
    socket.on('reset_queues', performSystemReset);
    
    socket.on('reset_Dailyqueues', async () => {
        try {
            const todayStart = new Date();
            todayStart.setHours(0, 0, 0, 0);
            const tomorrowStart = new Date(todayStart);
            tomorrowStart.setDate(todayStart.getDate() + 1);

            const delFilter = { iso_timestamp: { $gte: todayStart, $lt: tomorrowStart } };
            if (db) {
                const res = await db.collection('ticket_logs').deleteMany(delFilter);
                console.log(`🗑️ Deleted ${res.deletedCount} ticket_logs entries for today.`);
            }

            await resetTodayQueueCounters();
            socket.emit('reset_Dailyqueues_success');
            io.emit('ticket_logs_updated');
            syncState();
        } catch (err) { console.error("Daily reset failed:", err); }
    });

    socket.on('repeat_voice', (data) => {
        if (!data || !data.ticketNumber) return;
        io.emit('repeat_voice', {
            ticketNumber: data.ticketNumber,
            windowName: data.windowName || 'Designated Window'
        });
    });

    socket.on('request_queue_update', () => {
        socket.emit('queue_update', {
            currentServing: queueStore.currentServing,
            waitingQueue: queueStore.waiting
        });
    });

    socket.on('issue_ticket', async (data) => {
        try {
            const { dept, isPriority } = data || {};
            if (!dept || !['national', 'civil'].includes(dept)) {
                socket.emit('ticket_error', { error: 'Invalid department' });
                return;
            }

            const prefixes = { national: 'NID', civil: 'CR' };
            const prefix = prefixes[dept] || 'TKT';
            const typeChar = isPriority ? 'P' : 'R';
            const sequenceKey = isPriority ? 'priority' : 'regular';
            ticketSequences = normalizeTicketSequences(ticketSequences);
            const nextSequenceNumber = ticketSequences[dept][sequenceKey] || 1;
            const number = String(nextSequenceNumber).padStart(3, '0');
            const label = `${prefix}-${typeChar}-${number}`;
            const typeCharDept = isPriority ? 'Priority' : 'Regular';

            const ticket = {
                id: Date.now().toString() + Math.floor(Math.random() * 1000),
                label,
                department: dept,
                isPriority: !!isPriority,
                customerType: typeCharDept,
                status: 'WAITING',
                issuedTime: new Date(),
                iso_timestamp: new Date(),
                timestamp_readable: new Date().toLocaleString('en-PH')
            };

            queueStore.waiting[dept].push(ticket);
            ticketSequences[dept][sequenceKey] = nextSequenceNumber + 1;

            await Promise.all([
                logEvent('ticket_logs', { ...ticket, action: 'ISSUED' }),
                saveCurrentState()
            ]);

            socket.emit('ticket_assigned', { label: ticket.label, dept: ticket.department, isPriority: ticket.isPriority, customerType: ticket.customerType });
            if (process.env.DEBUG_TICKETS === '1') console.log(`ISSUED ${ticket.label} -> ${dept} priority=${ticket.isPriority}`);
            await io.emit('ticket_logs_updated');
            syncState();
        } catch (err) {
            socket.emit('ticket_error', { error: 'Server error creating ticket' });
        }
    });

    socket.on('assign_ticket', async (data) => {
        const { dept, window, ticketId } = data;
        const index = queueStore.waiting[dept].findIndex(t => t.id === ticketId);
        if (index !== -1 && !queueStore.currentServing[dept][window]) {
            const ticket = queueStore.waiting[dept].splice(index, 1)[0];
            cancelRequeueTimer(ticket.label);
            delete ticket.requeueTime;
            ticket.status = 'SERVING';
            ticket.window = window;
            queueStore.currentServing[dept][window] = ticket;
            
            await logEvent('ticket_logs', { ...ticket, action: 'ASSIGNED', window });
            await saveCurrentState();
            await io.emit('ticket_logs_updated');
            syncState();
        }
    });

    socket.on('complete_ticket', async (data) => {
        const { dept, window } = data;
        const ticket = queueStore.currentServing[dept][window];
        if (ticket) {
            cancelRequeueTimer(ticket.label);

            io.emit('stop_voice', { ticketLabel: ticket.label });

            await logEvent('ticket_logs', { label: ticket.label, department: ticket.department, window, action: 'COMPLETED' });
            queueStore.currentServing[dept][window] = null;
            await saveCurrentState();
            syncState();
        }
    });

    socket.on('terminate_ticket', async (data) => {
        const { dept, window } = data;
        const ticket = queueStore.currentServing[dept][window];
        if (ticket) {
            cancelRequeueTimer(ticket.label);

            io.emit('stop_voice', { ticketLabel: ticket.label });

            await logEvent('ticket_logs', { label: ticket.label, department: ticket.department, window, action: 'TERMINATED' });
            queueStore.currentServing[dept][window] = null;
            await saveCurrentState();
            syncState();
        }
    });

    socket.on('requeue_ticket', async (data) => {
        const { dept, window } = data;
        const ticket = queueStore.currentServing[dept][window];
        if (ticket) {
            if (ticket.requeueCount >= 1) {
                socket.emit('ticket_error', { error: 'Ticket may only be requeued once' });
                return;
            }

            ticket.status = 'WAITING';
            delete ticket.window;
            ticket.requeueTime = Date.now();
            ticket.requeueCount = (ticket.requeueCount || 0) + 1;
            queueStore.waiting[dept].push(ticket);

            io.emit('stop_voice', { ticketLabel: ticket.label });
            
            await logEvent('ticket_logs', { label: ticket.label, department: ticket.department, window, action: 'PENDING_REQUEUE' });
            queueStore.currentServing[dept][window] = null;
            await saveCurrentState();
            syncState();

            const ticketLabel = ticket.label;
            if (requeueTimers[ticketLabel]) clearTimeout(requeueTimers[ticketLabel]);
            requeueTimers[ticketLabel] = setTimeout(async () => {
                const deptQueues = queueStore.waiting[dept];
                const index = deptQueues.findIndex(t => t.label === ticketLabel);
                if (index !== -1) {
                    deptQueues.splice(index, 1);
                    await logEvent('ticket_logs', { label: ticketLabel, department: dept, action: 'AUTO_TERMINATED_AFTER_REQUEUE', reason: '10 minute requeue timeout' });
                    await saveCurrentState();
                    syncState();
                }
                delete requeueTimers[ticketLabel];
            }, 600000); 
        }
    });

    socket.on('manual_requeue_waiting_ticket', async (data) => {
        const { dept, ticketId } = data;
        if (!dept || !['national', 'civil'].includes(dept)) return;

        const queue = queueStore.waiting[dept];
        const index = queue.findIndex(t => t.id === ticketId);
        if (index !== -1) {
            const ticket = queue[index];
            if (ticket.requeueCount >= 1) {
                socket.emit('ticket_error', { error: 'Ticket may only be requeued once' });
                return;
            }

            const requeueTicket = queue.splice(index, 1)[0];
            cancelRequeueTimer(requeueTicket.label);
            requeueTicket.requeueTime = Date.now();
            requeueTicket.requeueCount = (requeueTicket.requeueCount || 0) + 1;
            queue.push(requeueTicket);
            
            await logEvent('ticket_logs', { label: requeueTicket.label, department: dept, action: 'MANUALLY_REQUEUED', reason: 'Staff requeue' });
            await saveCurrentState();
            syncState();
            
            const ticketLabel = requeueTicket.label;
            if (requeueTimers[ticketLabel]) clearTimeout(requeueTimers[ticketLabel]);
            requeueTimers[ticketLabel] = setTimeout(async () => {
                const deptQueues = queueStore.waiting[dept];
                const idx = deptQueues.findIndex(t => t.label === ticketLabel);
                if (idx !== -1) {
                    deptQueues.splice(idx, 1);
                    await logEvent('ticket_logs', { label: ticketLabel, department: dept, action: 'AUTO_TERMINATED_AFTER_REQUEUE', reason: '10 minute requeue timeout' });
                    await saveCurrentState();
                    syncState();
                }
                delete requeueTimers[ticketLabel];
            }, 600000);
        }
    });

    socket.on('acknowledge_requeue_ticket', async (data) => {
        const { dept, ticketId } = data;
        if (!dept || !['national', 'civil'].includes(dept)) return;

        const queue = queueStore.waiting[dept];
        const ticket = queue.find(t => t.id === ticketId);
        if (ticket) {
            await logEvent('ticket_logs', { label: ticket.label, department: dept, action: 'REQUEUED', reason: 'Requeue acknowledged by staff' });
            await saveCurrentState();
            syncState();
        }
    });

    socket.on('manual_terminate_waiting_ticket', async (data) => {
        const { dept, ticketId } = data;
        if (!dept || !['national', 'civil'].includes(dept)) return;

        const queue = queueStore.waiting[dept];
        const index = queue.findIndex(t => t.id === ticketId);
        if (index !== -1) {
            const ticket = queue.splice(index, 1)[0];
            cancelRequeueTimer(ticket.label);
            await logEvent('ticket_logs', { label: ticket.label, department: dept, action: 'TERMINATED', reason: 'Manual termination by staff' });
            await saveCurrentState();
            syncState();
        }
    });

    socket.on('disconnect', () => {
        const userSession = activeUsers[socket.id];
        if (userSession) {
            for (const [lockKey, owner] of Object.entries(windowLocks)) {
                if (owner === userSession.email) {
                    delete windowLocks[lockKey];
                }
            }
        }
        delete activeUsers[socket.id];
        broadcastActiveUsers();
    });

    const broadcastActiveUsers = () => {
        const unique = Array.from(new Map(Object.values(activeUsers).map(u => [u.email, u])).values());
        io.emit('active_users_list', unique);
    };
});

const HOST = process.env.HOST || '0.0.0.0';

console.log("\n=======================================================");
console.log("🚀 BOOTING UP: V2 WITH PROFILE API");
console.log("=======================================================\n");

server.listen(PORT, HOST, () => {
    const networkInterfaces = os.networkInterfaces();
    const localIp = Object.values(networkInterfaces)
        .flat()
        .filter((iface) => iface && iface.family === 'IPv4' && !iface.internal)
        .map((iface) => iface.address)[0] || 'localhost';

    console.log(`🚀 PSA Unified System Online: http://${localIp}:${PORT}/login.html`);
    console.log(`Kiosk: http://${localIp}:${PORT}/getTicketNumberV2.html`);
    console.log(`Public Display: http://${localIp}:${PORT}/queue_Status.html`);
});
