const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const QRCode = require('qrcode');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

// تشغيل السيرفر فوراً على localhost لمنع Timeout
const PORT = process.env.PORT || 3000;
server.listen(PORT, '127.0.0.1', () => {
    console.log(`🚀 Server running on http://127.0.0.1:${PORT}`);
});

app.use(express.json({ limit: '50mb' }));
app.use(express.static(__dirname)); // تقديم الملفات من نفس المجلد

// مسارات الصفحات
const pages = ['index', 'reception', 'doctor', 'pharmacy', 'lab', 'radiology', 'hospital_board', 'patient', 'gr'];
pages.forEach(page => {
    app.get('/' + page + '.html', (req, res) => {
        let p = path.join(__dirname, page + '.html');
        if (fs.existsSync(p)) res.sendFile(p);
        else res.status(404).send('File not found');
    });
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// قاعدة البيانات (JSON)
const DB_FILE = path.join(__dirname, 'database.json');
let db = { patients: [], ledger: [], cards: [], vouchers: [], media: [], pharmacyOrders: [], labOrders: [], departments: [], settings: {}, counters: { seq: 0 } };

if (fs.existsSync(DB_FILE)) {
    try { db = { ...db, ...JSON.parse(fs.readFileSync(DB_FILE, 'utf8')) }; } catch (e) {}
}

let saveTimer = null;
function saveDB() {
    if (saveTimer) clearTimeout(saveTimer);
    saveTimer = setTimeout(() => { try { fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); } catch (e) {} }, 500);
}

// بيانات افتراضية
if (db.departments.length === 0) {
    db.departments = [
        { id: 'doctor', name: 'الطبيب', icon: '👨‍⚕️', class: 'r-doctor' },
        { id: 'pharmacy', name: 'الصيدلية', icon: '💊', class: 'r-pharma' },
        { id: 'lab', name: 'المختبر', icon: '🧪', class: 'r-lab' },
        { id: 'xray', name: 'الأشعة والسونار', icon: '🩻', class: 'r-xray' }
    ];
    saveDB();
}

const DEPT_META = { doctor: { name: 'الطبيب', icon: '👨‍⚕️' }, pharmacy: { name: 'الصيدلية', icon: '💊' }, lab: { name: 'المختبر', icon: '🧪' }, xray: { name: 'الأشعة والسونار', icon: '🩻' }, reception: { name: 'الاستقبال', icon: '🏥' } };
const getActivePatients = () => db.patients.filter(p => p.status !== 'discharged');
const getPatient = (id) => db.patients.find(p => p.id === id);

// WebSocket
const clients = new Map();
const broadcaster = {
    queue: [], running: false,
    send(payload, resolver) { this.queue.push({ payload, resolver }); if (!this.running) this.start(); },
    start() {
        this.running = true;
        const tick = () => {
            if (!this.queue.length) { this.running = false; return; }
            const { payload, resolver } = this.queue.shift();
            try { const json = JSON.stringify(payload); resolver().forEach(ws => { if (ws.readyState === 1) ws.send(json); }); } catch (e) {}
            setTimeout(tick, 10);
        };
        tick();
    }
};

function findSockets(target) {
    const result = [];
    const [kind, id] = target.split(':');
    clients.forEach((info, ws) => {
        if (kind === 'all') result.push(ws);
        else if (kind === 'role' && info.role === id) result.push(ws);
        else if (kind === 'dept' && info.role === 'dept' && info.deptId === id) result.push(ws);
        else if (kind === 'patient' && info.role === 'patient' && info.patientId === id) result.push(ws);
    });
    return result;
}
const broadcast = (target, payload) => broadcaster.send(payload, () => findSockets(target));

function sendFullState(ws) {
    ws.send(JSON.stringify({
        type: 'init', patients: getActivePatients(), departments: db.departments,
        cards: db.cards, vouchers: db.vouchers, ledger: db.ledger, media: db.media,
        pharmacyOrders: db.pharmacyOrders, labOrders: db.labOrders, settings: db.settings
    }));
}

wss.on('connection', (ws, req) => {
    try {
        const url = new URL(req.url, 'http://localhost');
        const role = url.searchParams.get('role') || 'unknown';
        const deptId = url.searchParams.get('dept');
        const patientId = url.searchParams.get('pid');
        clients.set(ws, { role, deptId, patientId });

        if (role === 'patient' && patientId) ws.send(JSON.stringify({ type: 'init', patient: getPatient(patientId) }));
        else if (role === 'dept' && deptId) ws.send(JSON.stringify({ type: 'init', queue: getQueue(deptId), dept: deptId, deptMeta: DEPT_META[deptId] || {} }));
        else sendFullState(ws);

        ws.on('message', raw => { try { const m = JSON.parse(raw); handleMessage(m.type, m.data || {}, ws); } catch (e) {} });
        ws.on('close', () => clients.delete(ws)); ws.on('error', () => clients.delete(ws));
    } catch (e) { try { ws.close(); } catch (e2) {} }
});

// معالجة الرسائل
function handleMessage(type, data, ws) {
    switch (type) {
        case 'register':
            const name = String(data.name || '').trim();
            if (!name) return;
            db.counters.seq += 1;
            const id = 'P' + Date.now().toString(36).toUpperCase();
            const patient = { id, name, phone: data.phone || '', consult: data.consult || '', doctor: data.doctor || '', seq: db.counters.seq, current_dept: 'doctor', status: 'waiting', route: [{ dept: 'الاستقبال', icon: '🏥', status: 'done', at: Date.now() }, { dept: 'الطبيب', icon: '👨‍⚕️', status: 'current', at: null }], created_at: Date.now() };
            db.patients.push(patient); saveDB();
            broadcast('role:reception', { type: 'patient_added', patient });
            broadcast('role:tv', { type: 'patient_added', patient });
            broadcast('dept:doctor', { type: 'patient_added', patient });
            break;

        case 'route':
            const p = db.patients.find(x => x.id === data.id);
            if (!p || p.current_dept === data.toDept) return;
            const from = p.current_dept;
            p.route.forEach(r => { if (r.status === 'current') r.status = 'done'; });
            p.route.push({ dept: (DEPT_META[data.toDept] || { name: data.toDept }).name, icon: (DEPT_META[data.toDept] || { icon: '🏥' }).icon, status: 'current', at: Date.now() });
            p.current_dept = data.toDept; p.status = 'waiting'; saveDB();
            broadcast('role:reception', { type: 'patient_updated', patient: p });
            broadcast('role:tv', { type: 'patient_updated', patient: p });
            broadcast('dept:' + from, { type: 'patient_removed', patientId: p.id });
            broadcast('dept:' + data.toDept, { type: 'patient_added', patient: p });
            broadcast('patient:' + p.id, { type: 'patient_update', patient: p });
            break;

        case 'call':
            const pc = getPatient(data.id);
            if (pc) { pc.status = 'in_progress'; saveDB(); broadcast('role:tv', { type: 'patient_called', patient: pc }); broadcast('patient:' + pc.id, { type: 'called', patient: pc }); }
            break;

        case 'discharge':
            const pd = getPatient(data.id);
            if (pd) { pd.status = 'discharged'; saveDB(); broadcast('role:tv', { type: 'patient_removed', patientId: pd.id }); broadcast('patient:' + pd.id, { type: 'discharged', patient: pd }); }
            break;

        case 'send_to_pharmacy':
            db.pharmacyOrders.push(data); saveDB();
            broadcast('dept:pharmacy', { type: 'new_order', order: data }); break;

        case 'send_to_lab':
            db.labOrders.push(data); saveDB();
            broadcast('dept:lab', { type: 'new_lab_order', order: data }); break;

        case 'upload_media':
            const media = { id: 'MED-' + Date.now(), ...data };
            db.media.push(media); saveDB();
            broadcast('dept:doctor', { type: 'media_added', media }); break;
            
        case 'request_sync': sendFullState(ws); break;
    }
}

// QR API
app.get('/api/qr', async (req, res) => {
    try {
        const data = req.query.d || ''; const size = Math.min(600, Math.max(80, Number(req.query.s) || 300));
        if (!data) return res.status(400).end('missing');
        const buf = await QRCode.toBuffer(data, { width: size, margin: 2 });
        res.type('png').send(buf);
    } catch (e) { res.status(500).end(); }
});

function getQueue(dept) { return db.patients.filter(p => p.current_dept === dept && p.status !== 'discharged'); }