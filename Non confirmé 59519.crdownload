const http = require('http');
const fs   = require('fs');
const path = require('path');
const os   = require('os');

const PORT = process.env.PORT || 3000;
const IS_RENDER = !!process.env.RENDER;
const DATA_DIR  = IS_RENDER ? '/tmp/mmp-data' : path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ── Store en mémoire ──
const store = {
  ofs: {}, settings: {}, calendar: {}, history: {}, backups: {}, presence: {}, accounts: {}
};
const FILES = {};
Object.keys(store).forEach(k => { FILES[k] = path.join(DATA_DIR, k + '.json'); });

// Charger depuis disque
Object.keys(store).forEach(k => {
  try { if (fs.existsSync(FILES[k])) store[k] = JSON.parse(fs.readFileSync(FILES[k], 'utf8') || '{}'); }
  catch(e) { console.warn('Erreur chargement', k, e.message); }
});

// Sauvegarder (debounced)
const saveTimers = {};
function saveToDisk(key) {
  clearTimeout(saveTimers[key]);
  saveTimers[key] = setTimeout(() => {
    try { fs.writeFileSync(FILES[key], JSON.stringify(store[key], null, 2)); } catch(e) {}
  }, 500);
}

// ── SSE ──
const sseClients = new Set();
function broadcast(type, data) {
  const msg = `data: ${JSON.stringify({ type, data })}\n\n`;
  const dead = [];
  sseClients.forEach(res => { try { res.write(msg); } catch(e) { dead.push(res); } });
  dead.forEach(r => sseClients.delete(r));
}

setInterval(() => {
  const now = Date.now(); let changed = false;
  Object.keys(store.presence).forEach(sid => {
    if (now - (store.presence[sid].lastSeen || 0) > 90000) { delete store.presence[sid]; changed = true; }
  });
  if (changed) { saveToDisk('presence'); broadcast('presence', store.presence); }
}, 30000);

function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function readBody(req) {
  return new Promise(res => {
    let b=''; req.on('data',c=>b+=c); req.on('end',()=>{try{res(JSON.parse(b||'{}'))}catch(e){res({})}});
    req.on('error',()=>res({}));
  });
}

const APP_FILE = path.join(__dirname, 'app', 'index.html');
let appHtml = null;
function getAppHtml() { if(!appHtml) try { appHtml = fs.readFileSync(APP_FILE, 'utf8'); } catch(e){ return null; } return appHtml; }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost`);
  const pathname = url.pathname;
  setCORS(res);
  if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}

  // Servir l'app
  if(pathname==='/'||pathname==='/index.html'){
    const html=getAppHtml();
    if(!html){res.writeHead(503,{'Content-Type':'text/html'});res.end('<h2>⚠️ App non trouvée</h2>');return;}
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});
    return res.end(html);
  }

  // Health check
  if(pathname==='/status'||pathname==='/health'){
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({status:'ok',version:'8.1',ofCount:Object.keys(store.ofs).length,clients:sseClients.size,env:IS_RENDER?'render':'local'}));
  }

  // SSE
  if(pathname==='/events'){
    res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','X-Accel-Buffering':'no'});
    res.write('retry: 5000\n\n');
    res.write(`data: ${JSON.stringify({type:'init',data:{ofs:store.ofs,settings:store.settings,calendar:store.calendar,history:store.history,presence:store.presence,accounts:store.accounts}})}\n\n`);
    sseClients.add(res);
    const hb=setInterval(()=>{try{res.write(': heartbeat\n\n')}catch(e){clearInterval(hb);sseClients.delete(res);}},20000);
    req.on('close',()=>{clearInterval(hb);sseClients.delete(res);});
    return;
  }

  // API
  const apiMatch = pathname.match(/^\/api\/([a-z]+)\/?(.*)$/);
  if(apiMatch){
    const resource=apiMatch[1], sub=apiMatch[2]||'';
    if(!store[resource]){res.writeHead(404,{'Content-Type':'application/json'});return res.end(JSON.stringify({error:'Resource inconnue'}));}
    
    if(req.method==='GET'){res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify(store[resource]));}
    
    if(req.method==='POST'){
      const body=await readBody(req);
      if(resource==='presence'&&sub){store.presence[sub]={...body,lastSeen:Date.now()};saveToDisk('presence');broadcast('presence',store.presence);res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:true}));}
      if(resource==='history'){const id='h_'+Date.now()+'_'+Math.random().toString(36).slice(2,6);store.history[id]={...body,ts:Date.now()};saveToDisk('history');broadcast('history',store.history);res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:true,id}));}
      // Gestion des comptes et autres
      if(['accounts','ofs','settings'].includes(resource)){store[resource]=body;saveToDisk(resource);broadcast(resource,body);res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:true}));}
    }
    if(req.method==='DELETE'&&resource==='presence'&&sub){delete store.presence[sub];saveToDisk('presence');broadcast('presence',store.presence);res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:true}));}
  }
  res.writeHead(404);res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 MMP Planning v8.1 démarré sur le port ${PORT}\n`);
});
server.on('error', err => { console.error('Erreur serveur:', err.message); process.exit(1); });