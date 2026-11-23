// SERVIDOR DE PARTIDAS DO B.B. BOUNCE (WebSocket)
// Faz UMA coisa: retransmite, em tempo real, as posições e os eventos entre os jogadores da MESMA sala.
// O Firebase continua a tratar de contas, procura de sala, ranking e presentes. Aqui só corre a partida.
//
// Protocolo (texto JSON):
//  cliente -> servidor
//    {"op":"join","mid":"<sala>","uid":"<id>","name":"<nome>"}   (1ª mensagem, obrigatória)
//    {"op":"put","p":"/p/<uid>","d":{...}}     grava e retransmite   (também "/res/<uid>", "/players/<uid>", "/")
//    {"op":"patch","p":"/meta","d":{...}}      junta campos ao /meta
//    {"op":"post","p":"/ev","d":{...}}         evento novo (o servidor dá a chave)
//    {"op":"del","p":"/players/<uid>"}         apaga o meu jogador
//    {"op":"ping","t":123}                     -> {"e":"pong","t":123,"s":<hora do servidor>}
//  servidor -> cliente
//    {"e":"put","p":"/","d":{estado completo},"snap":1}   ao entrar (e ao voltar a ligar)
//    {"e":"put","p":"/p/<uid>","d":{...}}                  alterações dos OUTROS jogadores
//
// A forma dos dados é igual à que o jogo já usava no Firebase (meta, players, p, ev, res): o jogo quase não muda.
'use strict';
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PLAYERS = 8;            // por sala (a ranqueada usa até 7)
const MAX_MSG = 4096;             // bytes por mensagem
const MAX_RATE = 80;              // mensagens por segundo por ligação (acima disto ignora)
const ROOM_EMPTY_TTL = 120000;    // sala sem ninguém apaga-se ao fim de 2 min
const ROOM_MAX_AGE = 60 * 60000;  // e nenhuma sala vive mais de 1 hora
const IDLE_MS = 25000;            // ligação sem sinal há 25 s = morta
const MAX_EV = 400;               // eventos guardados por sala

const rooms = new Map();          // mid -> { state, clients:Set<ws>, evN, created, emptySince }
let totalMsgs = 0;

const server = http.createServer((req, res) => {
  if (req.url === '/saude' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, salas: rooms.size, ligacoes: wss.clients.size, mensagens: totalMsgs, hora: Date.now() }));
  } else { res.writeHead(404); res.end('nao'); }
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG, perMessageDeflate: false });

const OWN = /^\/(p|res|players)\/([A-Za-z0-9_-]{1,64})$/;
const clean = (s, n) => String(s == null ? '' : s).slice(0, n);

function newRoom() { return { state: { meta: {}, players: {}, p: {}, ev: {}, res: {} }, clients: new Set(), evN: 0, created: Date.now(), emptySince: 0 }; }

function broadcast(room, from, obj) {
  const txt = typeof obj === 'string' ? obj : JSON.stringify(obj);
  for (const c of room.clients) if (c !== from && c.readyState === 1) c.send(txt);
}

wss.on('connection', (ws, req) => {
  ws._socket && ws._socket.setNoDelay(true);        // sem espera de Nagle: cada posição sai logo
  ws.alive = true; ws.last = Date.now(); ws.room = null; ws.uid = null; ws.name = '?';
  ws.tokens = MAX_RATE; ws.tokT = Date.now();
  const joinTimer = setTimeout(() => { if (!ws.room) ws.terminate(); }, 6000);

  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {});
  ws.on('close', () => {
    clearTimeout(joinTimer);
    const r = ws.room; if (!r) return;
    r.clients.delete(ws);
    if (r.clients.size === 0) r.emptySince = Date.now();
  });

  ws.on('message', (buf) => {
    ws.last = Date.now(); ws.alive = true; totalMsgs++;
    const now = Date.now();
    ws.tokens = Math.min(MAX_RATE, ws.tokens + (now - ws.tokT) * MAX_RATE / 1000); ws.tokT = now;
    if (ws.tokens < 1) return; ws.tokens -= 1;       // demasiado rápido: ignora
    let m; try { m = JSON.parse(buf.toString('utf8')); } catch (e) { return; }
    if (!m || typeof m.op !== 'string') return;

    if (m.op === 'ping') { ws.send('{"e":"pong","t":' + (Number(m.t) || 0) + ',"s":' + Date.now() + '}'); return; }

    if (m.op === 'join') {
      if (ws.room) return;
      const mid = clean(m.mid, 64), uid = clean(m.uid, 64);
      if (!/^[A-Za-z0-9_-]{3,64}$/.test(mid) || !/^[A-Za-z0-9_-]{1,64}$/.test(uid)) { ws.close(1008, 'dados invalidos'); return; }
      let room = rooms.get(mid);
      if (!room) { room = newRoom(); rooms.set(mid, room); }
      const already = [...room.clients].some((c) => c.uid === uid);
      if (!already && room.clients.size >= MAX_PLAYERS) { ws.close(1008, 'sala cheia'); return; }
      for (const c of room.clients) if (c.uid === uid) { c.room = null; try { c.close(1000, 'nova ligacao'); } catch (e) {} room.clients.delete(c); }   // o mesmo jogador voltou a ligar: a ligação antiga sai
      ws.uid = uid; ws.name = clean(m.name, 24) || '?'; ws.room = room; room.emptySince = 0;
      room.clients.add(ws);
      clearTimeout(joinTimer);
      ws.send(JSON.stringify({ e: 'put', p: '/', d: room.state, snap: 1 }));
      return;
    }

    const room = ws.room; if (!room) return;
    const st = room.state, p = typeof m.p === 'string' ? m.p : '';

    if (m.op === 'put') {
      if (p === '/') {                                  // o criador cria a sala (só se ainda estiver vazia)
        if (Object.keys(st.players).length || Object.keys(st.meta).length) return;
        const d = m.d && typeof m.d === 'object' ? m.d : {};
        if (d.meta && typeof d.meta === 'object') st.meta = d.meta;
        if (d.players && typeof d.players === 'object') { const pl = d.players[ws.uid]; if (pl) st.players[ws.uid] = { name: clean(pl.name, 24) || ws.name }; }
        return;
      }
      const o = OWN.exec(p);
      if (!o || o[2] !== ws.uid) return;                // cada um só escreve nos seus caminhos
      const d = m.d;
      if (d == null || typeof d !== 'object') return;
      if (o[1] === 'players') d.name = clean(d.name, 24) || ws.name;
      st[o[1]][ws.uid] = d;
      broadcast(room, ws, { e: 'put', p, d });
      return;
    }

    if (m.op === 'patch') {
      if (p !== '/meta' || !m.d || typeof m.d !== 'object') return;
      for (const k of Object.keys(m.d)) {
        if (k !== 'start') continue;                    // só a hora de início pode mudar depois de criada
        const v = Number(m.d[k]); if (!isFinite(v)) continue;
        st.meta[k] = v;
        broadcast(room, ws, { e: 'put', p: '/meta/' + k, d: v });
      }
      return;
    }

    if (m.op === 'post') {
      if (p !== '/ev' || !m.d || typeof m.d !== 'object') return;
      const key = 'e' + String(++room.evN).padStart(6, '0');
      const d = m.d; d.u = ws.uid;                      // ninguém se faz passar por outro
      st.ev[key] = d;
      const keys = Object.keys(st.ev); if (keys.length > MAX_EV) delete st.ev[keys[0]];
      broadcast(room, ws, { e: 'put', p: '/ev/' + key, d });
      return;
    }

    if (m.op === 'del') {
      const o = OWN.exec(p);
      if (!o || o[2] !== ws.uid || o[1] !== 'players') return;
      delete st.players[ws.uid]; delete st.p[ws.uid];
      broadcast(room, ws, { e: 'put', p, d: null });
      return;
    }
  });
});

setInterval(() => {                                      // limpeza: ligações mortas e salas velhas
  const now = Date.now();
  for (const ws of wss.clients) {
    if (now - ws.last > IDLE_MS) { ws.terminate(); continue; }
    try { ws.ping(); } catch (e) {}
  }
  for (const [mid, r] of rooms) {
    if ((r.clients.size === 0 && r.emptySince && now - r.emptySince > ROOM_EMPTY_TTL) || now - r.created > ROOM_MAX_AGE) {
      for (const c of r.clients) try { c.close(1000, 'sala fechada'); } catch (e) {}
      rooms.delete(mid);
    }
  }
}, 5000);

server.listen(PORT, '0.0.0.0', () => console.log('Servidor B.B. Bounce a ouvir na porta ' + PORT + '  (teste: http://localhost:' + PORT + '/saude)'));
process.on('uncaughtException', (e) => console.error('erro:', e && e.stack || e));
