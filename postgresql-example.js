// (PostgreSQL ver)

const Ably = require('ably');
const { Pool } = require('pg');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config({ quiet: true });

const ablyApiKey = process.env.ABLY_API_KEY;
const ably = new Ably.Realtime({ key: ablyApiKey });

// PostgreSQL connection configuration
const pgConfig = {
  host: process.env.POSTGRESQL_HOST,
  user: process.env.POSTGRESQL_USER,
  password: process.env.POSTGRESQL_PWD,
  port: process.env.POSTGRESQL_PORT,
};

// channel/event to subscribe to
const channelName = process.env.ABLY_CHANNEL_NAME;
const eventName = process.env.ABLY_EVENT_NAME;
const channel = ably.channels.get(channelName);

// meta fields that should not be written to the DB
const META_KEYS = new Set([
  'vt', 'tb',
  'reqId',
  'replyChannel',
  'replyEvent',
  'noReply',
  'tmp_id'
]);

function safeInt(val) {
  const n = Number.parseInt(String(val), 10);
  return Number.isFinite(n) ? n : null;
}

async function publishReply(messageData, payload) {
  if (messageData.hasOwnProperty('noReply')) {
      return;
  }
  // silently exit if required info for reply is missing
  const reqId = messageData && messageData.reqId ? String(messageData.reqId) : null;
  const replyChannelName = messageData && messageData.replyChannel ? String(messageData.replyChannel) : null;
  const replyEventName = messageData && messageData.replyEvent ? String(messageData.replyEvent) : 'setStorageSonucuBilocan';

  if (!reqId || !replyChannelName) return;

  try {
    const replyChannel = ably.channels.get(replyChannelName);
    await replyChannel.publish(replyEventName, {
      reqId,
      ...payload,
      _at: new Date().toISOString()
    });
  } catch (e) {
    console.error('❌ Reply publish err:', e && e.message ? e.message : e);
  }
}

async function getTableColumns(pool, databaseName, tableName) {
  const cacheDir = path.join(__dirname, '.cache');
  const cacheFile = path.join(cacheDir, `${databaseName}_${tableName}.json`);
  const ONE_MONTH_MS = 30 * 24 * 60 * 60 * 1000;

  try {
    const stats = await fs.stat(cacheFile);
    const now = Date.now();
    if (now - stats.mtimeMs < ONE_MONTH_MS) {
      const data = await fs.readFile(cacheFile, 'utf8');
      return JSON.parse(data);
    }
  } catch (err) {
    // continue if file doesn't exist or can't be read
  }

  // get from DB if there's no cache or it's outdated
  try {
    // the standard way to get table columns in PostgreSQL is using information_schema
    const query = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = $1 
      AND table_catalog = $2
    `;
    const { rows } = await pool.query(query, [tableName, databaseName]);
    const columns = rows.map(row => row.column_name);

    if (columns.length === 0) return null;

    // write to cache
    await fs.mkdir(cacheDir, { recursive: true });
    await fs.writeFile(cacheFile, JSON.stringify(columns), 'utf8');
    
    return columns;
  } catch (err) {
    console.error(`❌ tb struct handle err (${tableName}):`, err.message);
    return null;
  }
}

// function for PostgreSQL connection
async function handleSetStorageEvent(messageData) {
  let pool = null;

  // default payload skeleton for reply
  const baseReply = {
    ok: false,
    op: null,
    vt: messageData ? messageData.vt : null,
    tb: messageData ? messageData.tb : null
  };

  try {
    // get the database name
    const databaseName = messageData.vt;
    if (!databaseName) {
      console.error('❌ not found:', databaseName, messageData);
      await publishReply(messageData, { ...baseReply, error: 'vt missing' });
      return;
    }

    // get the table name
    const tableName = messageData.tb;
    if (!tableName) {
      console.error('❌ tb not found:', tableName, messageData);
      await publishReply(messageData, { ...baseReply, error: 'tb missing' });
      return;
    }

    // in PostgreSQL, a new pool/connection may be needed for each database
    pool = new Pool({
      ...pgConfig,
      database: databaseName
    });

    // get table structure (from cache or DB)
    const tableColumns = await getTableColumns(pool, databaseName, tableName);
    const columnSet = tableColumns ? new Set(tableColumns) : null;

    // ID check - Insert or Update?
    // in PostgreSQL, ID is often lowercase 'id', but mysql-example.js used 'ID'
    const id = safeInt(messageData.ID);
    const isUpdate = id !== null && id > 0;

    // DB fields: must not be meta or undefined, must be fields that exist in the table
    const dataEntries = Object.entries(messageData)
      .filter(([k, v]) => !META_KEYS.has(k))
      .filter(([, v]) => v !== undefined)
      .filter(([k]) => columnSet === null || columnSet.has(k.toLowerCase()) || columnSet.has(k));

    if (isUpdate) {
      console.log('UPDATE', messageData);

      // ID not needed for Update
      const updateEntries = dataEntries.filter(([k]) => k.toUpperCase() !== 'ID');

      let paramCounter = 1;
      const updateFields = updateEntries
        .map(([k]) => `"${k}" = $${paramCounter++}`)
        .join(', ');

      if (!updateFields) {
        console.log(`ℹ️ UPDATE:SKIP | ${tableName}`);
        return;
      }

      const updateSql = `UPDATE "${tableName}" SET ${updateFields} WHERE "ID" = $${paramCounter}`;
      const updateValues = updateEntries.map(([, v]) => v);
      updateValues.push(id);

      const result = await pool.query(updateSql, updateValues);
      console.log(`✅ UPDATE:OK | ${tableName}, Aff.Rows: ${result.rowCount}`);
        
    } else {
      console.log('INSERT', messageData);

      // for Insert, don't add ID as a column if it's 0 / empty (leave it to serial/identity)
      const insertEntries = dataEntries.filter(([k, v]) => {
        if (k.toUpperCase() !== 'ID') return true;
        const id2 = safeInt(v);
        return id2 !== null && id2 > 0; // only write if ID was actually given
      });

      const insertFields = insertEntries.map(([k]) => `"${k}"`).join(', ');
      let paramCounter = 1;
      const insertPlaceholders = insertEntries.map(() => `$${paramCounter++}`).join(', ');

      if (!insertFields) {
        console.log(`ℹ️ INSERT:SKIP | ${tableName} (eklenecek alan yok)`);
        await publishReply(messageData, {
          ...baseReply,
          ok: false,
          op: 'insert',
          error: 'no_fields'
        });
        return;
      }

      // in PostgreSQL, the new ID is obtained with RETURNING ID
      const insertSql = `INSERT INTO "${tableName}" (${insertFields}) VALUES (${insertPlaceholders}) RETURNING "ID"`;
      const insertValues = insertEntries.map(([, v]) => (v === null ? null : v));

      const result = await pool.query(insertSql, insertValues);
      const insertedId = result.rows[0] ? result.rows[0].ID : null;
      console.log(`✅ INSERT:OK | ${tableName}, ID: ${insertedId}`);

      await publishReply(messageData, {
        ...baseReply,
        ok: true,
        op: 'insert',
        insertId: insertedId
      });
    }

  } catch (error) {
    console.error('❌ PostgreSQL err:', error && error.message ? error.message : error);

    // PostgreSQL error code 42P01: relation "..." does not exist
    if (error && error.code === '42P01') {
      console.error(`💡 tb "${messageData.tb}" not found.`);
    }

    await publishReply(messageData, {
      ...baseReply,
      ok: false,
      op: 'error',
      error: error && error.message ? error.message : String(error),
      code: error && error.code ? error.code : null
    });

  } finally {
    if (pool) {
      try {
        await pool.end();
      } catch (e) {
        console.error('pg-pool-close failed', e && e.message ? e.message : e);
      }
    }
  }
}

channel.subscribe(eventName, async (message) => {
  try {
    console.log('setStorage event (PG)', message.data);
    await handleSetStorageEvent(message.data);
  } catch (e) {
    console.error('storage set err:', e && e.message ? e.message : e);
  }
});

// process handlers
process.on('SIGINT', () => {
  console.log('closing...');
  try { ably.close(); } catch (e) {}
  process.exit();
});
