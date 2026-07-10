// ( mysql/mariadb ver )

const Ably = require('ably');
const mysqlExample = require('mysql2/promise');
const fs = require('fs').promises;
const path = require('path');
const ablyApiKey = process.env.ABLY_API_KEY;
const ably = new Ably.Realtime({ key: ablyApiKey });
require('dotenv').config({ quiet: true });

// MySQL connection configuration
const mysqlConfig = {
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PWD,
  port: process.env.MYSQL_PORT,
  timezone: process.env.DEFAULT_TIMEZONE,
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

async function getTableColumns(connection, databaseName, tableName) {
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
    const [rows] = await connection.execute(`DESCRIBE \`${tableName}\``);
    const columns = rows.map(row => row.Field);

    // write to cache
    await fs.mkdir(cacheDir, { recursive: true });
    await fs.writeFile(cacheFile, JSON.stringify(columns), 'utf8');
    
    return columns;
  } catch (err) {
    console.error(`❌ tb struct handle err (${tableName}):`, err.message);
    return null;
  }
}

// function for MySQL connection
async function handleSetStorageEvent(messageData) {
  let connection = null;

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

    // establish connection
    connection = await mysqlExample.createConnection({
      ...mysqlConfig,
      database: databaseName
    });

    // get table structure (from cache or DB)
    const tableColumns = await getTableColumns(connection, databaseName, tableName);
    const columnSet = tableColumns ? new Set(tableColumns) : null;

    // ID check - Insert or Update?
    const id = safeInt(messageData.ID);
    const isUpdate = id !== null && id > 0;

    // DB fields: must not be meta or undefined, must be fields that exist in the table
    const dataEntries = Object.entries(messageData)
      .filter(([k, v]) => !META_KEYS.has(k))
      .filter(([, v]) => v !== undefined)
      .filter(([k]) => columnSet === null || columnSet.has(k));

    if (isUpdate) {
      console.log('UPDATE', messageData);

      // ID not needed for Update
      const updateEntries = dataEntries.filter(([k]) => k !== 'ID');

      const updateFields = updateEntries
        .map(([k]) => `\`${k}\` = ?`)
        .join(', ');

      if (!updateFields) {
        console.log(`ℹ️ UPDATE:SKIP | ${tableName}`);
        return;
      }

      const updateSql = `UPDATE \`${tableName}\` SET ${updateFields} WHERE ID = ?`;
      const updateValues = updateEntries.map(([, v]) => v);
      updateValues.push(id);

      const [result] = await connection.execute(updateSql, updateValues);
      console.log(`✅ UPDATE:OK | ${tableName}, Aff.Rows: ${result.affectedRows}`);
        /*
      await publishReply(messageData, {
        ...baseReply,
        ok: true,
        op: 'update',
        id,
        affectedRows: result.affectedRows
      });
        */
    } else {
      console.log('INSERT', messageData);

      // for Insert, don't add ID as a column if it's 0 / empty (leave it to auto_increment)
      const insertEntries = dataEntries.filter(([k, v]) => {
        if (k !== 'ID') return true;
        const id2 = safeInt(v);
        return id2 !== null && id2 > 0; // only write if ID was actually given
      });

      const insertFields = insertEntries.map(([k]) => `\`${k}\``).join(', ');
      const insertPlaceholders = insertEntries.map(() => '?').join(', ');

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

      const insertSql = `INSERT INTO \`${tableName}\` (${insertFields}) VALUES (${insertPlaceholders})`;
      const insertValues = insertEntries.map(([, v]) => (v === null ? null : v));

      const [result] = await connection.execute(insertSql, insertValues);
      console.log(`✅ INSERT:OK | ${tableName}, ID: ${result.insertId}`);

      await publishReply(messageData, {
        ...baseReply,
        ok: true,
        op: 'insert',
        insertId: result.insertId
      });
    }

  } catch (error) {
    console.error('❌ MySQL err:', error && error.message ? error.message : error);

    if (error && error.code === 'ER_NO_SUCH_TABLE') {
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
    if (connection) {
      try {
        await connection.end();
      } catch (e) {
        console.error('mysql-conn-close failed', e && e.message ? e.message : e);
      }
    }
  }
}

channel.subscribe(eventName, async (message) => {
  try {
    console.log('setStorage event', message.data);
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
