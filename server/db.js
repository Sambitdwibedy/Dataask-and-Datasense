// Database connection — mysql2 pool (PostgreSQL → MySQL migration)
const mysql = require('mysql2/promise');
require('dotenv').config();

const pool = mysql.createPool({
  uri: process.env.DATABASE_URL,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  timezone: 'Z',
});

// Compatibility wrapper: returns [rows, fields] like mysql2 native
const query = (sql, params) => pool.execute(sql, params || []);

module.exports = { pool, query };
