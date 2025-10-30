const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const PORT = 5000;

let currentSchema = ['public'];

// 미들웨어
app.use(cors());
app.use(express.json());

// 연결 풀을 저장할 객체
let currentPool = null;

// PostgreSQL 연결 테스트
app.post('/api/connect', async (req, res) => {
  const { host, port, database, username, password } = req.body;

  try {
    // 기존 연결이 있으면 종료
    if (currentPool) {
      await currentPool.end();
    }

    // 새 연결 풀 생성
    currentPool = new Pool({
      host,
      port: parseInt(port),
      database,
      user: username,
      password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    // 연결 테스트
    const client = await currentPool.connect();
    client.release();

    res.json({ 
      success: true, 
      message: '데이터베이스에 성공적으로 연결되었습니다.' 
    });
  } catch (error) {
    console.error('DB 연결 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

app.get('/api/schema', async (req, res) => {
  if (!currentPool) {
    return res.status(400).json({ 
      success: false, 
      message: '데이터베이스에 연결되지 않았습니다.' 
    });
  }
  try {
    let params = [];
    let query = `
      select schema_name
      from information_schema.schemata
      where schema_name not in ('information_schema', 'pg_catalog', 'pg_toast');
    `;
    
    const result = await currentPool.query(query, params);
    const schema = result.rows.map(row => row.schema_name);

    // 스키마값 저장
    if(schema.length > 0) {
      currentSchema = schema;
    } else {
      currentSchema = ['public'];
    }

    res.json({ 
      success: true, 
      schema
    });
  } catch (error) {
    console.error('스키마 조회 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// 테이블 목록 조회 (검색어 포함)
app.get('/api/tables', async (req, res) => {
  if (!currentPool) {
    return res.status(400).json({ 
      success: false, 
      message: '데이터베이스에 연결되지 않았습니다.' 
    });
  }

  const { search } = req.query;

  try {
    let query;
    let params = [];

    if (search && search.trim() !== '') {
      // 검색어가 있으면 테이블명, 컬럼명, 코멘트에서 검색
      query = `
        SELECT DISTINCT t.table_name
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c 
          ON t.table_name = c.table_name 
          AND t.table_schema = c.table_schema
        LEFT JOIN pg_catalog.pg_statio_all_tables st 
          ON c.table_schema = st.schemaname 
          AND c.table_name = st.relname
        LEFT JOIN pg_catalog.pg_description pgd 
          ON pgd.objoid = st.relid 
          AND pgd.objsubid = c.ordinal_position
        WHERE t.table_schema = ANY($1)
          AND t.table_type = 'BASE TABLE'
          AND (
            t.table_name ILIKE $2
            OR c.column_name ILIKE $2
            OR pgd.description ILIKE $2
          )
        ORDER BY t.table_name;
      `;
      params = [currentSchema, `%${search}%`];
    } else {
      // 검색어가 없으면 전체 테이블 조회
      query = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = ANY($1)
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `;
      params = [currentSchema];
    }
    
    const result = await currentPool.query(query, params);
    const tables = result.rows.map(row => row.table_name);

    res.json({ 
      success: true, 
      tables 
    });
  } catch (error) {
    console.error('테이블 조회 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// 특정 테이블의 컬럼 정보 조회
app.get('/api/tables/:tableName/columns', async (req, res) => {
  if (!currentPool) {
    return res.status(400).json({ 
      success: false, 
      message: '데이터베이스에 연결되지 않았습니다.' 
    });
  }

  const { tableName } = req.params;

  try {
    const query = `
      SELECT 
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.is_nullable,
        c.column_default,
        pgd.description as comment,
        CASE 
          WHEN pk.column_name IS NOT NULL THEN 'PRI'
          WHEN uq.column_name IS NOT NULL THEN 'UNI'
          ELSE ''
        END as key_type
      FROM information_schema.columns c
      LEFT JOIN pg_catalog.pg_statio_all_tables st 
        ON c.table_schema = st.schemaname 
        AND c.table_name = st.relname
      LEFT JOIN pg_catalog.pg_description pgd 
        ON pgd.objoid = st.relid 
        AND pgd.objsubid = c.ordinal_position
      LEFT JOIN (
        SELECT ku.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku
          ON tc.constraint_name = ku.constraint_name
          AND tc.table_schema = ku.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = $2
          AND tc.table_schema = ANY($1)
      ) pk ON c.column_name = pk.column_name
      LEFT JOIN (
        SELECT ku.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage ku
          ON tc.constraint_name = ku.constraint_name
          AND tc.table_schema = ku.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
          AND tc.table_name = $2
          AND tc.table_schema = ANY($1)
      ) uq ON c.column_name = uq.column_name
      WHERE c.table_name = $2
        AND c.table_schema = ANY($1)
      ORDER BY c.ordinal_position;
    `;

    const result = await currentPool.query(query, [currentSchema, tableName]);
    
    const columns = result.rows.map(row => ({
      name: row.column_name,
      type: row.character_maximum_length 
        ? `${row.data_type}(${row.character_maximum_length})`
        : row.data_type,
      key: row.key_type,
      null: row.is_nullable.toUpperCase(),
      default: row.column_default || null,
      comment: row.comment || null,
      extra: ''
    }));

    res.json({ 
      success: true, 
      columns 
    });
  } catch (error) {
    console.error('컬럼 조회 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// 특정 테이블의 DDL 생성
app.get('/api/tables/:tableName/ddl', async (req, res) => {
  if (!currentPool) {
    return res.status(400).json({ 
      success: false, 
      message: '데이터베이스에 연결되지 않았습니다.' 
    });
  }

  const { tableName } = req.params;

  try {
    // 테이블 구조 조회
    const columnsQuery = `
      SELECT 
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        pgd.description as comment
      FROM information_schema.columns c
      LEFT JOIN pg_catalog.pg_statio_all_tables st 
        ON c.table_schema = st.schemaname 
        AND c.table_name = st.relname
      LEFT JOIN pg_catalog.pg_description pgd 
        ON pgd.objoid = st.relid 
        AND pgd.objsubid = c.ordinal_position
      WHERE c.table_name = $1
        AND c.table_schema = ANY($2)
      ORDER BY c.ordinal_position;
    `;

    // Primary Key 조회
    const pkQuery = `
      SELECT ku.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage ku
        ON tc.constraint_name = ku.constraint_name
        AND tc.table_schema = ku.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_name = $1
        AND tc.table_schema = ANY($2)
      ORDER BY ku.ordinal_position;
    `;

    // Unique 제약조건 조회
    const uniqueQuery = `
      SELECT ku.column_name, tc.constraint_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage ku
        ON tc.constraint_name = ku.constraint_name
        AND tc.table_schema = ku.table_schema
      WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = $1
        AND tc.table_schema = ANY($2)
      ORDER BY tc.constraint_name, ku.ordinal_position;
    `;

    // Foreign Key 조회
    const fkQuery = `
      SELECT
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.constraint_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      JOIN information_schema.referential_constraints AS rc
        ON rc.constraint_name = tc.constraint_name
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = $1
        AND tc.table_schema = ANY($2);
    `;

    const [columnsResult, pkResult, uniqueResult, fkResult] = await Promise.all([
      currentPool.query(columnsQuery, [tableName, currentSchema]),
      currentPool.query(pkQuery, [tableName, currentSchema]),
      currentPool.query(uniqueQuery, [tableName, currentSchema]),
      currentPool.query(fkQuery, [tableName, currentSchema])
    ]);

    // DDL 생성
    let ddl = `CREATE TABLE ${tableName} (\n`;
    
    // 컬럼 정의
    const columnDefs = columnsResult.rows.map(col => {
      let def = `  ${col.column_name} `;
      
      // 데이터 타입
      if (col.character_maximum_length) {
        def += `${col.data_type.toUpperCase()}(${col.character_maximum_length})`;
      } else if (col.numeric_precision) {
        def += `${col.data_type.toUpperCase()}(${col.numeric_precision}${col.numeric_scale ? ',' + col.numeric_scale : ''})`;
      } else {
        def += col.data_type.toUpperCase();
      }
      
      // NULL 여부
      if (col.is_nullable === 'NO') {
        def += ' NOT NULL';
      }
      
      // DEFAULT 값
      if (col.column_default) {
        def += ` DEFAULT ${col.column_default}`;
      }
      
      return def;
    });
    
    ddl += columnDefs.join(',\n');
    
    // Primary Key
    if (pkResult.rows.length > 0) {
      const pkColumns = pkResult.rows.map(row => row.column_name).join(', ');
      ddl += `,\n  PRIMARY KEY (${pkColumns})`;
    }
    
    // Unique 제약조건
    const uniqueConstraints = {};
    uniqueResult.rows.forEach(row => {
      if (!uniqueConstraints[row.constraint_name]) {
        uniqueConstraints[row.constraint_name] = [];
      }
      uniqueConstraints[row.constraint_name].push(row.column_name);
    });
    
    Object.entries(uniqueConstraints).forEach(([name, columns]) => {
      ddl += `,\n  CONSTRAINT ${name} UNIQUE (${columns.join(', ')})`;
    });
    
    // Foreign Key
    fkResult.rows.forEach(row => {
      ddl += `,\n  CONSTRAINT ${row.constraint_name} FOREIGN KEY (${row.column_name}) REFERENCES ${row.foreign_table_name}(${row.foreign_column_name})`;
    });
    
    ddl += '\n);';
    
    // 컬럼 코멘트 추가
    const comments = columnsResult.rows
      .filter(col => col.comment)
      .map(col => `COMMENT ON COLUMN ${tableName}.${col.column_name} IS '${col.comment}';`)
      .join('\n');
    
    if (comments) {
      ddl += '\n\n' + comments;
    }

    res.json({ 
      success: true, 
      ddl 
    });
  } catch (error) {
    console.error('DDL 생성 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// 특정 테이블의 DML 생성 (INSERT 문)
app.get('/api/tables/:tableName/dml', async (req, res) => {
  if (!currentPool) {
    return res.status(400).json({ 
      success: false, 
      message: '데이터베이스에 연결되지 않았습니다.' 
    });
  }

  const { tableName } = req.params;
  const { limit = 100 } = req.query; // 기본 100개 row

  try {
    // 컬럼 목록 조회
    const columnsQuery = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = $1
        AND table_schema = ANY($2)
      ORDER BY ordinal_position;
    `;
    
    const columnsResult = await currentPool.query(columnsQuery, [tableName, currentSchema]);
    const columns = columnsResult.rows.map(row => row.column_name);
    
    // 데이터 조회
    const dataQuery = `SELECT * FROM ${tableName} LIMIT $1;`;
    const dataResult = await currentPool.query(dataQuery, [parseInt(limit)]);
    
    // INSERT 문 생성
    let dml = '';
    dataResult.rows.forEach(row => {
      const values = columns.map(col => {
        const value = row[col];
        if (value === null) return 'NULL';
        if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
        if (value instanceof Date) return `'${value.toISOString()}'`;
        if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
        return value;
      }).join(', ');
      
      dml += `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values});\n`;
    });

    res.json({ 
      success: true, 
      dml,
      rowCount: dataResult.rows.length
    });
  } catch (error) {
    console.error('DML 생성 오류:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// 연결 해제
app.post('/api/disconnect', async (req, res) => {
  if (currentPool) {
    await currentPool.end();
    currentPool = null;
  }
  res.json({ 
    success: true, 
    message: '연결이 해제되었습니다.' 
  });
});

app.listen(PORT, () => {
  console.log(`서버가 포트 ${PORT}에서 실행 중입니다.`);
});