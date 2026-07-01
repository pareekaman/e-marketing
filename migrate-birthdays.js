// One-time script: populate birthday & joining_date for existing users from Google Sheet data
// Run with: node migrate-birthdays.js
require('dotenv').config();
const mysql = require('mysql2/promise');

const DATA = [
  { name: 'Akhilesh Vyas',      birthday: '2001-04-28', joining_date: '2025-06-02' },
  { name: 'Taaran Jain',        birthday: '2003-04-25', joining_date: '2025-06-16' },
  { name: 'Priya Saini',        birthday: '1997-10-07', joining_date: '2025-05-12' },
  { name: 'Garvit Kedia',       birthday: '2002-04-08', joining_date: '2024-04-14' },
  { name: 'Purvi Saini',        birthday: '2003-11-21', joining_date: '2024-12-04' },
  { name: 'Nisha Madaan',       birthday: '1989-11-14', joining_date: '2024-11-10' },
  { name: 'Nupur Kothari',      birthday: '1999-05-17', joining_date: '2024-09-23' },
  { name: 'Aman Bejal',         birthday: '2001-05-03', joining_date: '2024-07-16' },
  { name: 'Akshita Jain',       birthday: '2004-12-13', joining_date: '2024-03-01' },
  { name: 'Divya Srivastava',   birthday: '2001-07-12', joining_date: '2023-12-11' },
  { name: 'Tushar Chauhan',     birthday: '1998-08-01', joining_date: '2023-07-20' },
  { name: 'Ritu Tilokani',      birthday: '2002-01-07', joining_date: '2023-06-12' },
  { name: 'Sakshi Saini',       birthday: '2001-10-12', joining_date: '2023-04-03' },
  { name: 'Pradhuman Kumar',    birthday: '1987-12-09', joining_date: '2023-04-01' },
  { name: 'Saurav Pareek',      birthday: '1999-01-14', joining_date: '2023-02-13' },
  { name: 'Satish Khichi',      birthday: '1989-12-27', joining_date: '2022-04-06' },
  { name: 'Kritika Saini',      birthday: '1998-11-08', joining_date: '2022-04-04' },
  { name: 'Rotan Singh',        birthday: '1984-02-29', joining_date: '2021-11-11' },
  { name: 'Swati Joshi',        birthday: '1992-10-20', joining_date: '2021-06-16' },
  { name: 'Divyy Jain',         birthday: '2003-03-31', joining_date: '2025-09-29' },
  { name: 'Kushagra Dubey',     birthday: '2004-06-08', joining_date: '2025-10-10' },
  { name: 'Nikita khandelwal',  birthday: '2002-07-27', joining_date: '2025-11-03' },
  { name: 'Bhanu sharma',       birthday: '2005-12-04', joining_date: '2025-12-03' },
  { name: 'Abhishek Samriya',   birthday: '2004-10-29', joining_date: '2025-12-15' },
  { name: 'Harsh Daharwal',     birthday: '2003-02-20', joining_date: '2026-01-05' },
  { name: 'Simran Gurnani',     birthday: '1999-03-05', joining_date: '2022-01-21' },
  { name: 'Aman Pareek',        birthday: '2006-10-11', joining_date: '2026-02-25' },
  { name: 'Gaurav Gupta',       birthday: '2002-11-12', joining_date: '2026-03-30' },
  { name: 'Vishal Jaga',        birthday: '2001-06-12', joining_date: '2026-04-06' },
  { name: 'Ashish Jha',         birthday: '1999-10-20', joining_date: '2026-04-13' },
  { name: 'Chirag',             birthday: '2001-09-03', joining_date: '2026-05-01' },
  { name: 'Naman Gupta',        birthday: '2004-08-24', joining_date: '2026-05-25' },
];

async function main() {
  const db = await mysql.createConnection({
    host:     process.env.DB_HOST     || 'localhost',
    user:     process.env.DB_USER     || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME     || 'emarketing_task_manager',
    port:     Number(process.env.DB_PORT) || 3306,
  });

  let updated = 0, notFound = [];
  for (const row of DATA) {
    const [res] = await db.query(
      'UPDATE users SET birthday=?, joining_date=? WHERE name=?',
      [row.birthday, row.joining_date, row.name]
    );
    if (res.affectedRows > 0) { updated++; console.log(`✅ ${row.name}`); }
    else { notFound.push(row.name); console.log(`⚠️  NOT FOUND: ${row.name}`); }
  }

  console.log(`\nDone. Updated: ${updated}, Not found: ${notFound.length}`);
  if (notFound.length) console.log('Missing:', notFound.join(', '));
  await db.end();
}

main().catch(err => { console.error(err); process.exit(1); });
