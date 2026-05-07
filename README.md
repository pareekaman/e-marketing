# E-Marketing Task Manager

Checklist + Delegation web app with email automation, Google Sheets integration, and role-based access (Admin / HOD / PC / User).

---

## ⚡ Quick Start — Already Configured!

Iss project me **MySQL credentials already `.env` file me bhare hue hain** aur server pehli baar start hote hi:
- ✅ Saari tables apne aap create kar lega (`users`, `delegation_tasks`, `checklist_tasks`, `task_approvals`, `task_comments`, `task_transfers`, `fms_sheets`, `fms_steps`, `fms_step_doers`, `fms_extra_rows`, `week_plans`)
- ✅ Default admin user seed kar dega: **`aman@test.com` / `password`**

Bas database empty hona chahiye — server khud setup kar lega.

---

## 📁 Project Structure

```
emarketing-task-manager/
├── server.js              # Express app (Vercel-compatible)
├── package.json           # Dependencies
├── vercel.json            # Vercel build + routing config
├── .env                   # Your secrets (already filled, NEVER commit)
├── .env.example           # Template for reference
├── .gitignore             # Keeps .env out of Git
├── README.md              # This file
└── public/
    ├── index.html         # Login page
    ├── app.html           # Main app
    └── emarketing-logo.png # Brand logo
```

---

## 🚀 GitHub → Vercel Deployment

### Step 1: GitHub pe push karo

```bash
cd emarketing-task-manager
git init
git add .
git commit -m "Initial commit — E-Marketing Task Manager"
git branch -M main
git remote add origin https://github.com/<your-username>/emarketing-task-manager.git
git push -u origin main
```

> ⚠️ Push se pehle `git status` chala ke confirm karo `.env` file repo me **nahi** ja rahi (`.gitignore` me already hai).

### Step 2: Vercel pe deploy karo

1. https://vercel.com → **Add New → Project**
2. GitHub repo import karo
3. Framework preset: **Other**
4. Build Settings: default chhod do
5. **Environment Variables** add karo (ye exact values jo `.env` me hain):

| Variable | Value |
|---|---|
| `DB_HOST` | `87.106.200.69` |
| `DB_USER` | `my_user` |
| `DB_PASSWORD` | `StrongPassword123!` |
| `DB_NAME` | `emarketing_task_manager` |
| `DB_PORT` | `3306` |
| `DB_SSL` | `false` |
| `NODE_ENV` | `production` |
| `SESSION_SECRET` | koi 32+ char random string |
| `APP_URL` | (deploy ke baad fill karna — Vercel URL) |
| `SMTP_USER` | Gmail address (optional, email feature ke liye) |
| `SMTP_PASS` | Gmail App Password (optional) |
| `SMTP_FROM_NAME` | `E-Marketing Task Manager` |
| `GOOGLE_CREDENTIALS` | Service account JSON (optional, sheets feature ke liye) |

6. **Deploy** daabo

### Step 3: APP_URL update karo

Pehli deploy ke baad Vercel jo URL dega (e.g. `https://emarketing-task-manager.vercel.app`), use Settings → Environment Variables → `APP_URL` me set karke **Redeploy** karo. Ye email links ke liye chahiye.

### Step 4: Login karo

URL khol ke `aman@test.com` / `password` se login karo. Andar jaake naye users add kar sakte ho.

> ⚠️ Production me default password turant change kar dena (Profile → Change Password).

---

## 💻 Local Development

```bash
# 1. Dependencies install karo
npm install

# 2. .env already filled hai, bas check kar lo
# 3. Run karo
npm run dev   # nodemon (auto-restart on changes)
# ya
npm start
```

Browser me `http://localhost:3000` khol lo.

Pehli baar start hone par console me ye dikhega:
```
✅ MySQL Connected Successfully!
✅ DB migrations checked
🌱 Default admin seeded → aman@test.com / password
✦ E-Marketing Task Manager: http://localhost:3000
```

---

## 🗄️ Database — MySQL Server `87.106.200.69`

**Important checks pehli baar deploy karne se pehle**:

1. **Database `emarketing_task_manager` exist karta hai?**
   - Agar nahi to MySQL server pe banao:
     ```sql
     CREATE DATABASE emarketing_task_manager
       CHARACTER SET utf8mb4
       COLLATE utf8mb4_unicode_ci;
     ```
2. **User `my_user` ke paas iss database pe full permissions hain?**
   ```sql
   GRANT ALL PRIVILEGES ON emarketing_task_manager.* TO 'my_user'@'%';
   FLUSH PRIVILEGES;
   ```
3. **Remote connections allowed hain?**
   - MySQL config me `bind-address = 0.0.0.0`
   - Firewall pe port `3306` open
   - Vercel ke serverless functions ke liye remote access zaroori hai
4. **Database empty ho** — agar koi purane client ka data ya tables hai to drop kar do:
   ```sql
   DROP DATABASE IF EXISTS emarketing_task_manager;
   CREATE DATABASE emarketing_task_manager CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   ```

Server pehli baar start hote hi 11 tables apne aap ban jaayengi aur admin user seed ho jaayega.

---

## ⚠️ Vercel Caveats

### File system read-only hai
Profile photos jaisa kuch agar disk pe save hota hai, **fail hoga**. App me images base64 me DB me save hoti hain (`profile_image LONGTEXT`) — to ye safe hai.

### Cold starts
Pehli request 1–3 sec slow ho sakti hai. Active use me normal speed.

### Function timeout
Free tier pe 10 sec limit. Heavy Google Sheets sync agar lambi chale to fail ho sakti hai (Pro plan pe limit zyada).

### Database connection limits
Agar zyada concurrent users ho to "too many connections" error aa sakta hai. `connectionLimit: 5` set hai — provider ke max ke hisaab se adjust karna pad sakta hai.

---

## 📧 Email Automation (optional)

### Setup:
1. Gmail pe 2FA on karo: https://myaccount.google.com/security
2. App Password banao: https://myaccount.google.com/apppasswords
3. `.env` me ya Vercel env vars me daalo:
   ```
   SMTP_USER=yourgmail@gmail.com
   SMTP_PASS=abcdefghijklmnop
   ```

### Use:
1. Login → **Profile** → **Notification Email** field
2. Real Gmail/Outlook address daalo → Save
3. Jab koi delegation task assign hoga + uss user ka notification_email set hai → automatic email jaayegi

Checklist tasks par email **nahi** jaati. Agar SMTP env vars blank hain to email feature silent disabled rahega — app normal chalegi.

---

## 🔧 Troubleshooting

**"MySQL Connection Failed" error**
- DB_HOST, DB_USER, DB_PASSWORD env vars sahi hain?
- MySQL server remote connections allow karta hai?
- Database exist karta hai aur user ke paas permissions hain?
- Firewall pe port 3306 open hai?

**Login ke baad "Not authenticated" error**
- `NODE_ENV=production` set hai? Vercel HTTPS hai, isliye cookie `secure` flag chahiye.

**"too many connections" error after some time**
- MySQL provider ka max_connections check karo, `connectionLimit` ko us ke 60% pe set karo

**Static files (logo) load nahi ho rahe**
- Vercel deployment logs me dekh lo `public/` files include hui hain
- `vercel.json` me `includeFiles: public/**` already set hai

**SMTP "Invalid login"**
- Regular Gmail password kaam nahi karega — App Password chahiye (16 chars, no spaces)
- 2FA on hai? Confirm karo

---

## 🔑 Default Login

```
Email:    aman@test.com
Password: password
```

⚠️ **Production me turant change karo** (Profile → Change Password).

---

## 📝 Tables Created Automatically

Server start hone par ye 11 tables apne aap ban jaati hain:

1. `users` — Login + role + profile
2. `delegation_tasks` — Delegation tasks
3. `checklist_tasks` — Checklist tasks
4. `task_approvals` — Task approval requests
5. `task_comments` — Comments on tasks
6. `task_transfers` — Task transfer requests
7. `fms_sheets` — FMS (Google Sheets sync) configs
8. `fms_steps` — FMS step definitions
9. `fms_step_doers` — Step → user assignments
10. `fms_extra_rows` — FMS extra input rows
11. `week_plans` — Weekly planning targets
