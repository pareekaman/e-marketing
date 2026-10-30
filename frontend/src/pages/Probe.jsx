// Phase 0 check only: proves a React page mounts inside the app, reads the
// session and calls the API through the old api(). Not linked from the nav.
import { useEffect, useState } from 'react';
import { api, me } from '../lib/app.js';

export default function Probe() {
  const [count, setCount] = useState(null);
  useEffect(() => {
    api('/api/holidays').then(r => setCount(Array.isArray(r) ? r.length : (r && r.holidays ? r.holidays.length : '?')));
  }, []);
  const user = me();
  return (
    <div className="card" style={{ padding: 16 }}>
      <b>React is running.</b> Signed in as {user ? user.name : '—'}; holidays from the API: {count ?? '…'}
    </div>
  );
}
