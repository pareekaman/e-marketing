// Users → the table rows, rendered straight into #usersTbody (was the body of
// renderUsersTable in public/js/users.js). users.js still loads and filters
// the list and hands it over with usersSetReact; Edit and Delete are its own.
import { useEffect, useState } from 'react';
import { legacy, css } from '../lib/app.js';

const roleLabel = r => (r === 'admin' ? '👑 Admin' : r === 'hod' ? '🏢 HOD' : r === 'pc' ? '🖥️ PC' : '👤 User');
const roleCls = r => (['admin', 'hod', 'pc', 'user'].includes(r) ? r : 'user');

export default function UsersRows() {
  const [users, setUsers] = useState(null);
  useEffect(() => {
    legacy('usersSetReact', list => setUsers(Array.isArray(list) ? list : []));
    return () => legacy('usersSetReact', null);
  }, []);

  if (!users) return null;
  if (!users.length) return <tr><td colSpan={6} ref={css('text-align:center;padding:24px;color:#94a3b8')}>No users found</td></tr>;
  return users.map(u => {
    const userRole = u.user_role || u.role;
    return (
      <tr key={u.id}>
        <td ref={css('font-weight:600')}>{u.name}</td>
        <td ref={css('color:#64748b')}>{u.email}</td>
        <td ref={css('color:#64748b')}>{u.phone || '—'}</td>
        <td ref={css('color:#64748b')}>{u.department || '—'}</td>
        <td>
          <span className={`role-badge ${roleCls(u.role)}`} title="App Role — permissions">{roleLabel(u.role)}</span>
          {userRole !== u.role ? <>{' '}<br /><span className={`role-badge ${roleCls(userRole)}`} ref={css('font-size:10px;margin-top:3px;display:inline-block;opacity:.85')} title="User Role — leave hierarchy">{`→ ${roleLabel(userRole)}`}</span></> : null}
        </td>
        <td>
          <button className="action-btn edit" onClick={() => legacy('openEditUser', u.id)}>Edit</button>
          {' '}
          <button className="action-btn delete" ref={css('margin-left:6px')} onClick={() => legacy('deleteUser', u.id)}>Delete</button>
        </td>
      </tr>
    );
  });
}
