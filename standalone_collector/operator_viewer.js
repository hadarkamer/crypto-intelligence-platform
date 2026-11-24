const base = '/coinglass/operator';
const ticket = location.hash.slice(1);
history.replaceState(null, '', base);
const status = document.getElementById('status');
const verify = document.getElementById('verify');
async function connect() {
  if (!/^[A-Za-z0-9_-]{43}$/.test(ticket)) throw Error();
  const admitted = await fetch(base + '/admit', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ticket}), credentials: 'same-origin', cache: 'no-store'
  });
  if (!admitted.ok) throw Error();
  const {default: RFB} = await import('/coinglass/operator/assets/core/rfb.js');
  const rfb = new RFB(document.getElementById('screen'), 'wss://' + location.host + base + '/display');
  rfb.scaleViewport = true;
  rfb.addEventListener('connect', () => {
    status.textContent = 'התחברו בחלון האתר הרשמי. לאחר הכניסה האספן יבדוק את המפה.';
    verify.hidden = false;
  });
  rfb.addEventListener('disconnect', () => {
    verify.hidden = true;
    status.textContent = 'חלון החיבור נסגר. אפשר לראות את תוצאת הבדיקה באפליקציה.';
  });
  verify.onclick = async () => {
    verify.disabled = true;
    try {
      const result = await fetch(base + '/verify', {method: 'POST', credentials: 'same-origin', cache: 'no-store'});
      if (result.ok) status.textContent = 'האספן בודק את המפה המבוקשת.';
      else verify.disabled = false;
    } catch { verify.disabled = false; }
  };
}
connect().catch(() => {
  verify.hidden = true;
  status.textContent = 'לא ניתן לפתוח את חלון החיבור. חזרו לאפליקציה לפתיחת חלון חדש.';
});
