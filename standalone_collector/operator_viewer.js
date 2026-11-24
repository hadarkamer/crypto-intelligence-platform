const base = '/coinglass/operator';
const status = document.getElementById('status');
const verify = document.getElementById('verify');
async function connect() {
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
