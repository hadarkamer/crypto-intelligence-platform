"""Entrypoint inside a private PID/filesystem/network namespace; no HTTP listener.

The human operates ordinary Chromium, including its native address bar. There is
no debugging protocol, DOM automation, screen capture or input recording in
the authentication phase. All browser files live on the namespace's tmpfs and
disappear when its PID 1 exits.
"""
import os
from pathlib import Path
import re
import resource
import select
import socket
import struct
import subprocess
import sys
import time

NULL = subprocess.DEVNULL
PROFILE='/tmp/browser'
PROXY='--proxy-server=http://127.0.0.1:3128'
HOME_TITLE='CoinGlass | Crypto, Stocks, Macro & Global Market Data'
LOGIN_TITLE='Login to CoinGlass'
CHROME_FLAGS=[PROXY,'--disable-quic','--no-first-run','--no-default-browser-check',
              '--disable-background-networking','--renderer-process-limit=3',
              '--disable-sync','--disable-breakpad','--disable-crash-reporter',
              '--window-position=0,0','--window-size=1200,800']


def spawn(args):
    return subprocess.Popen(args,stdin=NULL,stdout=NULL,stderr=NULL,close_fds=True)


def cmd(args):
    return subprocess.run(args,stdin=NULL,stdout=subprocess.PIPE,stderr=NULL,
                          timeout=3,check=False,text=True).stdout


def windows():
    ids=cmd(['/usr/bin/xprop','-root','-notype','_NET_CLIENT_LIST'])
    return re.findall(r'0x[0-9a-fA-F]+',ids)


class LoginReturn:
    """Navigation hint from a single native window, after the viewer has arrived."""
    def __init__(self):
        self.login_window = None
        self.home_since = None

    def observe(self, window_ids, now):
        if len(window_ids) != 1:
            self.home_since = None
            return False
        wid = window_ids[0]
        title = cmd(['/usr/bin/xprop','-id',wid,'-notype','_NET_WM_NAME'])
        if LOGIN_TITLE in title:
            self.login_window = wid
        if self.login_window != wid or HOME_TITLE not in title:
            self.home_since = None
            return False
        if self.home_since is None:
            self.home_since = now
        return now-self.home_since >= 1


def send_checkpoint(output, state):
    import json
    from session_state import STATE_LIMIT
    payload = json.dumps(state,separators=(',',':'),allow_nan=False).encode('utf-8')
    if not payload or len(payload)>STATE_LIMIT:
        raise ValueError('invalid_operator_checkpoint')
    # Do not close the verifying browser until the parent has committed the
    # scoped source state and incident resolution in the same DB transaction.
    frame = memoryview(struct.pack('!I',len(payload)) + payload)
    while frame:
        sent = output.write(frame)
        if not sent:
            raise RuntimeError('operator_checkpoint_unacknowledged')
        frame = frame[sent:]
    while select.select([output],[],[],600)[0]:
        command = output.read(1)
        if command == b'S':
            return
        if command != b'V':
            break
    raise RuntimeError('operator_checkpoint_unacknowledged')


def main():
    probe=sys.argv[1:]==['--probe']
    # Do not inherit the private state-output socket into any UI process.
    output=None if probe else os.fdopen(os.dup(0),'r+b',buffering=0)
    devnull=os.open('/dev/null',os.O_RDONLY)
    os.dup2(devnull,0);os.close(devnull)
    os.umask(0o077)
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    for path in ('/tmp/home','/tmp/xdg',PROFILE):
        Path(path).mkdir(mode=0o700,exist_ok=True)
    # Namespace must have just loopback and a private /proc; this process never
    # receives the operator's project credentials, DB environment or host home.
    if {name for _,name in socket.if_nameindex()} != {'lo'}:
        return 1
    if any(name in os.environ for name in ('DATABASE_URL','COINGLASS_SESSION_KEY','OPENAI_API_KEY')):
        return 1
    display=spawn(['/usr/bin/Xtigervnc',':19','-geometry','1200x800','-depth','24',
                   '-rfbport','-1','-rfbunixpath','/channel/display','-rfbunixmode','0600',
                   '-SecurityTypes','None','-nolisten','tcp','-AcceptCutText=0','-SendCutText=0',
                   '-FrameRate','15','-NeverShared=1','-DisconnectClients=0','-AcceptSetDesktopSize=0'])
    for _ in range(40):
        if display.poll() is not None:
            return 1
        if Path('/channel/display').is_socket():
            break
        time.sleep(.1)
    else:
        return 1
    # No application launcher/menu exposed through the private window manager.
    cfg=Path('/tmp/openbox.xml')
    cfg.write_text('<openbox_config xmlns="http://openbox.org/3.4/rc"><keyboard/><mouse/><menu/></openbox_config>')
    wm=spawn(['/usr/bin/openbox','--config-file',str(cfg),'--sm-disable'])
    relay=spawn(['/usr/bin/socat','TCP4-LISTEN:3128,bind=127.0.0.1,reuseaddr,fork',
                 'UNIX-CONNECT:/channel/egress'])
    browser=spawn(['/usr/bin/chromium','--user-data-dir='+PROFILE,*CHROME_FLAGS,
                   'about:blank' if probe else 'https://www.coinglass.com/login'])
    attached = False
    transition = LoginReturn()
    until=time.monotonic()+(16 if probe else 570)
    while time.monotonic()<until and all(p.poll() is None for p in (browser,relay,wm,display)):
        ids=windows()
        command = None
        if output and select.select([output], [], [], 0)[0]:
            command = output.read(1)
            if command == b'A':
                attached = True
            if not command:
                return 1
        if ids and (probe or (attached and (command==b'V' or transition.observe(ids,time.monotonic())))):
            break
        time.sleep(.35)
    else:
        return 1
    for wid in ids:
        cmd(['/usr/bin/wmctrl','-ic',wid])
    # Chromium alone serializes its temporary browsing state. A source session
    # that cannot survive a normal browser closure simply fails verification.
    try:
        browser.wait(timeout=12)
    except subprocess.TimeoutExpired:
        return 1
    display.terminate()
    display.wait(timeout=4)
    symbol,model,timeframe=(None,None,None) if probe else sys.argv[1:]
    from playwright.sync_api import sync_playwright
    from modern_map_reader import open_verified_map
    from session_capture import snapshot_coinglass
    with sync_playwright() as playwright:
        context=playwright.chromium.launch_persistent_context(PROFILE,
            executable_path='/usr/bin/chromium',chromium_sandbox=True,headless=True,
            args=CHROME_FLAGS,accept_downloads=False)
        try:
            page=context.new_page()
            if probe:
                page.goto("about:blank")
                return 0 if page.url=="about:blank" else 1
            if timeframe != '12H':
                open_verified_map(page,symbol,int(model),'12H')
            open_verified_map(page,symbol,int(model),timeframe)
            state=snapshot_coinglass(context,page)
            with output:
                send_checkpoint(output,state)
        finally:
            context.close()
    return 0


if __name__=='__main__':
    try:
        sys.exit(main())
    except Exception:
        # No exception or credential-bearing request details leave this namespace.
        sys.exit(1)
