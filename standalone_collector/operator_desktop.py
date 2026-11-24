"""One ephemeral local Linux desktop; no URL, shell or executable comes from a request.

The namespace has its own PID, mount, IPC and network views. Its only outbound
route is a byte-transparent TLS CONNECT relay to the named source/identity
hosts. No debugging endpoint or VNC TCP listener is opened by the parent.
"""
import asyncio
from contextlib import suppress
from dataclasses import dataclass
import ipaddress
import json
import os
from pathlib import Path
import socket
import sys
import tempfile

from session_state import STATE_LIMIT, validate_state

HERE = Path(__file__).resolve().parent
ASSETS = Path('/usr/share/novnc')
BWRAP = '/usr/bin/bwrap'
FILES = ('operator_desktop_child.py', 'session_capture.py', 'session_state.py',
         'modern_map_reader.py', 'modern_map_evidence.py', 'heatmap_hover.py',
         'model1_execution.py', 'collection_metrics.py')
PORT = 443


def permitted_host(host):
    host = host.lower()
    return (host == 'coinglass.com' or host.endswith('.coinglass.com')
            or host in ('cdn.coinglasscdn.com', 'accounts.google.com', 'oauth2.googleapis.com',
                        'lh3.googleusercontent.com')
            or host == 'gstatic.com' or host.endswith('.gstatic.com'))


async def _copy(reader, writer):
    while chunk := await reader.read(65536):
        writer.write(chunk)
        await writer.drain()


async def _relay(first, second):
    tasks = [asyncio.create_task(first), asyncio.create_task(second)]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _connect_tunnel(reader, writer):
    remote = None
    try:
        async with asyncio.timeout(8):
            header = await reader.readuntil(b'\r\n\r\n')
            method, authority, version = header.split(b'\r\n',1)[0].decode('ascii').split()
            host, port = authority.split(':')
            if (method != 'CONNECT' or version not in ('HTTP/1.0','HTTP/1.1')
                    or port != '443' or not permitted_host(host)
                    or not all(part and all(ch.isascii() and (ch.isalnum() or ch=='-') for ch in part)
                               for part in host.split('.'))):
                return
            addresses = await asyncio.get_running_loop().getaddrinfo(host, PORT, type=socket.SOCK_STREAM)
            if not addresses or any(not ipaddress.ip_address(addr[4][0]).is_global for addr in addresses):
                return
            # Dial the validated numeric IP; leave TLS and certificate checks to Chromium.
            for family, _, _, _, sockaddr in addresses:
                try:
                    upstream, remote = await asyncio.open_connection(sockaddr[0], PORT, family=family)
                    break
                except OSError:
                    continue
            if remote is None:
                return
            writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await writer.drain()
        await _relay(_copy(reader, remote), _copy(upstream, writer))
    except (ValueError, OSError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        pass
    finally:
        writer.close()
        if remote is not None:
            remote.close()


def namespace_args(directory):
    # Only immutable system files and the audited reader are visible. In
    # particular, neither application storage nor the host's /proc or /tmp is bound.
    args = [BWRAP,'--unshare-all','--unshare-user','--die-with-parent','--new-session',
            '--cap-drop','ALL','--hostname','coinglass-operator','--clearenv',
            '--ro-bind','/usr','/usr','--symlink','usr/bin','/bin',
            '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
            '--proc','/proc','--dev','/dev','--size','67108864','--tmpfs','/dev/shm',
            '--size','268435456','--tmpfs','/tmp',
            '--dir','/etc','--dir','/run','--dir','/app',
            '--ro-bind','/etc/ssl','/etc/ssl','--ro-bind','/etc/fonts','/etc/fonts',
            '--ro-bind','/etc/passwd','/etc/passwd','--ro-bind','/etc/group','/etc/group',
            '--ro-bind-try','/etc/chromium','/etc/chromium',
            '--ro-bind-try','/etc/chromium.d','/etc/chromium.d',
            '--bind',str(directory),'/channel','--chdir','/tmp','--setenv','HOME','/tmp/home',
            '--setenv','PATH','/usr/local/bin:/usr/bin:/bin','--setenv','LANG','C.UTF-8',
            '--setenv','DISPLAY',':19','--setenv','XDG_RUNTIME_DIR','/tmp/xdg',
            '--setenv','PYTHONDONTWRITEBYTECODE','1']
    for filename in FILES:
        args.extend(('--ro-bind',str(HERE/filename),'/app/'+filename))
    return args


def installed():
    # The supplied service Dockerfile owns this layout; never infer isolation
    # from an environment variable naming an arbitrary binary or remote host.
    files = (BWRAP,'/usr/bin/Xtigervnc','/usr/bin/chromium','/usr/bin/socat',
             '/usr/bin/xprop','/usr/bin/wmctrl','/usr/bin/openbox','/usr/local/bin/python')
    policy = Path('/etc/chromium/policies/managed/operator.json')
    try:
        if json.loads(policy.read_text()) != json.loads((HERE/'operator_chromium_policy.json').read_text()):
            return False
    except (OSError,ValueError):
        return False
    return (os.getuid()!=0 and all(os.access(path,os.X_OK) for path in files)
            and (ASSETS/'core/rfb.js').is_file() and all((HERE/f).is_file() for f in FILES))


async def probe():
    if not installed():
        return False
    with tempfile.TemporaryDirectory() as directory:
        try:
            proc=await asyncio.create_subprocess_exec(*namespace_args(directory),
                '/usr/local/bin/python','/app/operator_desktop_child.py','--probe',
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL, env={})
            try:
                return await asyncio.wait_for(proc.wait(),50)==0
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
        except (OSError, TimeoutError):
            return False


@dataclass
class Desktop:
    directory: object
    proc: object
    channel: socket.socket
    proxy: object
    connections: set

    @classmethod
    async def start(cls, symbol, model, timeframe):
        directory=tempfile.TemporaryDirectory(prefix='coinglass-desktop-')
        parent, child=socket.socketpair()
        conns=set()
        proxy=None
        try:
            def accept(reader, writer):
                if len(conns)>=64:
                    writer.close()
                    return
                task=asyncio.create_task(_connect_tunnel(reader,writer))
                conns.add(task);task.add_done_callback(conns.discard)
            proxy=await asyncio.start_unix_server(accept,path=directory.name+'/egress',limit=8192)
            proc=await asyncio.create_subprocess_exec(*namespace_args(directory.name),
                '/usr/local/bin/python','/app/operator_desktop_child.py',symbol,str(model),timeframe,
                stdin=child, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                env={}, start_new_session=True)
            parent.setblocking(False)
            return cls(directory,proc,parent,proxy,conns)
        except BaseException:
            parent.close()
            if proxy:
                proxy.close();await proxy.wait_closed()
            directory.cleanup()
            raise
        finally:
            child.close()

    async def connect_display(self):
        for _ in range(70):
            if self.proc.returncode is not None:
                raise RuntimeError('desktop_start_failed')
            try:
                return await asyncio.open_unix_connection(self.directory.name+'/display')
            except (FileNotFoundError, ConnectionRefusedError):
                await asyncio.sleep(.1)
        raise RuntimeError('desktop_start_failed')

    async def verify_now(self):
        await asyncio.get_running_loop().sock_sendall(self.channel, b'V')

    async def verified_state(self):
        # Dedicated socketpair attached to the child's fd 0; no cookie is ever routed
        # via service stdout, a filesystem result, the VNC viewer, or an HTTP response.
        raw=bytearray()
        loop=asyncio.get_running_loop()
        while chunk:=await loop.sock_recv(self.channel, min(8192,STATE_LIMIT+1-len(raw))):
            raw.extend(chunk)
            if len(raw)>STATE_LIMIT:
                raise RuntimeError('invalid_operator_receipt')
        if await self.proc.wait()!=0 or not raw:
            raise RuntimeError('protected_map_unconfirmed')
        return validate_state(json.loads(raw))

    async def close(self):
        self.channel.close()
        if self.proc.returncode is None:
            with suppress(ProcessLookupError):
                self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(),2)
            except TimeoutError:
                with suppress(ProcessLookupError):
                    self.proc.kill()
                await self.proc.wait()
        self.proxy.close();await self.proxy.wait_closed()
        tasks=list(self.connections)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        self.directory.cleanup()


if __name__ == '__main__':
    if sys.argv[1:] != ['--check-runtime']:
        raise SystemExit(2)
    ready = asyncio.run(probe())
    print(json.dumps({'isolated_operator_browser': ready, 'provider_requests': 0}))
    raise SystemExit(0 if ready else 1)
