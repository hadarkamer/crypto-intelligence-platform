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
import struct
import sys
import tempfile

from session_state import STATE_LIMIT, validate_state

HERE = Path(__file__).resolve().parent
ASSETS = Path('/usr/share/novnc')
BWRAP = '/usr/bin/bwrap'
FILES = ('operator_desktop_child.py', 'session_capture.py', 'session_state.py',
         'modern_map_reader.py', 'modern_map_evidence.py', 'heatmap_hover.py',
         'model1_execution.py', 'collection_metrics.py', 'capture_failure_diagnostics.py',
         'capture_failure_observer.py', 'capture_readiness.py', 'heatmap_models.py',
         'image_detail.py', 'model1_price_range.py', 'price_detail_input.py')
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


def namespace_args(directory, system=Path('/')):
    # Only immutable system files and the audited reader are visible. In
    # particular, neither application storage nor the host's /proc or /tmp is bound.
    args = [BWRAP,'--unshare-all','--unshare-user','--die-with-parent','--new-session',
            '--cap-drop','ALL','--hostname','coinglass-operator','--clearenv',
            '--ro-bind',str(system/'usr'),'/usr','--symlink','usr/bin','/bin',
            '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
            '--proc','/proc','--dev','/dev','--size','67108864','--tmpfs','/dev/shm',
            '--size','268435456','--tmpfs','/tmp',
            '--dir','/etc','--dir','/run','--dir','/app',
            '--dir','/etc/ssl','--ro-bind',str(system/'etc/ssl/certs'),'/etc/ssl/certs',
            '--ro-bind-try',str(system/'etc/ssl/openssl.cnf'),'/etc/ssl/openssl.cnf',
            '--ro-bind',str(system/'etc/fonts'),'/etc/fonts',
            '--ro-bind','/etc/passwd','/etc/passwd','--ro-bind','/etc/group','/etc/group',
            '--ro-bind-try',str(system/'etc/chromium'),'/etc/chromium',
            '--ro-bind-try',str(system/'etc/chromium.d'),'/etc/chromium.d',
            '--bind',str(directory),'/channel','--chdir','/tmp','--setenv','HOME','/tmp/home',
            '--setenv','PATH','/usr/local/bin:/usr/bin:/bin','--setenv','LANG','C.UTF-8',
            '--setenv','DISPLAY',':19','--setenv','XDG_RUNTIME_DIR','/tmp/xdg',
            '--setenv','PYTHONDONTWRITEBYTECODE','1']
    for filename in FILES:
        args.extend(('--ro-bind',str(HERE/filename),'/app/'+filename))
    return args


def installation_issue(system):
    # These paths are owned by the supplied image. A configured flag or
    # executable supplied by an HTTP client is never a capability proof.
    binaries = ('/usr/bin/Xtigervnc', '/usr/bin/chromium', '/usr/bin/socat',
                '/usr/bin/xprop', '/usr/bin/wmctrl', '/usr/bin/openbox', '/usr/local/bin/python')
    if os.getuid() == 0:
        return 'service_user_must_be_nonroot'
    if not os.access(BWRAP,os.X_OK) or not all(os.access(system/path.lstrip('/'),os.X_OK) for path in binaries):
        return 'image_binaries_missing'
    if not (system/'usr/share/novnc/core/rfb.js').is_file() or not all((HERE/f).is_file() for f in FILES):
        return 'image_assets_missing'
    policy = system/'etc/chromium/policies/managed/operator.json'
    try:
        if json.loads(policy.read_text()) == json.loads((HERE/'operator_chromium_policy.json').read_text()):
            return None
    except (OSError, ValueError):
        pass
    return 'browser_policy_missing_or_invalid'


async def sandbox_preflight(directory, system=Path('/')):
    # Test the exact child namespace and its mounts, before launching any UI.
    proc = await asyncio.create_subprocess_exec(*namespace_args(directory,system), '/usr/bin/true',
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE, env={})
    try:
        async with asyncio.timeout(10):
            _, diagnostic = await proc.communicate()
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    if proc.returncode == 0:
        return None
    # The command only runs true with fixed image paths, no credentials, no
    # network. Return an enum even if the host reports something unexpected.
    detail = diagnostic[:4096].lower()
    denied = any(marker in detail for marker in (b'operation not permitted', b'permission denied',
                                                  b'no permissions to create'))
    if denied and (b'namespace' in detail or b'unshare' in detail):
        return 'host_namespace_denied'
    if denied and (b'mount' in detail or b'pivot' in detail):
        return 'host_mount_denied'
    if denied and (b'netlink' in detail or b'loopback' in detail):
        return 'host_network_namespace_denied'
    return 'host_sandbox_denied' if denied else 'sandbox_image_setup_failed'


async def _rfb_greeting(directory, proc):
    for _ in range(150):
        if proc.returncode is not None:
            return False
        try:
            reader, writer = await asyncio.open_unix_connection(str(Path(directory)/'display'))
            break
        except (FileNotFoundError, ConnectionRefusedError):
            await asyncio.sleep(.1)
    else:
        return False
    try:
        return await reader.readexactly(12) == b'RFB 003.008\n'
    finally:
        writer.close()
        await writer.wait_closed()


PROBE_STAGES = frozenset(('isolated_environment', 'private_display', 'window_manager',
                          'native_browser', 'native_browser_close', 'reader_import',
                          'sandboxed_verifier', 'local_verification'))


async def probe_result(system=Path('/')):
    issue = installation_issue(system)
    if issue:
        return issue
    with tempfile.TemporaryDirectory() as directory:
        try:
            issue = await sandbox_preflight(directory,system)
            if issue:
                return issue
            proc = await asyncio.create_subprocess_exec(*namespace_args(directory,system),
                '/usr/local/bin/python', '/app/operator_desktop_child.py', '--probe',
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, env={})
            greeting = asyncio.create_task(_rfb_greeting(directory, proc))
            try:
                async with asyncio.timeout(55):
                    stages, _ = await proc.communicate()
                    if proc.returncode != 0:
                        known = [stage for stage in stages.decode('ascii',errors='ignore').splitlines()
                                 if stage in PROBE_STAGES]
                        return (known[-1] if known else 'sandbox_child') + '_failed'
                    return 'ready' if await greeting else 'private_rfb_handshake_failed'
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
                greeting.cancel()
                await asyncio.gather(greeting,return_exceptions=True)
        except TimeoutError:
            return 'runtime_probe_timeout'
        except (OSError, asyncio.IncompleteReadError):
            return 'runtime_transport_unavailable'


async def probe():
    return await probe_result() == 'ready'


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

    async def viewer_attached(self):
        await asyncio.get_running_loop().sock_sendall(self.channel,b'A')

    async def verify_now(self):
        await asyncio.get_running_loop().sock_sendall(self.channel,b'V')

    async def _read_exact(self,size):
        raw = bytearray()
        loop = asyncio.get_running_loop()
        while len(raw)<size:
            chunk = await loop.sock_recv(self.channel,min(8192,size-len(raw)))
            if not chunk:
                raise RuntimeError('protected_map_unconfirmed')
            raw.extend(chunk)
        return raw

    async def verified_state(self):
        size = struct.unpack('!I',await self._read_exact(4))[0]
        if not 0<size<=STATE_LIMIT:
            raise RuntimeError('invalid_operator_receipt')
        return validate_state(json.loads(await self._read_exact(size)))

    async def acknowledge_commit(self):
        # The encrypted transaction is durable even if the local verifier
        # crashes before observing this private acknowledgement.
        await asyncio.get_running_loop().sock_sendall(self.channel,b'S')
        with suppress(TimeoutError):
            await asyncio.wait_for(self.proc.wait(),5)

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
    if sys.argv[1:] == ['--check-runtime']:
        system = Path('/')
    elif len(sys.argv)==3 and sys.argv[1]=='--check-native-fixture':
        # CI test fixture: the real distro binaries from the built image, on
        # a genuine unprivileged host without the outer Docker seccomp layer.
        # Service requests cannot select an executable, root, or host policy.
        system = Path(sys.argv[2]).resolve(strict=True)
    else:
        raise SystemExit(2)
    reason = asyncio.run(probe_result(system))
    ready = reason == 'ready'
    print(json.dumps({'isolated_operator_browser':ready,'reason':reason,'provider_requests':0}))
    raise SystemExit(0 if ready else 1)
