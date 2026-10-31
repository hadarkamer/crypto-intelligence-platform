"""Pure fake-file/clock/libc controls. No process or native allocator probes."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

spec=importlib.util.spec_from_file_location('process_memory_under_test',Path(__file__).with_name('research_process_memory.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
SMAPS=b'1000-9000 ---p 00000000 00:00 0 [rollup]\nRss: 1000 kB\nPss: 900 kB\nShared_Clean: 100 kB\nShared_Dirty: 0 kB\nPrivate_Clean: 100 kB\nPrivate_Dirty: 800 kB\nAnonymous: 800 kB\nPss_Anon: 800 kB\nPss_File: 100 kB\n'

class Function:
    def __init__(self,result):self.result=result;self.calls=0
    def __call__(self):self.calls+=1;return self.result
class FakeCtypes:
    class Structure:pass
    c_size_t=object();c_void_p=object();c_char_p=object()
    @staticmethod
    def sizeof(value):return 80 if isinstance(value,type) and issubclass(value,FakeCtypes.Structure) else 8
    @staticmethod
    def alignment(value):return 8
def libc(version=b'2.36',**changed):
    fields=dict(arena=1000,ordblks=3,smblks=2,hblks=1,hblkhd=500,usmblks=0,fsmblks=100,uordblks=600,fordblks=400,keepcost=300);fields.update(changed)
    return SimpleNamespace(gnu_get_libc_version=Function(version),mallinfo2=Function(SimpleNamespace(**fields)))

class Controls(unittest.TestCase):
    def sample(self,**kwargs):
        self.calls=[]
        def read(path,cap):self.calls.append((path,cap));return b'Name: test\nVmRSS: 1001 kB\n' if path.endswith('/status') else SMAPS
        base=dict(platform_name='linux',read_file=read,clock=Function(1.),utc_now=lambda:'2026-10-31T21:00:00+00:00',getallocatedblocks=lambda:123,getgccounts=lambda:(1,2,3));base.update(kwargs);return m.sample(**base)
    def mall(self,lib=None,**kwargs):
        self.lib=libc() if lib is None else lib;return m.read_mallinfo2(platform_name='linux',ctypes_api=FakeCtypes,loader=lambda target:self.lib,**kwargs)
    def test_smaps_numeric_fields_only(self):
        r=m.parse_smaps_rollup(SMAPS);self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['fields_bytes']['Rss'],1024000);self.assertNotIn('1000-9000',str(r))
    def test_optional_absence_is_none_not_zero(self):self.assertIsNone(m.parse_smaps_rollup(SMAPS)['fields_bytes']['SwapPss'])
    def test_duplicates_do_not_silently_override(self):r=m.parse_smaps_rollup(SMAPS+b'Rss: 5 kB\n');self.assertEqual(r['status'],'PARTIAL');self.assertIsNone(r['fields_bytes']['Rss'])
    def test_wrong_units_rejected(self):self.assertEqual(m.parse_status(b'VmRSS: 5 bytes\n')['status'],'UNAVAILABLE')
    def test_negative_rejected(self):self.assertEqual(m.parse_status(b'VmRSS: -5 kB\n')['status'],'UNAVAILABLE')
    def test_overflow_rejected(self):self.assertIsNone(m.parse_status(b'VmRSS: 999999999999999999 kB\n')['fields_bytes']['VmRSS'])
    def test_status_read_cap(self):self.assertEqual(m.parse_status(b'x'*(m.STATUS_CAP+1))['errors'],['READ_CAP'])
    def test_smaps_read_cap(self):self.assertEqual(m.parse_smaps_rollup(b'x'*(m.SMAPS_CAP+1))['errors'],['READ_CAP'])
    def test_invalid_read_type(self):self.assertEqual(m.parse_status('VmRSS: 5 kB')['errors'],['INVALID_READ_TYPE'])
    def test_missing_required_partial(self):self.assertEqual(m.parse_smaps_rollup(b'Rss: 2 kB\n')['status'],'PARTIAL')
    def test_sample_exact_two_bounded_reads_no_native_default(self):
        r=self.sample(mallinfo_reader=lambda:(_ for _ in ()).throw(AssertionError('native called')));self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['rss_bytes'],1001*1024);self.assertEqual(r['allocator']['status'],'NOT_REQUESTED');self.assertEqual(self.calls,[('/proc/self/status',16384),('/proc/self/smaps_rollup',65536)])
    def test_unsupported_platform_no_files(self):r=self.sample(platform_name='win32');self.assertEqual(self.calls,[]);self.assertEqual(r['platform'],'UNSUPPORTED')
    def test_file_failure_keeps_python_counts(self):r=self.sample(read_file=lambda *args:(_ for _ in ()).throw(PermissionError()));self.assertEqual(r['status'],'PARTIAL');self.assertEqual(r['python_counters']['allocated_blocks_count'],123)
    def test_baseexception_file_failure_failopen(self):self.assertEqual(self.sample(read_file=lambda *args:(_ for _ in ()).throw(KeyboardInterrupt()))['status'],'PARTIAL')
    def test_boolean_block_counter_rejected(self):self.assertIsNone(self.sample(getallocatedblocks=lambda:True)['python_counters']['allocated_blocks_count'])
    def test_gc_raw_signed_tuple_preserved(self):self.assertEqual(self.sample(getgccounts=lambda:(-1,2,3))['python_counters']['gc_count_tuple'],[-1,2,3])
    def test_boolean_gc_tuple_rejected(self):self.assertIsNone(self.sample(getgccounts=lambda:(True,2,3))['python_counters']['gc_count_tuple'])
    def test_counter_errors_failopen(self):self.assertEqual(self.sample(getgccounts=lambda:(_ for _ in ()).throw(RuntimeError()))['status'],'PARTIAL')
    def test_clock_error_failopen(self):self.assertIn('CLOCK_ERROR',self.sample(clock=lambda:(_ for _ in ()).throw(RuntimeError()))['errors'])
    def test_optional_native_failure_failopen(self):self.assertEqual(self.sample(include_mallinfo2=True,mallinfo_reader=lambda:(_ for _ in ()).throw(KeyboardInterrupt()))['allocator']['status'],'UNAVAILABLE')
    def test_requested_native_unavailable_top_quality_partial(self):self.assertEqual(self.sample(include_mallinfo2=True,mallinfo_reader=lambda:m._unavailable_mall('TEST'))['status'],'PARTIAL')
    def test_invalid_native_result_failopen(self):self.assertEqual(self.sample(include_mallinfo2=True,mallinfo_reader=lambda:None)['allocator']['status'],'UNAVAILABLE')
    def test_invalid_boolean_configuration(self):self.assertIn('INVALID_CONFIGURATION',self.sample(include_mallinfo2=1)['errors'])
    def test_nonfinite_clock_not_json_nan(self):self.assertIsNone(self.sample(clock=lambda:float('nan'))['elapsed_ms'])
    def test_mallinfo_coherent_bytes_vs_counts(self):r=self.mall();self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['fields']['arena'],1000);self.assertEqual(r['field_units']['hblks'],'COUNT');self.assertEqual(r['field_units']['hblkhd'],'BYTES');self.assertEqual(self.lib.mallinfo2.calls,1)
    def test_glibc_too_old_does_not_call(self):lib=libc(b'2.32');self.assertEqual(self.mall(lib)['status'],'UNAVAILABLE');self.assertEqual(lib.mallinfo2.calls,0)
    def test_glibc_minimum_version_supported(self):self.assertEqual(self.mall(libc(b'2.33'))['status'],'COMPLETE')
    def test_glibc_version_invalid_does_not_call(self):lib=libc(b'notglibc');self.assertEqual(self.mall(lib)['status'],'UNAVAILABLE');self.assertEqual(lib.mallinfo2.calls,0)
    def test_glibc_version_type(self):lib=libc('2.36');self.assertEqual(self.mall(lib)['status'],'UNAVAILABLE');self.assertEqual(lib.mallinfo2.calls,0)
    def test_mallinfo_incoherent_not_complete(self):self.assertEqual(self.mall(libc(arena=5))['status'],'PARTIAL')
    def test_mallinfo_boolean_field_rejected(self):self.assertEqual(self.mall(libc(arena=True))['status'],'UNAVAILABLE')
    def test_mallinfo_missing_symbol_failopen(self):self.assertEqual(self.mall(SimpleNamespace(gnu_get_libc_version=Function(b'2.36')))['status'],'UNAVAILABLE')
    def test_native_unsupported_platform_before_loader(self):self.assertEqual(m.read_mallinfo2(platform_name='win32',ctypes_api=FakeCtypes,loader=lambda _:(_ for _ in ()).throw(AssertionError()))['errors'],['UNSUPPORTED_PLATFORM'])

if __name__=='__main__':unittest.main()
